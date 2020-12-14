/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.mware.bolt.transport;

import com.mware.bolt.BoltChannel;
import com.mware.bolt.transport.pipeline.ProtocolHandshaker;
import com.mware.bolt.transport.pipeline.WebSocketFrameTranslator;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

import java.util.List;
import java.util.function.Predicate;

import static com.mware.bolt.transport.pipeline.ProtocolHandshaker.BOLT_MAGIC_PREAMBLE;

public class TransportSelectionHandler extends ByteToMessageDecoder {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(NettyServer.class);
    private static final String WEBSOCKET_MAGIC = "GET ";
    private static final int MAX_WEBSOCKET_HANDSHAKE_SIZE = 65536;
    private static final int MAX_WEBSOCKET_FRAME_SIZE = 65536;

    private final BoltChannel boltChannel;
    private final SslContext sslCtx;
    private final boolean encryptionRequired;
    private final boolean isEncrypted;
    private final BoltProtocolFactory boltProtocolFactory;

    TransportSelectionHandler(BoltChannel boltChannel, SslContext sslCtx, boolean encryptionRequired, boolean isEncrypted,
                              BoltProtocolFactory boltProtocolFactory) {
        this.boltChannel = boltChannel;
        this.sslCtx = sslCtx;
        this.encryptionRequired = encryptionRequired;
        this.isEncrypted = isEncrypted;
        this.boltProtocolFactory = boltProtocolFactory;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Will use the first five bytes to detect a protocol.
        if (in.readableBytes() < 5) {
            return;
        }

        if (detectSsl(in)) {
            enableSsl(ctx);
        } else if (isHttp(in)) {
            switchToWebsocket(ctx);
        } else if (isBoltPreamble(in)) {
            switchToSocket(ctx);
        } else {
            // TODO: send a alert_message for a ssl connection to terminate the handshake
            in.clear();
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        try {
            // Netty throws a NativeIoException on connection reset - directly importing that class
            // caused a host of linking errors, because it depends on JNI to work. Hence, we just
            // test on the message we know we'll get.
            if (exceptionContains(cause, e -> e.getMessage().contains("Connection reset by peer"))) {
                LOGGER.warn("Fatal error occurred when initialising pipeline, " +
                        "remote peer unexpectedly closed connection: %s", ctx.channel());
            } else {
                LOGGER.error("Fatal error occurred when initialising pipeline: " + ctx.channel(), cause);
            }
        } finally {
            ctx.close();
        }
    }

    private boolean isBoltPreamble(ByteBuf in) {
        return in.getInt(0) == BOLT_MAGIC_PREAMBLE;
    }

    private boolean detectSsl(ByteBuf buf) {
        return sslCtx != null && SslHandler.isEncrypted(buf);
    }

    private boolean isHttp(ByteBuf buf) {
        for (int i = 0; i < WEBSOCKET_MAGIC.length(); ++i) {
            if (buf.getUnsignedByte(buf.readerIndex() + i) != WEBSOCKET_MAGIC.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private void enableSsl(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        p.addLast(sslCtx.newHandler(ctx.alloc()));
        p.addLast(new TransportSelectionHandler(boltChannel, null, encryptionRequired, true, boltProtocolFactory));
        p.remove(this);
    }

    private void switchToSocket(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        p.addLast(newHandshaker());
        p.remove(this);
    }

    private void switchToWebsocket(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        p.addLast(
                new HttpServerCodec(),
                new HttpObjectAggregator(MAX_WEBSOCKET_HANDSHAKE_SIZE),
                new WebSocketServerProtocolHandler("/", null, false, MAX_WEBSOCKET_FRAME_SIZE),
                new WebSocketFrameAggregator(MAX_WEBSOCKET_FRAME_SIZE),
                new WebSocketFrameTranslator(),
                newHandshaker());
        p.remove(this);
    }

    private ProtocolHandshaker newHandshaker() {
        return new ProtocolHandshaker(boltProtocolFactory, boltChannel, encryptionRequired, isEncrypted);
    }

    public static boolean exceptionContains(Throwable cause, Predicate<Throwable> toLookFor) {
        while (cause != null) {
            if (toLookFor.test(cause)) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
}
