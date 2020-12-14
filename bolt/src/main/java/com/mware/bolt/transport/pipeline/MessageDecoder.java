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
package com.mware.bolt.transport.pipeline;

import com.mware.bolt.messaging.BigConnectPack;
import com.mware.bolt.messaging.BoltRequestMessageReader;
import com.mware.bolt.messaging.UnpackerProvider;
import com.mware.bolt.v1.packstream.ByteBufInput;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import static io.netty.buffer.ByteBufUtil.hexDump;

public class MessageDecoder extends SimpleChannelInboundHandler<ByteBuf> {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(MessageDecoder.class);

    private final ByteBufInput input;
    private final BigConnectPack.Unpacker unpacker;
    private final BoltRequestMessageReader reader;

    public MessageDecoder(UnpackerProvider unpackProvider, BoltRequestMessageReader reader) {
        this.input = new ByteBufInput();
        this.unpacker = unpackProvider.newUnpacker(input);
        this.reader = reader;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {
        input.start(byteBuf);
        byteBuf.markReaderIndex();
        try {
            reader.read(unpacker);
        } catch (Throwable error) {
            logMessageOnError(byteBuf);
            throw error;
        } finally {
            input.stop();
        }
    }

    private void logMessageOnError(ByteBuf byteBuf) {
        // move reader index back to the beginning of the message in order to log its full content
        byteBuf.resetReaderIndex();
        LOGGER.error("Failed to read an inbound message:\n" + hexDump(byteBuf) + '\n');
    }
}

