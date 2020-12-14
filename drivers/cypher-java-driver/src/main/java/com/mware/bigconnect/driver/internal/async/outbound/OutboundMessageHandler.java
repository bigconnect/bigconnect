/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
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
package com.mware.bigconnect.driver.internal.async.outbound;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToMessageEncoder;
import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.internal.async.connection.BoltProtocolUtil;
import com.mware.bigconnect.driver.internal.logging.ChannelActivityLogger;
import com.mware.bigconnect.driver.internal.messaging.Message;
import com.mware.bigconnect.driver.internal.messaging.MessageFormat;

import java.util.List;

import static io.netty.buffer.ByteBufUtil.hexDump;

public class OutboundMessageHandler extends MessageToMessageEncoder<Message>
{
    public static final String NAME = OutboundMessageHandler.class.getSimpleName();
    private final ChunkAwareByteBufOutput output;
    private final MessageFormat.Writer writer;
    private final Logging logging;

    private Logger log;

    public OutboundMessageHandler( MessageFormat messageFormat, Logging logging )
    {
        this.output = new ChunkAwareByteBufOutput();
        this.writer = messageFormat.newWriter( output );
        this.logging = logging;
    }

    @Override
    public void handlerAdded( ChannelHandlerContext ctx )
    {
        log = new ChannelActivityLogger( ctx.channel(), logging, getClass() );
    }

    @Override
    public void handlerRemoved( ChannelHandlerContext ctx )
    {
        log = null;
    }

    @Override
    protected void encode( ChannelHandlerContext ctx, Message msg, List<Object> out )
    {
        log.debug( "C: %s", msg );

        ByteBuf messageBuf = ctx.alloc().ioBuffer();
        output.start( messageBuf );
        try
        {
            writer.write( msg );
            output.stop();
        }
        catch ( Throwable error )
        {
            output.stop();
            // release buffer because it will not get added to the out list and no other handler is going to handle it
            messageBuf.release();
            throw new EncoderException( "Failed to write outbound message: " + msg, error );
        }

        if ( log.isTraceEnabled() )
        {
            log.trace( "C: %s", hexDump( messageBuf ) );
        }

        BoltProtocolUtil.writeMessageBoundary( messageBuf );
        out.add( messageBuf );
    }
}
