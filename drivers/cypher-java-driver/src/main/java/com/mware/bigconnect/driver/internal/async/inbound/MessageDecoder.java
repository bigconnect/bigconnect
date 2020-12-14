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
package com.mware.bigconnect.driver.internal.async.inbound;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class MessageDecoder extends ByteToMessageDecoder
{
    private static final Cumulator DEFAULT_CUMULATOR = determineDefaultCumulator();

    private boolean readMessageBoundary;

    public MessageDecoder()
    {
        setCumulator( DEFAULT_CUMULATOR );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        if ( msg instanceof ByteBuf )
        {
            // on every read check if input buffer is empty or not
            // if it is empty then it's a message boundary and full message is in the buffer
            readMessageBoundary = ((ByteBuf) msg).readableBytes() == 0;
        }
        super.channelRead( ctx, msg );
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
    {
        if ( readMessageBoundary )
        {
            // now we have a complete message in the input buffer

            // increment ref count of the buffer and create it's duplicate that shares the content
            // duplicate will be the output of this decoded and input for the next one
            ByteBuf messageBuf = in.retainedDuplicate();

            // signal that whole message was read by making input buffer seem like it was fully read/consumed
            in.readerIndex( in.readableBytes() );

            // pass the full message to the next handler in the pipeline
            out.add( messageBuf );

            readMessageBoundary = false;
        }
    }

    private static Cumulator determineDefaultCumulator()
    {
        String value = System.getProperty( "messageDecoderCumulator", "" );
        if ( "merge".equals( value ) )
        {
            return MERGE_CUMULATOR;
        }
        return COMPOSITE_CUMULATOR;
    }
}
