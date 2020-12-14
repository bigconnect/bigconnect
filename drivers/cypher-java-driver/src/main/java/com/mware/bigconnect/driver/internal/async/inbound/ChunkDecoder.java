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
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.internal.logging.ChannelActivityLogger;

public class ChunkDecoder extends LengthFieldBasedFrameDecoder
{
    private static final int MAX_FRAME_BODY_LENGTH = 0xFFFF;
    private static final int LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_LENGTH = 2;
    private static final int LENGTH_ADJUSTMENT = 0;
    private static final int INITIAL_BYTES_TO_STRIP = LENGTH_FIELD_LENGTH;
    private static final int MAX_FRAME_LENGTH = LENGTH_FIELD_LENGTH + MAX_FRAME_BODY_LENGTH;

    private final Logging logging;
    private Logger log;

    public ChunkDecoder( Logging logging )
    {
        super( MAX_FRAME_LENGTH, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, LENGTH_ADJUSTMENT, INITIAL_BYTES_TO_STRIP );
        this.logging = logging;
    }

    @Override
    public void handlerAdded( ChannelHandlerContext ctx )
    {
        log = new ChannelActivityLogger( ctx.channel(), logging, getClass() );
    }

    @Override
    protected void handlerRemoved0( ChannelHandlerContext ctx )
    {
        log = null;
    }

    @Override
    protected ByteBuf extractFrame( ChannelHandlerContext ctx, ByteBuf buffer, int index, int length )
    {
        if ( log.isTraceEnabled() )
        {
            int originalReaderIndex = buffer.readerIndex();
            int readerIndexWithChunkHeader = originalReaderIndex - INITIAL_BYTES_TO_STRIP;
            int lengthWithChunkHeader = INITIAL_BYTES_TO_STRIP + length;
            String hexDump = ByteBufUtil.hexDump( buffer, readerIndexWithChunkHeader, lengthWithChunkHeader );
            log.trace( "S: %s", hexDump );
        }
        return super.extractFrame( ctx, buffer, index, length );
    }
}
