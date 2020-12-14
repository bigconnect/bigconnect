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
import com.mware.bigconnect.driver.internal.async.connection.BoltProtocolUtil;
import com.mware.bigconnect.driver.internal.packstream.PackOutput;

import static java.util.Objects.requireNonNull;
import static com.mware.bigconnect.driver.internal.async.connection.BoltProtocolUtil.CHUNK_HEADER_SIZE_BYTES;
import static com.mware.bigconnect.driver.internal.async.connection.BoltProtocolUtil.DEFAULT_MAX_OUTBOUND_CHUNK_SIZE_BYTES;

public class ChunkAwareByteBufOutput implements PackOutput
{
    private final int maxChunkSize;

    private ByteBuf buf;
    private int currentChunkStartIndex;
    private int currentChunkSize;

    public ChunkAwareByteBufOutput()
    {
        this( DEFAULT_MAX_OUTBOUND_CHUNK_SIZE_BYTES );
    }

    ChunkAwareByteBufOutput( int maxChunkSize )
    {
        this.maxChunkSize = verifyMaxChunkSize( maxChunkSize );
    }

    public void start( ByteBuf newBuf )
    {
        assertNotStarted();
        buf = requireNonNull( newBuf );
        startNewChunk( 0 );
    }

    public void stop()
    {
        writeChunkSizeHeader();
        buf = null;
        currentChunkStartIndex = 0;
        currentChunkSize = 0;
    }

    @Override
    public PackOutput writeByte( byte value )
    {
        ensureCanFitInCurrentChunk( 1 );
        buf.writeByte( value );
        currentChunkSize += 1;
        return this;
    }

    @Override
    public PackOutput writeBytes( byte[] data )
    {
        int offset = 0;
        int length = data.length;
        while ( offset < length )
        {
            // Ensure there is an open chunk, and that it has at least one byte of space left
            ensureCanFitInCurrentChunk( 1 );

            // Write as much as we can into the current chunk
            int amountToWrite = Math.min( availableBytesInCurrentChunk(), length - offset );

            buf.writeBytes( data, offset, amountToWrite );
            currentChunkSize += amountToWrite;
            offset += amountToWrite;
        }
        return this;
    }

    @Override
    public PackOutput writeShort( short value )
    {
        ensureCanFitInCurrentChunk( 2 );
        buf.writeShort( value );
        currentChunkSize += 2;
        return this;
    }

    @Override
    public PackOutput writeInt( int value )
    {
        ensureCanFitInCurrentChunk( 4 );
        buf.writeInt( value );
        currentChunkSize += 4;
        return this;
    }

    @Override
    public PackOutput writeLong( long value )
    {
        ensureCanFitInCurrentChunk( 8 );
        buf.writeLong( value );
        currentChunkSize += 8;
        return this;
    }

    @Override
    public PackOutput writeDouble( double value )
    {
        ensureCanFitInCurrentChunk( 8 );
        buf.writeDouble( value );
        currentChunkSize += 8;
        return this;
    }

    private void ensureCanFitInCurrentChunk( int numberOfBytes )
    {
        int targetChunkSize = currentChunkSize + numberOfBytes;
        if ( targetChunkSize > maxChunkSize )
        {
            writeChunkSizeHeader();
            startNewChunk( buf.writerIndex() );
        }
    }

    private void startNewChunk( int index )
    {
        currentChunkStartIndex = index;
        BoltProtocolUtil.writeEmptyChunkHeader( buf );
        currentChunkSize = CHUNK_HEADER_SIZE_BYTES;
    }

    private void writeChunkSizeHeader()
    {
        // go to the beginning of the chunk and write the size header
        int chunkBodySize = currentChunkSize - CHUNK_HEADER_SIZE_BYTES;
        BoltProtocolUtil.writeChunkHeader( buf, currentChunkStartIndex, chunkBodySize );
    }

    private int availableBytesInCurrentChunk()
    {
        return maxChunkSize - currentChunkSize;
    }

    private void assertNotStarted()
    {
        if ( buf != null )
        {
            throw new IllegalStateException( "Already started" );
        }
    }

    private static int verifyMaxChunkSize( int maxChunkSize )
    {
        if ( maxChunkSize <= 0 )
        {
            throw new IllegalArgumentException( "Max chunk size should be > 0, given: " + maxChunkSize );
        }
        return maxChunkSize;
    }
}
