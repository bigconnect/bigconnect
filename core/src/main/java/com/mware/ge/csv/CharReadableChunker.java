/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package com.mware.ge.csv;

import com.mware.ge.csv.Source.Chunk;

import java.io.IOException;
import java.util.Arrays;

/**
 * Chunks up a {@link CharReadable}.
 */
public abstract class CharReadableChunker implements Chunker
{
    protected final CharReadable reader;
    protected final int chunkSize;
    protected volatile long position;
    private char[] backBuffer; // grows on demand
    private int backBufferCursor;

    public CharReadableChunker(CharReadable reader, int chunkSize )
    {
        this.reader = reader;
        this.chunkSize = chunkSize;
        this.backBuffer = new char[chunkSize >> 4];
    }

    @Override
    public ChunkImpl newChunk()
    {
        return new ChunkImpl( new char[chunkSize] );
    }

    @Override
    public void close() throws IOException
    {
        reader.close();
    }

    @Override
    public long position()
    {
        return position;
    }

    protected int fillFromBackBuffer( char[] into )
    {
        if ( backBufferCursor > 0 )
        {   // Read from and reset back buffer
            assert backBufferCursor < chunkSize;
            System.arraycopy( backBuffer, 0, into, 0, backBufferCursor );
            int result = backBufferCursor;
            backBufferCursor = 0;
            return result;
        }
        return 0;
    }

    protected int storeInBackBuffer( char[] data, int offset, int length )
    {
        System.arraycopy( data, offset, backBuffer( length ), backBufferCursor, length );
        backBufferCursor += length;
        return length;
    }

    private char[] backBuffer( int length )
    {
        if ( backBufferCursor + length > backBuffer.length )
        {
            backBuffer = Arrays.copyOf( backBuffer, backBufferCursor + length );
        }
        return backBuffer;
    }

    public static class ChunkImpl implements Chunk
    {
        final char[] buffer;
        private int length;
        private String sourceDescription;

        public ChunkImpl( char[] buffer )
        {
            this.buffer = buffer;
        }

        public void initialize( int length, String sourceDescription )
        {
            this.length = length;
            this.sourceDescription = sourceDescription;
        }

        @Override
        public int startPosition()
        {
            return 0;
        }

        @Override
        public String sourceDescription()
        {
            return sourceDescription;
        }

        @Override
        public int maxFieldSize()
        {
            return buffer.length;
        }

        @Override
        public int length()
        {
            return length;
        }

        @Override
        public char[] data()
        {
            return buffer;
        }

        @Override
        public int backPosition()
        {
            return 0;
        }
    }
}
