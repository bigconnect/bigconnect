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

/**
 * In a scenario where there's one reader reading chunks of data, handing those chunks to one or
 * more processors (parsers) of that data, this class comes in handy. This pattern allows for
 * multiple {@link BufferedCharSeeker seeker instances}, each operating over one chunk, not transitioning itself
 * into the next.
 */
public class ClosestNewLineChunker extends CharReadableChunker
{
    public ClosestNewLineChunker( CharReadable reader, int chunkSize )
    {
        super( reader, chunkSize );
    }

    /**
     * Fills the given chunk with data from the underlying {@link CharReadable}, up to a good cut-off point
     * in the vicinity of the buffer size.
     *
     * @param chunk {@link Chunk} to read data into.
     * @return the next {@link Chunk} of data, ending with a new-line or not for the last chunk.
     * @throws IOException on reading error.
     */
    @Override
    public synchronized boolean nextChunk( Chunk chunk ) throws IOException
    {
        ChunkImpl into = (ChunkImpl) chunk;
        int offset = fillFromBackBuffer( into.buffer );
        int leftToRead = chunkSize - offset;
        int read = reader.read( into.buffer, offset, leftToRead );
        if ( read == leftToRead )
        {   // Read from reader. We read data into the whole buffer and there seems to be more data left in reader.
            // This means we're most likely not at the end so seek backwards to the last newline character and
            // put the characters after the newline character(s) into the back buffer.
            int newlineOffset = offsetOfLastNewline( into.buffer );
            if ( newlineOffset > -1 )
            {   // We found a newline character some characters back
                read -= storeInBackBuffer( into.data(), newlineOffset + 1, chunkSize - (newlineOffset + 1) );
            }
            else
            {   // There was no newline character, isn't that weird?
                throw new IllegalStateException( "Weird input data, no newline character in the whole buffer " +
                        chunkSize + ", not supported a.t.m." );
            }
        }
        // else we couldn't completely fill the buffer, this means that we're at the end of a data source, we're good.

        if ( read > 0 )
        {
            offset += read;
            position += read;
            into.initialize( offset, reader.sourceDescription() );
            return true;
        }
        return false;
    }

    private static int offsetOfLastNewline( char[] buffer )
    {
        for ( int i = buffer.length - 1; i >= 0; i-- )
        {
            if ( buffer[i] == '\n' )
            {
                return i;
            }
        }
        return -1;
    }
}
