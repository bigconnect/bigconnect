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

import java.io.Closeable;
import java.io.IOException;

/**
 * Takes a bigger stream of data and chunks it up into smaller chunks. The {@link Chunk chunks} are allocated
 * explicitly and are passed into {@link #nextChunk(Chunk)} to be filled/assigned with data representing
 * next chunk from the stream. This design allows for efficient reuse of chunks when there are multiple concurrent
 * processors, each processing chunks of data.
 */
public interface Chunker extends Closeable
{
    /**
     * @return a new allocated {@link Chunk} which is to be later passed into {@link #nextChunk(Chunk)}
     * to fill it with data. When a {@link Chunk} has been fully processed then it can be passed into
     * {@link #nextChunk(Chunk)} again to get more data.
     */
    Chunk newChunk();

    /**
     * Fills a previously {@link #newChunk() allocated chunk} with data to be processed after completion
     * of this call.
     *
     * @param chunk {@link Chunk} to fill with data.
     * @return {@code true} if at least some amount of data was passed into the given {@link Chunk},
     * otherwise {@code false} denoting the end of the stream.
     * @throws IOException on I/O error.
     */
    boolean nextChunk(Chunk chunk) throws IOException;

    /**
     * @return byte position of how much data has been returned from {@link #nextChunk(Chunk)}.
     */
    long position();
}
