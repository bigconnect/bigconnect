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

import com.mware.ge.collection.RawIterator;

import java.io.IOException;

/**
 * Joins multiple {@link CharReadable} into one. There will never be one read which reads from multiple sources.
 * If the end of one source is reached those (smaller amount of) characters are returned as one read and the next
 * read will start reading from the new source.
 * <p>
 * Newline will be injected in between two sources, even if the former doesn't end with such. This to not have the
 * last line in the former and first in the latter to look like one long line, if reading characters off of this
 * reader character by character (w/o knowing that there are multiple sources underneath).
 */
public class MultiReadable implements CharReadable {
    private final RawIterator<CharReadable, IOException> actual;

    private CharReadable current = CharReadable.EMPTY;
    private boolean requiresNewLine;
    private long previousPosition;

    public MultiReadable(RawIterator<CharReadable, IOException> readers) {
        this.actual = readers;
    }

    @Override
    public void close() throws IOException {
        closeCurrent();
    }

    private void closeCurrent() throws IOException {
        if (current != null) {
            current.close();
        }
    }

    @Override
    public String sourceDescription() {
        return current.sourceDescription();
    }

    @Override
    public long position() {
        return previousPosition + current.position();
    }

    private boolean goToNextSource() throws IOException {
        if (actual.hasNext()) {
            if (current != null) {
                previousPosition += current.position();
            }
            closeCurrent();
            current = actual.next();
            return true;
        }
        return false;
    }

    @Override
    public SectionedCharBuffer read(SectionedCharBuffer buffer, int from) throws IOException {
        while (true) {
            current.read(buffer, from);
            if (buffer.hasAvailable()) {
                // OK we read something from the current reader
                checkNewLineRequirement(buffer.array(), buffer.front() - 1);
                return buffer;
            }

            // Even if there's no line-ending at the end of this source we should introduce one
            // otherwise the last line of this source and the first line of the next source will
            // look like one long line.
            if (requiresNewLine) {
                buffer.append('\n');
                requiresNewLine = false;
                return buffer;
            }

            if (!goToNextSource()) {
                break;
            }
            from = buffer.pivot();
        }
        return buffer;
    }

    private void checkNewLineRequirement(char[] array, int lastIndex) {
        char lastChar = array[lastIndex];
        requiresNewLine = lastChar != '\n' && lastChar != '\r';
    }

    @Override
    public int read(char[] into, int offset, int length) throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            int read = current.read(into, offset + totalRead, length - totalRead);
            if (read == -1) {
                if (totalRead > 0) {
                    // Something has been read, but we couldn't fulfill the request with the current source.
                    // Return what we've read so far so that we don't mix multiple sources into the same read,
                    // for source traceability reasons.
                    return totalRead;
                }

                if (!goToNextSource()) {
                    break;
                }

                if (requiresNewLine) {
                    into[offset + totalRead] = '\n';
                    totalRead++;
                    requiresNewLine = false;
                }
            } else if (read > 0) {
                totalRead += read;
                checkNewLineRequirement(into, offset + totalRead - 1);
            }
        }
        return totalRead;
    }

    @Override
    public long length() {
        return current.length();
    }
}
