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
package com.mware.ge.elasticsearch5.lucene;

import java.io.IOException;
import java.io.Reader;

public final class FastCharStream implements CharStream {
    char[] buffer = null;

    int bufferLength = 0; // end of valid chars
    int bufferPosition = 0; // next char to read

    int tokenStart = 0; // offset in buffer
    int bufferStart = 0; // position in file of buffer

    Reader input; // source of chars

    /**
     * Constructs from a Reader.
     */
    public FastCharStream(Reader r) {
        input = r;
    }

    @Override
    public final char readChar() throws IOException {
        if (bufferPosition >= bufferLength) {
            refill();
        }
        return buffer[bufferPosition++];
    }

    private final void refill() throws IOException {
        int newPosition = bufferLength - tokenStart;

        if (tokenStart == 0) { // token won't fit in buffer
            if (buffer == null) { // first time: alloc buffer
                buffer = new char[2048];
            } else if (bufferLength == buffer.length) { // grow buffer
                char[] newBuffer = new char[buffer.length * 2];
                System.arraycopy(buffer, 0, newBuffer, 0, bufferLength);
                buffer = newBuffer;
            }
        } else { // shift token to front
            System.arraycopy(buffer, tokenStart, buffer, 0, newPosition);
        }

        bufferLength = newPosition; // update state
        bufferPosition = newPosition;
        bufferStart += tokenStart;
        tokenStart = 0;

        int charsRead = // fill space in buffer
            input.read(buffer, newPosition, buffer.length - newPosition);
        if (charsRead == -1) {
            throw new IOException("read past eof");
        } else {
            bufferLength += charsRead;
        }
    }

    @Override
    public final char BeginToken() throws IOException {
        tokenStart = bufferPosition;
        return readChar();
    }

    @Override
    public final void backup(int amount) {
        bufferPosition -= amount;
    }

    @Override
    public final String GetImage() {
        return new String(buffer, tokenStart, bufferPosition - tokenStart);
    }

    @Override
    public final char[] GetSuffix(int len) {
        char[] value = new char[len];
        System.arraycopy(buffer, bufferPosition - len, value, 0, len);
        return value;
    }

    @Override
    public final void Done() {
        try {
            input.close();
        } catch (IOException e) {
        }
    }

    @Override
    @Deprecated
    public final int getColumn() {
        return bufferStart + bufferPosition;
    }

    @Override
    @Deprecated
    public final int getLine() {
        return 1;
    }

    @Override
    public final int getEndColumn() {
        return bufferStart + bufferPosition;
    }

    @Override
    public final int getEndLine() {
        return 1;
    }

    @Override
    public final int getBeginColumn() {
        return bufferStart + tokenStart;
    }

    @Override
    public final int getBeginLine() {
        return 1;
    }
}
