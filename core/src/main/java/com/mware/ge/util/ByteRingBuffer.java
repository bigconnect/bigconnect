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
package com.mware.ge.util;

import static com.mware.ge.util.Preconditions.checkArgument;

public class ByteRingBuffer {
    private byte[] buffer;
    private int position;
    private int used;

    public ByteRingBuffer(int size) {
        checkArgument(size > 0, "size must be greater than 0");
        buffer = new byte[size];
    }

    public void resize(int newSize) {
        checkArgument(newSize >= getUsed(), "cannot resize smaller than data used");
        byte[] newBuffer = new byte[newSize];
        int newUsed = read(newBuffer, 0, newSize);
        buffer = newBuffer;
        position = 0;
        used = newUsed;
    }

    public int getSize() {
        return buffer.length;
    }

    public int getFree() {
        return getSize() - getUsed();
    }

    public int getUsed() {
        return used;
    }

    public void clear() {
        position = 0;
        used = 0;
    }

    public int write(byte[] buf, int pos, int len) {
        checkArgument(len >= 0, "len must be >= 0");
        checkArgument(len <= getFree(), "out of free space");
        if (used == 0) {
            position = 0;
        }
        int p1 = position + used;
        if (p1 < buffer.length) { // free space in two pieces
            int part1Len = Math.min(len, buffer.length - p1);
            append(buf, pos, part1Len);
            int part2Len = Math.min(len - part1Len, position);
            append(buf, pos + part1Len, part2Len);
            return part1Len + part2Len;
        } else { // free space in one piece
            append(buf, pos, len);
            return len;
        }
    }

    public int write(byte[] buf) {
        return write(buf, 0, buf.length);
    }

    private void append(byte[] buf, int pos, int len) {
        checkArgument(len >= 0, "len must be >= 0");
        if (len == 0) {
            return;
        }
        int p = clip(position + used);
        System.arraycopy(buf, pos, buffer, p, len);
        used += len;
    }

    public int read(byte[] buf, int pos, int len) {
        checkArgument(len >= 0, "len must be >=0");
        if (getUsed() == 0) {
            return -1;
        }
        int part1Length = Math.min(len, Math.min(used, buffer.length - position));
        remove(buf, pos, part1Length);
        int part2Length = Math.min(len - part1Length, used);
        remove(buf, pos + part1Length, part2Length);
        return part1Length + part2Length;
    }

    public int read(byte[] buf) {
        return read(buf, 0, buf.length);
    }

    public int read() {
        if (getUsed() < 1) {
            return -1;
        }
        byte[] buffer = new byte[1];
        read(buffer);
        return buffer[0];
    }

    private void remove(byte[] buf, int pos, int len) {
        checkArgument(len >= 0, "len must be >= 0");
        if (len == 0) {
            return;
        }
        System.arraycopy(buffer, position, buf, pos, len);
        position = clip(position + len);
        used -= len;
    }

    private int clip(int p) {
        return (p < buffer.length) ? p : (p - buffer.length);
    }
}
