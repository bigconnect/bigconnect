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
package com.mware.ge.store.util;

import com.mware.ge.security.ByteSequence;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ByteSequenceUtils {
    public static boolean equals(ByteSequence byteSequence, byte[] bytes) {
        if (byteSequence.length() != bytes.length) {
            return false;
        }
        if (byteSequence.isBackedByArray() && byteSequence.offset() == 0) {
            return Arrays.equals(bytes, byteSequence.getBackingArray());
        }

        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != byteSequence.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    public static String toString(ByteSequence byteSequence) {
        return new String(
                byteSequence.getBackingArray(),
                byteSequence.offset(),
                byteSequence.length(),
                StandardCharsets.UTF_8
        );
    }

    public static int indexOf(ByteSequence bytes, byte b) {
        return indexOf(bytes, b, 0);
    }

    public static int indexOf(ByteSequence bytes, byte b, int startIndex) {
        // TODO utf-8 encoding issues?
        for (int i = startIndex; i < bytes.length(); i++) {
            if (bytes.byteAt(i) == b) {
                return i;
            }
        }
        return -1;
    }

    public static ByteSequence subSequence(ByteSequence bytes, int startIndex) {
        return subSequence(bytes, startIndex, bytes.length());
    }

    public static ByteSequence subSequence(ByteSequence bytes, int startIndex, int endIndex) {
        return bytes.subSequence(startIndex, endIndex);
    }

    public static void putIntoByteBuffer(ByteSequence bytes, ByteBuffer bb) {
        bb.put(bytes.getBackingArray(), bytes.offset(), bytes.length());
    }

    public static byte[] getBytes(ByteSequence byteSequence) {
        return byteSequence.toArray();
    }
}
