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
package com.mware.ge.accumulo.iterator.util;

import org.apache.hadoop.io.Text;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DataInputStreamUtils {
    public static Text decodeText(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length == 0) {
            return new Text();
        }
        byte[] data = new byte[length];
        int read = in.read(data, 0, length);
        if (read != length) {
            throw new IOException("Unexpected data length expected " + length + " found " + read);
        }
        return new Text(data);
    }

    public static String decodeString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length == 0) {
            return "";
        }
        byte[] data = new byte[length];
        int read = in.read(data, 0, length);
        if (read != length) {
            throw new IOException("Unexpected data length expected " + length + " found " + read);
        }
        return new String(data, DataOutputStreamUtils.CHARSET);
    }

    public static byte[] decodeByteArray(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        byte[] data = new byte[len];
        int read = in.read(data);
        if (read != len) {
            throw new IOException("Unexpected read length. Expected " + len + " found " + read);
        }
        return data;
    }

    public static int[] decodeIntArray(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        int[] data = new int[len];
        for (int i = 0; i < len; i++) {
            data[i] = in.readInt();
        }
        return data;
    }

    public static ByteArrayWrapper decodeByteArrayWrapper(DataInputStream in) throws IOException {
        byte[] result = decodeByteArray(in);
        if (result == null) {
            return null;
        }
        return new ByteArrayWrapper(result);
    }

    public static Set<String> decodeSetOfStrings(DataInputStream in) throws IOException {
        int count = in.readInt();
        Set<String> results = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            results.add(decodeString(in));
        }
        return results;
    }
}
