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
package com.mware.ge.store.mutations;

import com.mware.ge.security.ByteBufferUtil;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A utility class for reading and writing bytes to byte buffers without synchronization.
 */
public class UnsynchronizedBuffer {
    // created this little class instead of using ByteArrayOutput stream and DataOutputStream
    // because both are synchronized... lots of small syncs slow things down

    /**
     * A byte buffer writer.
     */
    public static class Writer {

        int offset = 0;
        byte data[];

        /**
         * Creates a new writer.
         */
        public Writer() {
            data = new byte[64];
        }

        /**
         * Creates a new writer.
         *
         * @param initialCapacity initial byte capacity
         */
        public Writer(int initialCapacity) {
            data = new byte[initialCapacity];
        }

        private void reserve(int l) {
            if (offset + l > data.length) {
                int newSize = UnsynchronizedBuffer.nextArraySize(offset + l);

                byte[] newData = new byte[newSize];
                System.arraycopy(data, 0, newData, 0, offset);
                data = newData;
            }

        }

        /**
         * Adds bytes to this writer's buffer.
         *
         * @param bytes  byte array
         * @param off    offset into array to start copying bytes
         * @param length number of bytes to add
         * @throws IndexOutOfBoundsException if off or length are invalid
         */
        public void add(byte[] bytes, int off, int length) {
            reserve(length);
            System.arraycopy(bytes, off, data, offset, length);
            offset += length;
        }

        /**
         * Adds a Boolean value to this writer's buffer.
         *
         * @param b Boolean value
         */
        public void add(boolean b) {
            reserve(1);
            if (b)
                data[offset++] = 1;
            else
                data[offset++] = 0;
        }

        /**
         * Gets (a copy of) the contents of this writer's buffer.
         *
         * @return byte buffer contents
         */
        public byte[] toArray() {
            byte ret[] = new byte[offset];
            System.arraycopy(data, 0, ret, 0, offset);
            return ret;
        }

        /**
         * Gets a <code>ByteBuffer</code> wrapped around this writer's buffer.
         *
         * @return byte buffer
         */
        public ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(data, 0, offset);
        }

        /**
         * Adds an integer value to this writer's buffer. The integer is encoded as a variable-length
         * list of bytes. See {@link #writeVLong(long)} for a description of the encoding.
         *
         * @param i integer value
         */
        public void writeVInt(int i) {
            writeVLong(i);
        }

        /**
         * Adds a long value to this writer's buffer. The long is encoded as a variable-length list of
         * bytes. For a description of the encoding scheme, see <code>WritableUtils.writeVLong()</code>
         * in the Hadoop API. [<a href=
         * "http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/io/WritableUtils.html#writeVLong%28java.io.DataOutput,%20long%29">link</a>]
         *
         * @param i long value
         */
        public void writeVLong(long i) {
            reserve(9);
            offset = UnsynchronizedBuffer.writeVLong(data, offset, i);
        }
    }

    /**
     * A byte buffer reader.
     */
    public static class Reader {
        int offset;
        byte data[];

        /**
         * Creates a new reader.
         *
         * @param b bytes to read
         */
        public Reader(byte b[]) {
            this.data = b;
        }

        /**
         * Creates a new reader.
         *
         * @param buffer byte buffer containing bytes to read
         */
        public Reader(ByteBuffer buffer) {
            if (buffer.hasArray() && buffer.array().length == buffer.arrayOffset() + buffer.limit()) {
                offset = buffer.arrayOffset() + buffer.position();
                data = buffer.array();
            } else {
                offset = 0;
                data = ByteBufferUtil.toBytes(buffer);
            }
        }

        /**
         * Reads an integer value from this reader's buffer.
         *
         * @return integer value
         */
        public int readInt() {
            return (data[offset++] << 24) + ((data[offset++] & 255) << 16) + ((data[offset++] & 255) << 8)
                    + ((data[offset++] & 255) << 0);
        }

        /**
         * Reads a long value from this reader's buffer.
         *
         * @return long value
         */
        public long readLong() {
            return (((long) data[offset++] << 56) + ((long) (data[offset++] & 255) << 48)
                    + ((long) (data[offset++] & 255) << 40) + ((long) (data[offset++] & 255) << 32)
                    + ((long) (data[offset++] & 255) << 24) + ((data[offset++] & 255) << 16)
                    + ((data[offset++] & 255) << 8) + ((data[offset++] & 255) << 0));
        }

        /**
         * Reads bytes from this reader's buffer, filling the given byte array.
         *
         * @param b byte array to fill
         */
        public void readBytes(byte b[]) {
            System.arraycopy(data, offset, b, 0, b.length);
            offset += b.length;
        }

        /**
         * Reads a Boolean value from this reader's buffer.
         *
         * @return Boolean value
         */
        public boolean readBoolean() {
            return (data[offset++] == 1);
        }

        /**
         * Reads an integer value from this reader's buffer, assuming the integer was encoded as a
         * variable-length list of bytes.
         *
         * @return integer value
         */
        public int readVInt() {
            return (int) readVLong();
        }

        /**
         * Reads a long value from this reader's buffer, assuming the long was encoded as a
         * variable-length list of bytes.
         *
         * @return long value
         */
        public long readVLong() {
            byte firstByte = data[offset++];
            int len = decodeVIntSize(firstByte);
            if (len == 1) {
                return firstByte;
            }
            long i = 0;
            for (int idx = 0; idx < len - 1; idx++) {
                byte b = data[offset++];
                i = i << 8;
                i = i | (b & 0xFF);
            }
            return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
        }
    }

    /**
     * Given the first byte of a vint/vlong, determine the sign
     *
     * @param value the first byte
     * @return is the value negative
     */
    public static boolean isNegativeVInt(byte value) {
        return value < -120 || (value >= -112 && value < 0);
    }


    /**
     * Parse the first byte of a vint/vlong to determine the number of bytes
     *
     * @param value the first byte of the vint/vlong
     * @return the total number of bytes (1 to 9)
     */
    public static int decodeVIntSize(byte value) {
        if (value >= -112) {
            return 1;
        } else if (value < -120) {
            return -119 - value;
        }
        return -111 - value;
    }

    /**
     * Determines what next array size should be by rounding up to next power of two.
     *
     * @param i current array size
     * @return next array size
     * @throws IllegalArgumentException if i is negative
     */
    public static int nextArraySize(int i) {
        if (i < 0)
            throw new IllegalArgumentException();

        if (i > (1 << 30))
            return Integer.MAX_VALUE; // this is the next power of 2 minus one... a special case

        if (i == 0) {
            return 1;
        }

        // round up to next power of two
        int ret = i;
        ret--;
        ret |= ret >> 1;
        ret |= ret >> 2;
        ret |= ret >> 4;
        ret |= ret >> 8;
        ret |= ret >> 16;
        ret++;

        return ret;
    }

    /**
     * Use the provided byte[] to buffer only the bytes used to write out the integer i to the
     * DataOutput out. This will only ever make one write call to the DataOutput.
     */
    public static void writeVInt(DataOutput out, byte[] workBuffer, int i) throws IOException {
        int size = UnsynchronizedBuffer.writeVInt(workBuffer, 0, i);
        out.write(workBuffer, 0, size);
    }

    /**
     * Use the provided byte[] to buffer only the bytes used to write out the long i to the DataOutput
     * out. This will only ever make one write call to the DataOutput.
     */
    public static void writeVLong(DataOutput out, byte[] workBuffer, long i) throws IOException {
        int size = UnsynchronizedBuffer.writeVLong(workBuffer, 0, i);
        out.write(workBuffer, 0, size);
    }

    /**
     * Writes a variable int directly to a byte array.
     */
    public static int writeVInt(byte[] dest, int offset, int i) {
        return writeVLong(dest, offset, i);
    }

    /**
     * Writes a variable long directly to a byte array.
     *
     * @param dest   The destination array for the long to be written to
     * @param offset The location where to write the long to
     * @param value  The long value being written into byte array
     * @return Returns the new offset location
     */
    public static int writeVLong(byte[] dest, int offset, long value) {
        if (value >= -112 && value <= 127) {
            dest[offset++] = (byte) value;
            return offset;
        }

        int len = -112;
        if (value < 0) {
            value ^= -1L; // take one's complement'
            len = -120;
        }

        long tmp = value;
        while (tmp != 0) {
            tmp = tmp >> 8;
            len--;
        }

        dest[offset++] = (byte) len;

        len = (len < -120) ? -(len + 120) : -(len + 112);

        for (int idx = len; idx != 0; idx--) {
            int shiftbits = (idx - 1) * 8;
            long mask = 0xFFL << shiftbits;
            dest[offset++] = (byte) ((value & mask) >> shiftbits);
        }
        return offset;
    }
}
