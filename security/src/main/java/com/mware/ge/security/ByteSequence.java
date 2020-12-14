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
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mware.ge.security;

/**
 * A sequence of bytes.
 */
public abstract class ByteSequence implements Comparable<ByteSequence> {

    /**
     * Gets a byte within this sequence.
     *
     * @param i index into sequence
     * @return byte
     * @throws IllegalArgumentException if i is out of range
     */
    public abstract byte byteAt(int i);

    /**
     * Gets the length of this sequence.
     *
     * @return sequence length
     */
    public abstract int length();

    /**
     * Returns a portion of this sequence.
     *
     * @param start index of subsequence start (inclusive)
     * @param end   index of subsequence end (exclusive)
     */
    public abstract ByteSequence subSequence(int start, int end);

    /**
     * Returns a byte array containing the bytes in this sequence. This method
     * may copy the sequence data or may return a backing byte array directly.
     *
     * @return byte array
     */
    public abstract byte[] toArray();

    /**
     * Determines whether this sequence is backed by a byte array.
     *
     * @return true if sequence is backed by a byte array
     */
    public abstract boolean isBackedByArray();

    /**
     * Gets the backing byte array for this sequence.
     *
     * @return byte array
     */
    public abstract byte[] getBackingArray();

    /**
     * Gets the offset for this sequence. This value represents the starting
     * point for the sequence in the backing array, if there is one.
     *
     * @return offset (inclusive)
     */
    public abstract int offset();

    /**
     * Compares the two given byte sequences, byte by byte, returning a negative,
     * zero, or positive result if the first sequence is less than, equal to, or
     * greater than the second. The comparison is performed starting with the
     * first byte of each sequence, and proceeds until a pair of bytes differs,
     * or one sequence runs out of byte (is shorter). A shorter sequence is
     * considered less than a longer one.
     *
     * @param bs1 first byte sequence to compare
     * @param bs2 second byte sequence to compare
     * @return comparison result
     */
    public static int compareBytes(ByteSequence bs1, ByteSequence bs2) {

        int minLen = Math.min(bs1.length(), bs2.length());

        for (int i = 0; i < minLen; i++) {
            int a = (bs1.byteAt(i) & 0xff);
            int b = (bs2.byteAt(i) & 0xff);

            if (a != b) {
                return a - b;
            }
        }

        return bs1.length() - bs2.length();
    }

    /**
     * Compares this byte sequence to another.
     *
     * @param obs byte sequence to compare
     * @return comparison result
     * @see #compareBytes(ByteSequence, ByteSequence)
     */
    public int compareTo(ByteSequence obs) {
        if (isBackedByArray() && obs.isBackedByArray()) {
            return WritableComparator.compareBytes(getBackingArray(), offset(), length(), obs.getBackingArray(), obs.offset(), obs.length());
        }

        return compareBytes(this, obs);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ByteSequence) {
            ByteSequence obs = (ByteSequence) o;

            if (this == o)
                return true;

            if (length() != obs.length())
                return false;

            return compareTo(obs) == 0;
        }

        return false;

    }

    @Override
    public int hashCode() {
        int hash = 1;
        if (isBackedByArray()) {
            byte[] data = getBackingArray();
            int end = offset() + length();
            for (int i = offset(); i < end; i++)
                hash = (31 * hash) + data[i];
        } else {
            for (int i = 0; i < length(); i++)
                hash = (31 * hash) + byteAt(i);
        }
        return hash;
    }

}
