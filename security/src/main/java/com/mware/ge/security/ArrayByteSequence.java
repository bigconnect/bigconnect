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

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * An implementation of {@link ByteSequence} that uses a backing byte array.
 */
public class ArrayByteSequence extends ByteSequence implements Serializable {

    private static final long serialVersionUID = 1L;

    protected byte data[];
    protected int offset;
    protected int length;

    /**
     * Creates a new sequence. The given byte array is used directly as the
     * backing array, so later changes made to the array reflect into the new
     * sequence.
     *
     * @param data byte data
     */
    public ArrayByteSequence(byte data[]) {
        this.data = data;
        this.offset = 0;
        this.length = data.length;
    }

    /**
     * Creates a new sequence from a subsequence of the given byte array. The
     * given byte array is used directly as the backing array, so later changes
     * made to the (relevant portion of the) array reflect into the new sequence.
     *
     * @param data   byte data
     * @param offset starting offset in byte array (inclusive)
     * @param length number of bytes to include in sequence
     * @throws IllegalArgumentException if the offset or length are out of bounds
     *                                  for the given byte array
     */
    public ArrayByteSequence(byte data[], int offset, int length) {

        if (offset < 0 || offset > data.length || length < 0 || (offset + length) > data.length) {
            throw new IllegalArgumentException(" Bad offset and/or length data.length = " + data.length + " offset = " + offset + " length = " + length);
        }

        this.data = data;
        this.offset = offset;
        this.length = length;

    }

    /**
     * Creates a new sequence from the given string. The bytes are determined from
     * the string using the default platform encoding.
     *
     * @param s string to represent as bytes
     */
    public ArrayByteSequence(String s) {
        this(s.getBytes(Constants.UTF8));
    }

    /**
     * Creates a new sequence based on a byte buffer. If the byte buffer has an
     * array, that array (and the buffer's offset and limit) are used; otherwise,
     * a new backing array is created and a relative bulk get is performed to
     * transfer the buffer's contents (starting at its current position and
     * not beyond its limit).
     *
     * @param buffer byte buffer
     */
    public ArrayByteSequence(ByteBuffer buffer) {
        if (buffer.hasArray()) {
            this.data = buffer.array();
            this.offset = buffer.arrayOffset();
            this.length = buffer.limit();
        } else {
            this.data = new byte[buffer.remaining()];
            this.offset = 0;
            buffer.get(data);
        }
    }

    @Override
    public byte byteAt(int i) {

        if (i < 0) {
            throw new IllegalArgumentException("i < 0, " + i);
        }

        if (i >= length) {
            throw new IllegalArgumentException("i >= length, " + i + " >= " + length);
        }

        return data[offset + i];
    }

    @Override
    public byte[] getBackingArray() {
        return data;
    }

    @Override
    public boolean isBackedByArray() {
        return true;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public int offset() {
        return offset;
    }

    @Override
    public ByteSequence subSequence(int start, int end) {

        if (start > end || start < 0 || end > length) {
            throw new IllegalArgumentException("Bad start and/end start = " + start + " end=" + end + " offset=" + offset + " length=" + length);
        }

        return new ArrayByteSequence(data, offset + start, end - start);
    }

    @Override
    public byte[] toArray() {
        if (offset == 0 && length == data.length)
            return data;

        byte[] copy = new byte[length];
        System.arraycopy(data, offset, copy, 0, length);
        return copy;
    }

    public String toString() {
        return new String(data, offset, length, Constants.UTF8);
    }
}
