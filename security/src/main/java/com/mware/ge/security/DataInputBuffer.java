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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;

/**
 * A reusable {@link DataInput} implementation that reads from an in-memory
 * buffer.
 * <p/>
 * <p>This saves memory over creating a new DataInputStream and
 * ByteArrayInputStream each time data is read.
 * <p/>
 * <p>Typical usage is something like the following:<pre>
 * <p/>
 * DataInputBuffer buffer = new DataInputBuffer();
 * while (... loop condition ...) {
 *   byte[] data = ... get data ...;
 *   int dataLength = ... get data length ...;
 *   buffer.reset(data, dataLength);
 *   ... read buffer using DataInput methods ...
 * }
 * </pre>
 */
class DataInputBuffer extends DataInputStream {
    private static class Buffer extends ByteArrayInputStream {
        public Buffer() {
            super(new byte[]{});
        }

        public void reset(byte[] input, int start, int length) {
            this.buf = input;
            this.count = start + length;
            this.mark = start;
            this.pos = start;
        }

        public byte[] getData() {
            return buf;
        }

        public int getPosition() {
            return pos;
        }

        public int getLength() {
            return count;
        }
    }

    private Buffer buffer;

    /**
     * Constructs a new empty buffer.
     */
    public DataInputBuffer() {
        this(new Buffer());
    }

    private DataInputBuffer(Buffer buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    /**
     * Resets the data that the buffer reads.
     */
    public void reset(byte[] input, int length) {
        buffer.reset(input, 0, length);
    }

    /**
     * Resets the data that the buffer reads.
     */
    public void reset(byte[] input, int start, int length) {
        buffer.reset(input, start, length);
    }

    public byte[] getData() {
        return buffer.getData();
    }

    /**
     * Returns the current position in the input.
     */
    public int getPosition() {
        return buffer.getPosition();
    }

    /**
     * Returns the length of the input.
     */
    public int getLength() {
        return buffer.getLength();
    }

}
