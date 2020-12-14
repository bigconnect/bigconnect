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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ByteBufferUtil {
    public static byte[] toBytes(ByteBuffer buffer) {
        if (buffer == null)
            return null;
        if (buffer.hasArray()) {
            // did not use buffer.get() because it changes the position
            return Arrays.copyOfRange(buffer.array(), buffer.position() + buffer.arrayOffset(),
                    buffer.limit() + buffer.arrayOffset());
        } else {
            byte[] data = new byte[buffer.remaining()];
            // duplicate inorder to avoid changing position
            buffer.duplicate().get(data);
            return data;
        }
    }

    public static List<ByteBuffer> toByteBuffers(Collection<byte[]> bytesList) {
        if (bytesList == null)
            return null;
        ArrayList<ByteBuffer> result = new ArrayList<ByteBuffer>();
        for (byte[] bytes : bytesList) {
            result.add(ByteBuffer.wrap(bytes));
        }
        return result;
    }

    public static List<byte[]> toBytesList(Collection<ByteBuffer> bytesList) {
        if (bytesList == null)
            return null;
        ArrayList<byte[]> result = new ArrayList<>(bytesList.size());
        for (ByteBuffer bytes : bytesList) {
            result.add(toBytes(bytes));
        }
        return result;
    }

    public static String toString(ByteBuffer bytes) {
        if (bytes.hasArray()) {
            return new String(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining(),
                    UTF_8);
        } else {
            return new String(toBytes(bytes), UTF_8);
        }
    }

    public static ByteBuffer toByteBuffers(ByteSequence bs) {
        if (bs == null)
            return null;

        if (bs.isBackedByArray()) {
            return ByteBuffer.wrap(bs.getBackingArray(), bs.offset(), bs.length());
        } else {
            // TODO create more efficient impl
            return ByteBuffer.wrap(bs.toArray());
        }
    }
}
