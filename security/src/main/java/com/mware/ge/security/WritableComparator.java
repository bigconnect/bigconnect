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
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.DataInput;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Comparator for {@link WritableComparable}s.
 * <p/>
 * <p>This base implemenation uses the natural ordering.  To define alternate
 * orderings, override {@link #compare(WritableComparable, WritableComparable)}.
 * <p/>
 * <p>One may optimize compare-intensive operations by overriding
 * {@link #compare(byte[], int, int, byte[], int, int)}.  Static utility methods are
 * provided to assist in optimized implementations of this method.
 */
class WritableComparator implements RawComparator {

    private static final ConcurrentHashMap<Class, WritableComparator> comparators
            = new ConcurrentHashMap<Class, WritableComparator>(); // registry

    /**
     * Get a comparator for a {@link WritableComparable} implementation.
     */
    public static WritableComparator get(Class<? extends WritableComparable> c) {
        WritableComparator comparator = comparators.get(c);
        if (comparator == null) {
            // force the static initializers to run
            forceInit(c);
            // look to see if it is defined now
            comparator = comparators.get(c);
            // if not, use the generic one
            if (comparator == null) {
                comparator = new WritableComparator(c, true);
            }
        }
        return comparator;
    }

    /**
     * Force initialization of the static members.
     * As of Java 5, referencing a class doesn't force it to initialize. Since
     * this class requires that the classes be initialized to declare their
     * comparators, we force that initialization to happen.
     *
     * @param cls the class to initialize
     */
    private static void forceInit(Class<?> cls) {
        try {
            Class.forName(cls.getName(), true, cls.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Can't initialize class " + cls, e);
        }
    }

    /**
     * Register an optimized comparator for a {@link WritableComparable}
     * implementation. Comparators registered with this method must be
     * thread-safe.
     */
    public static void define(Class c, WritableComparator comparator) {
        comparators.put(c, comparator);
    }

    private final Class<? extends WritableComparable> keyClass;
    private final WritableComparable key1;
    private final WritableComparable key2;
    private final DataInputBuffer buffer;

    protected WritableComparator() {
        this(null);
    }

    /**
     * Construct for a {@link WritableComparable} implementation.
     */
    protected WritableComparator(Class<? extends WritableComparable> keyClass) {
        this(keyClass, false);
    }

    protected WritableComparator(Class<? extends WritableComparable> keyClass,
                                 boolean createInstances) {
        this.keyClass = keyClass;
        if (createInstances) {
            key1 = newKey();
            key2 = newKey();
            buffer = new DataInputBuffer();
        } else {
            key1 = key2 = null;
            buffer = null;
        }
    }

    /**
     * Returns the WritableComparable implementation class.
     */
    public Class<? extends WritableComparable> getKeyClass() {
        return keyClass;
    }

    /**
     * Construct a new {@link WritableComparable} instance.
     */
    public WritableComparable newKey() {
        return ReflectionUtils.newInstance(keyClass);
    }

    /**
     * Optimization hook.  Override this to make SequenceFile.Sorter's scream.
     * <p/>
     * <p>The default implementation reads the data into two {@link
     * WritableComparable}s (using {@link
     * Writable#readFields(DataInput)}, then calls {@link
     * #compare(WritableComparable, WritableComparable)}.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            buffer.reset(b1, s1, l1);                   // parse key1
            key1.readFields(buffer);

            buffer.reset(b2, s2, l2);                   // parse key2
            key2.readFields(buffer);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return compare(key1, key2);                   // compare them
    }

    /**
     * Compare two WritableComparables.
     * <p/>
     * <p> The default implementation uses the natural ordering, calling {@link
     * Comparable#compareTo(Object)}.
     */
    @SuppressWarnings("unchecked")
    public int compare(WritableComparable a, WritableComparable b) {
        return a.compareTo(b);
    }

    @Override
    public int compare(Object a, Object b) {
        return compare((WritableComparable) a, (WritableComparable) b);
    }

    /**
     * Lexicographic order of binary data.
     */
    public static int compareBytes(byte[] b1, int s1, int l1,
                                   byte[] b2, int s2, int l2) {
        return FastByteComparisons.compareTo(b1, s1, l1, b2, s2, l2);
    }

    /**
     * Compute hash for binary data.
     */
    public static int hashBytes(byte[] bytes, int offset, int length) {
        int hash = 1;
        for (int i = offset; i < offset + length; i++)
            hash = (31 * hash) + (int) bytes[i];
        return hash;
    }

    /**
     * Compute hash for binary data.
     */
    public static int hashBytes(byte[] bytes, int length) {
        return hashBytes(bytes, 0, length);
    }

    /**
     * Parse an unsigned short from a byte array.
     */
    public static int readUnsignedShort(byte[] bytes, int start) {
        return (((bytes[start] & 0xff) << 8) +
                ((bytes[start + 1] & 0xff)));
    }

    /**
     * Parse an integer from a byte array.
     */
    public static int readInt(byte[] bytes, int start) {
        return (((bytes[start] & 0xff) << 24) +
                ((bytes[start + 1] & 0xff) << 16) +
                ((bytes[start + 2] & 0xff) << 8) +
                ((bytes[start + 3] & 0xff)));

    }

    /**
     * Parse a float from a byte array.
     */
    public static float readFloat(byte[] bytes, int start) {
        return Float.intBitsToFloat(readInt(bytes, start));
    }

    /**
     * Parse a long from a byte array.
     */
    public static long readLong(byte[] bytes, int start) {
        return ((long) (readInt(bytes, start)) << 32) +
                (readInt(bytes, start + 4) & 0xFFFFFFFFL);
    }

    /**
     * Parse a double from a byte array.
     */
    public static double readDouble(byte[] bytes, int start) {
        return Double.longBitsToDouble(readLong(bytes, start));
    }

    /**
     * Reads a zero-compressed encoded long from a byte array and returns it.
     *
     * @param bytes byte array with decode long
     * @param start starting index
     * @return deserialized long
     * @throws java.io.IOException
     */
    public static long readVLong(byte[] bytes, int start) throws IOException {
        int len = bytes[start];
        if (len >= -112) {
            return len;
        }
        boolean isNegative = (len < -120);
        len = isNegative ? -(len + 120) : -(len + 112);
        if (start + 1 + len > bytes.length)
            throw new IOException(
                    "Not enough number of bytes for a zero-compressed integer");
        long i = 0;
        for (int idx = 0; idx < len; idx++) {
            i = i << 8;
            i = i | (bytes[start + 1 + idx] & 0xFF);
        }
        return (isNegative ? (i ^ -1L) : i);
    }

    /**
     * Reads a zero-compressed encoded integer from a byte array and returns it.
     *
     * @param bytes byte array with the encoded integer
     * @param start start index
     * @return deserialized integer
     * @throws java.io.IOException
     */
    public static int readVInt(byte[] bytes, int start) throws IOException {
        return (int) readVLong(bytes, start);
    }
}
