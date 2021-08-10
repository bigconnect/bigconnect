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

import com.mware.ge.security.ColumnVisibility;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Mutation represents an action that manipulates a row in a table. A mutation holds a list of
 * column/value pairs that represent an atomic set of modifications to make to a row.
 *
 * <p>
 * Convenience methods which takes columns and value as CharSequence (String implements
 * CharSequence) are provided. CharSequence is converted to UTF-8 by constructing a new Text object.
 *
 * <p>
 * When always passing in the same data as a CharSequence/String, it's probably more efficient to
 * call the Text put methods. This way the data is only encoded once and only one Text object is
 * created.
 *
 * <p>
 * All of the put methods append data to the mutation; they do not overwrite anything that was
 * previously put. The mutation holds a list of all columns/values that were put into it.
 *
 * <p>
 * The putDelete() methods do not remove something that was previously added to the mutation;
 * rather, they indicate that Accumulo should insert a delete marker for that row column. A delete
 * marker effectively hides entries for that row column with a timestamp earlier than the marker's.
 * (The hidden data is eventually removed during Accumulo garbage collection.)
 */
public class StoreMutation {
    private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY =
            new ThreadLocal<CharsetEncoder>() {
                @Override
                protected CharsetEncoder initialValue() {
                    return Charset.forName("UTF-8").newEncoder().
                            onMalformedInput(CodingErrorAction.REPORT).
                            onUnmappableCharacter(CodingErrorAction.REPORT);
                }
            };
    public static final byte[] EMPTY_BYTES = new byte[0];
    /**
     * Internally, this class keeps most mutation data in a byte buffer. If a cell value put into a
     * mutation exceeds this size, then it is stored in a separate buffer, and a reference to it is
     * inserted into the main buffer.
     */
    static final int VALUE_SIZE_COPY_CUTOFF = 1 << 15;

    private byte[] row;
    private byte[] data;
    private int entries;
    private List<byte[]> values;
    private UnsynchronizedBuffer.Writer buffer;
    private List<StoreColumnUpdate> updates;

    /**
     * Creates a new mutation. A defensive copy is made.
     *
     * @param row
     *          row ID
     */
    public StoreMutation(byte[] row) {
        this(row, 0, row.length);
    }

    /**
     * Creates a new mutation. A defensive copy is made.
     *
     * @param row
     *          byte array containing row ID
     * @param start
     *          starting index of row ID in byte array
     * @param length
     *          length of row ID in byte array
     * @throws IndexOutOfBoundsException
     *           if start or length is invalid
     */
    public StoreMutation(byte[] row, int start, int length) {
        this(row, start, length, 64);
    }

    /**
     * Creates a new mutation. A defensive copy is made.
     *
     * @param row
     *          byte array containing row ID
     * @param start
     *          starting index of row ID in byte array
     * @param length
     *          length of row ID in byte array
     * @param initialBufferSize
     *          the initial size, in bytes, of the internal buffer for serializing
     * @throws IndexOutOfBoundsException
     *           if start or length is invalid
     */
    public StoreMutation(byte[] row, int start, int length, int initialBufferSize) {
        this.row = new byte[length];
        System.arraycopy(row, start, this.row, 0, length);
        buffer = new UnsynchronizedBuffer.Writer(initialBufferSize);
    }

    /**
     * Creates a new mutation.
     */
    public StoreMutation() {}

    /**
     * Creates a new mutation.
     *
     * @param rowId
     *          row ID
     */
    public StoreMutation(String rowId) {
        this.row = encode(rowId);
        buffer = new UnsynchronizedBuffer.Writer(64);
    }

    private byte[] encode(CharSequence value) {
        try {
            ByteBuffer bb = encode(value.toString(), true);
            int length = bb.limit();
            byte[] bytes = new byte[length];
            System.arraycopy(bb.array(), 0, bytes, 0, length);
            return bytes;
        } catch(CharacterCodingException e) {
            throw new RuntimeException("Should not have happened ", e);
        }
    }

    /**
     * Gets the row ID for this mutation. Not a defensive copy.
     *
     * @return row ID
     */
    public byte[] getRow() {
        return row;
    }

    private void put(byte b[]) {
        put(b, b.length);
    }

    private void put(byte b[], int length) {
        buffer.writeVLong(length);
        buffer.add(b, 0, length);
    }

    private void put(boolean b) {
        buffer.add(b);
    }

    private void put(int i) {
        buffer.writeVLong(i);
    }

    private void put(long l) {
        buffer.writeVLong(l);
    }

    /**
     * Puts a modification in this mutation. Column visibility is empty; timestamp is not set. All
     * parameters are defensively copied.
     *
     * @param columnFamily
     *          column family
     * @param columnQualifier
     *          column qualifier
     * @param value
     *          cell value
     */
    public void put(CharSequence columnFamily, CharSequence columnQualifier, byte[] value) {
        put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, false, value);
    }

    /**
     * Puts a modification in this mutation. Column visibility is empty. All appropriate parameters
     * are defensively copied.
     *
     * @param columnFamily
     *          column family
     * @param columnQualifier
     *          column qualifier
     * @param timestamp
     *          timestamp
     * @param value
     *          cell value
     */
    public void put(CharSequence columnFamily, CharSequence columnQualifier, long timestamp, byte[] value) {
        put(columnFamily, columnQualifier, EMPTY_BYTES, true, timestamp, false, value);
    }

    /**
     * Puts a modification in this mutation. Timestamp is not set. All parameters are defensively
     * copied.
     *
     * @param columnFamily
     *          column family
     * @param columnQualifier
     *          column qualifier
     * @param columnVisibility
     *          column visibility
     * @param value
     *          cell value
     */
    public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, byte[] value) {
        put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0L, false, value);
    }


    /**
     * Puts a modification in this mutation. Timestamp is not set. All parameters are defensively
     * copied.
     *
     * @param columnFamily
     *          column family
     * @param columnQualifier
     *          column qualifier
     * @param columnVisibility
     *          column visibility
     * @param timestamp
     *          timestamp
     * @param value
     *          cell value
     */
    public void put(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility, long timestamp, byte[] value) {
        put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, false, value);
    }

    /**
     * Puts a deletion in this mutation. Timestamp is not set. All parameters are defensively copied.
     *
     * @param columnFamily
     *          column family
     * @param columnQualifier
     *          column qualifier
     */
    public void putDelete(CharSequence columnFamily, CharSequence columnQualifier) {
        put(columnFamily, columnQualifier, EMPTY_BYTES, false, 0l, true, EMPTY_BYTES);
    }

    /**
     * Puts a deletion in this mutation. Timestamp is not set. All parameters are defensively copied.
     *
     * @param columnFamily
     *          column family
     * @param columnQualifier
     *          column qualifier
     * @param columnVisibility
     *          column visibility
     */
    public void putDelete(CharSequence columnFamily, CharSequence columnQualifier,
                          ColumnVisibility columnVisibility) {
        put(columnFamily, columnQualifier, columnVisibility.getExpression(), false, 0l, true,
                EMPTY_BYTES);
    }

    /**
     * Puts a deletion in this mutation. All appropriate parameters are defensively copied.
     *
     * @param columnFamily
     *          column family
     * @param columnQualifier
     *          column qualifier
     * @param columnVisibility
     *          column visibility
     * @param timestamp
     *          timestamp
     */
    public void putDelete(CharSequence columnFamily, CharSequence columnQualifier, ColumnVisibility columnVisibility,
                          long timestamp) {
        put(columnFamily, columnQualifier, columnVisibility.getExpression(), true, timestamp, true,
                EMPTY_BYTES);
    }

    /**
     * Puts a modification in this mutation. All appropriate parameters are defensively copied.
     *
     * @param columnFamily
     *          column family
     * @param columnQualifier
     *          column qualifier
     * @param columnVisibility
     *          column visibility
     * @param timestamp
     *          timestamp
     * @param value
     *          cell value
     */
    public void put(
            CharSequence columnFamily,
            CharSequence columnQualifier,
            byte[] columnVisibility,
            boolean hasts,
            long timestamp,
            boolean deleted,
            byte[] value
    ) {
        byte[] bcf = encode(columnFamily.toString());
        byte[] bcq = encode(columnQualifier.toString());

        put(bcf, bcf.length, bcq, bcq.length, columnVisibility, hasts, timestamp, deleted,
                value, value.length);
    }

    private void put(
            byte[] cf,
            int cfLength,
            byte[] cq,
            int cqLength,
            byte[] cv,
            boolean hasts,
            long ts,
            boolean deleted,
            byte[] val,
            int valLength
    ) {
        if (buffer == null) {
            throw new IllegalStateException("Can not add to mutation after serializing it");
        }
        put(cf, cfLength);
        put(cq, cqLength);
        put(cv);
        put(hasts);
        if (hasts) {
            put(ts);
        }
        put(deleted);

        if (valLength < VALUE_SIZE_COPY_CUTOFF) {
            put(val, valLength);
        } else {
            if (values == null) {
                values = new ArrayList<>();
            }
            byte copy[] = new byte[valLength];
            System.arraycopy(val, 0, copy, 0, valLength);
            values.add(copy);
            put(-1 * values.size());
        }

        entries++;
    }

    public byte[] serialize() {
        if (buffer != null) {
            data = buffer.toArray();
            buffer = null;
        }

        return data;
    }

    /**
     * Gets the modifications and deletions in this mutation. After calling this method, further
     * modifications to this mutation are ignored. Changes made to the returned updates do not affect
     * this mutation.
     *
     * @return list of modifications and deletions
     */
    public List<StoreColumnUpdate> getUpdates() {
        serialize();

        UnsynchronizedBuffer.Reader in = new UnsynchronizedBuffer.Reader(data);

        if (updates == null) {
            if (entries == 1) {
                updates = Collections.singletonList(deserializeColumnUpdate(in));
            } else {
                StoreColumnUpdate[] tmpUpdates = new StoreColumnUpdate[entries];

                for (int i = 0; i < entries; i++)
                    tmpUpdates[i] = deserializeColumnUpdate(in);

                updates = Arrays.asList(tmpUpdates);
            }
        }

        return updates;
    }

    protected StoreColumnUpdate newColumnUpdate(byte[] cf, byte[] cq, byte[] cv, boolean hasts, long ts,
                                           boolean deleted, byte[] val) {
        return new StoreColumnUpdate(cf, cq, cv, hasts, ts, deleted, val);
    }

    private StoreColumnUpdate deserializeColumnUpdate(UnsynchronizedBuffer.Reader in) {
        byte[] cf = readBytes(in);
        byte[] cq = readBytes(in);
        byte[] cv = readBytes(in);
        boolean hasts = in.readBoolean();
        long ts = 0;
        if (hasts)
            ts = in.readVLong();
        boolean deleted = in.readBoolean();

        byte[] val;
        int valLen = (int) in.readVLong();

        if (valLen < 0) {
            val = values.get((-1 * valLen) - 1);
        } else if (valLen == 0) {
            val = EMPTY_BYTES;
        } else {
            val = new byte[valLen];
            in.readBytes(val);
        }

        return newColumnUpdate(cf, cq, cv, hasts, ts, deleted, val);
    }

    private byte[] readBytes(UnsynchronizedBuffer.Reader in) {
        int len = (int) in.readVLong();
        if (len == 0)
            return EMPTY_BYTES;

        byte bytes[] = new byte[len];
        in.readBytes(bytes);
        return bytes;
    }

    /**
     * Converts the provided String to bytes using the
     * UTF-8 encoding. If <code>replace</code> is true, then
     * malformed input is replaced with the
     * substitution character, which is U+FFFD. Otherwise the
     * method throws a MalformedInputException.
     * @return ByteBuffer: bytes stores at ByteBuffer.array()
     *                     and length is ByteBuffer.limit()
     */
    public static ByteBuffer encode(String string, boolean replace)
            throws CharacterCodingException {
        CharsetEncoder encoder = ENCODER_FACTORY.get();
        if (replace) {
            encoder.onMalformedInput(CodingErrorAction.REPLACE);
            encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        ByteBuffer bytes =
                encoder.encode(CharBuffer.wrap(string.toCharArray()));
        if (replace) {
            encoder.onMalformedInput(CodingErrorAction.REPORT);
            encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return bytes;
    }

    public byte[] getData() {
        return data;
    }


}
