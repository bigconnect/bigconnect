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
package com.mware.ge.store;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

// We are doing custom serialization to make this as fast as possible since this can get called many times
public class StorableEdgeInfo {
    public static final String CHARSET_NAME = "UTF-8";
    private byte[] bytes;
    private transient long timestamp;
    private transient String label;
    private transient String vertexId;
    private transient boolean decoded;

    // here for serialization
    protected StorableEdgeInfo() {

    }

    public StorableEdgeInfo(String label, String vertexId) {
        this(label, vertexId, System.currentTimeMillis(), true);
    }

    public StorableEdgeInfo(String label, String vertexId, long timestamp, boolean includeEdgeVertexIds) {
        if (label == null) {
            throw new IllegalArgumentException("label cannot be null");
        }
        if (includeEdgeVertexIds && vertexId == null) {
            throw new IllegalArgumentException("vertexId cannot be null");
        }
        this.label = label;
        this.vertexId = vertexId;
        this.timestamp = timestamp;
        this.decoded = true;
    }

    public StorableEdgeInfo(byte[] bytes, long timestamp) {
        this.timestamp = timestamp;
        this.bytes = bytes;
    }

    public String getLabel() {
        decodeBytes();
        return label;
    }

    public String getVertexId() {
        decodeBytes();
        return vertexId;
    }

    // fast access method to avoid creating a new instance of an EdgeInfo
    public static String getVertexId(byte[] buffer) {
        int offset = 0;

        // skip label
        int strLen = readInt(buffer, offset);
        offset += 4;
        if (strLen > 0) {
            offset += strLen;
        }

        strLen = readInt(buffer, offset);
        return readString(buffer, offset, strLen);
    }

    private void decodeBytes() {
        if (!decoded) {
            int offset = 0;

            int strLen = readInt(this.bytes, offset);
            offset += 4;
            this.label = readString(this.bytes, offset, strLen);
            offset += strLen;

            strLen = readInt(this.bytes, offset);
            offset += 4;
            this.vertexId = readString(this.bytes, offset, strLen);

            this.decoded = true;
        }
    }

    public byte[] getLabelBytes() {
        // Used to use ByteBuffer here but it was to slow
        int labelBytesLength = readInt(this.bytes, 0);
        return Arrays.copyOfRange(this.bytes, 4, 4 + labelBytesLength);
    }

    public byte[] getBytes() {
        if (bytes == null) {
            try {
                byte[] labelBytes = label.getBytes(CHARSET_NAME);
                int labelBytesLength = labelBytes.length;

                byte[] vertexIdBytes = vertexId.getBytes(CHARSET_NAME);
                int vertexIdBytesLength = vertexIdBytes.length;
                int len = 4 + labelBytesLength + 4 + vertexIdBytesLength;

                byte[] buffer = new byte[len];
                int offset = 0;

                writeInt(labelBytesLength, buffer, offset);
                offset += 4;
                if (labelBytes != null) {
                    System.arraycopy(labelBytes, 0, buffer, offset, labelBytesLength);
                    offset += labelBytesLength;
                }

                writeInt(vertexIdBytesLength, buffer, offset);
                offset += 4;
                if (vertexIdBytes != null) {
                    System.arraycopy(vertexIdBytes, 0, buffer, offset, vertexIdBytesLength);
                }

                this.bytes = buffer;
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException("Could not encode edge info", ex);
            }
        }
        return bytes;
    }

    private void writeInt(int value, byte[] buffer, int offset) {
        buffer[offset++] = (byte) ((value >> 24) & 0xff);
        buffer[offset++] = (byte) ((value >> 16) & 0xff);
        buffer[offset++] = (byte) ((value >> 8) & 0xff);
        buffer[offset] = (byte) (value & 0xff);
    }

    private static int readInt(byte[] buffer, int offset) {
        return ((buffer[offset] & 0xff) << 24)
                | ((buffer[offset + 1] & 0xff) << 16)
                | ((buffer[offset + 2] & 0xff) << 8)
                | ((buffer[offset + 3] & 0xff));
    }

    private static String readString(byte[] buffer, int offset, int length) {
        byte[] d = new byte[length];
        System.arraycopy(buffer, offset, d, 0, length);
        try {
            return new String(d, CHARSET_NAME);
        } catch (IOException ex) {
            throw new RuntimeException("Could not decode edge info", ex);
        }
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "EdgeInfo{" +
                "vertexId='" + vertexId + '\'' +
                ", label='" + label + '\'' +
                '}';
    }
}
