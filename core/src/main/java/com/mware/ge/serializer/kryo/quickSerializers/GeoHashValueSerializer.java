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
package com.mware.ge.serializer.kryo.quickSerializers;

import com.mware.ge.type.GeoHash;
import com.mware.ge.util.UTF8;
import com.mware.ge.values.storable.GeoHashValue;
import com.mware.ge.values.storable.StringValue;
import com.mware.ge.values.storable.Values;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class GeoHashValueSerializer implements QuickTypeSerializer<GeoHashValue> {
    @Override
    public byte[] objectToBytes(GeoHashValue value) {
        GeoHash geoHash = (GeoHash) value.asObjectCopy();
        byte[] valueBytes = UTF8.encode(geoHash.getHash());
        return ByteBuffer.allocate(1 + Integer.BYTES + valueBytes.length)
                .order(ByteOrder.BIG_ENDIAN)
                .put(MARKER_GEOHASHVALUE)
                .putInt(valueBytes.length)
                .put(valueBytes)
                .array();
    }

    @Override
    public GeoHashValue valueToObject(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        buffer.get(); // marker
        int count = buffer.getInt();
        int remaining = buffer.remaining();
        if (count > remaining) {
            throw new IllegalArgumentException(
                    "Bad string format; claims string is " + count + " bytes long, " +
                            "but only " + remaining + " bytes remain in buffer" );
        }
        byte[] valueBytes = new byte[count];
        buffer.get(valueBytes);
        String hash = UTF8.decode(valueBytes);
        return Values.geoHashValue(new GeoHash(hash));
    }
}
