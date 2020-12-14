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

import com.mware.ge.serializer.kryo.quickSerializers.QuickTypeSerializer;
import com.mware.ge.values.storable.LocalTimeArray;
import com.mware.ge.values.storable.LocalTimeValue;
import com.mware.ge.values.storable.TimeArray;
import com.mware.ge.values.storable.Values;
import com.mware.ge.values.utils.TemporalUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

class LocalTimeArraySerializer implements QuickTypeSerializer<LocalTimeArray> {
    @Override
    public byte[] objectToBytes(LocalTimeArray value) {
        LocalTime[] v = value.asObjectCopy();
        int arrLength = v.length;
        ByteBuffer buf = ByteBuffer.allocate(1 + Integer.BYTES + Long.BYTES * arrLength).order(ByteOrder.BIG_ENDIAN);
        buf.put(MARKER_LOCALTIMEARRAY);
        buf.putInt(arrLength);
        for (int i = 0; i < value.length(); i++) {
            buf.putLong(v[i].toNanoOfDay());
        }
        return buf.array();
    }

    @Override
    public LocalTimeArray valueToObject(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        buffer.get(); // marker
        int arrLength = buffer.getInt();
        LocalTime[] arr = new LocalTime[arrLength];
        for ( int i = 0; i < arrLength; i++ ) {
            long nanosOfDayUTC = buffer.getLong();
            arr[i] = LocalTimeValue.localTimeRaw(nanosOfDayUTC);
        }
        return Values.localTimeArray( arr );
    }
}
