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

import com.mware.ge.values.storable.DateTimeValue;
import com.mware.ge.values.storable.TimeZones;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

class DateTimeValueSerializer implements QuickTypeSerializer<DateTimeValue> {
    protected static final int BLOCKS_DATETIME = 3;

    static final int ZONE_ID_FLAG = 0x0100_0000;
    static final int ZONE_ID_MASK = 0x00FF_FFFF;
    static final int ZONE_ID_HIGH = 0x0080_0000;
    static final int ZONE_ID_EXT = 0xFF00_0000;

    @Override
    public byte[] objectToBytes(DateTimeValue value) {
        ZonedDateTime zonedDateTime = value.asObjectCopy();
        byte[] data = encode(zonedDateTime);

        return ByteBuffer.allocate(1 + data.length)
                .order(ByteOrder.BIG_ENDIAN)
                .put(MARKER_DATETIMEVALUE)
                .put(data)
                .array();
    }

    @Override
    public DateTimeValue valueToObject(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        buffer.get(); // marker
        return DateTimeValue.datetime(decode(buffer));
    }

    public static byte[] encode(ZonedDateTime value) {
        long epochSecondUTC = value.toEpochSecond();
        int nano = value.getNano();
        ZoneId zone = value.getZone();
        long[] data = new long[BLOCKS_DATETIME];

        byte[] _arr;
        if (zone instanceof ZoneOffset) {
            int offsetSeconds = ((ZoneOffset) zone).getTotalSeconds();
            data[0] = nano;
            data[1] = epochSecondUTC;
            data[2] = offsetSeconds & ZONE_ID_MASK;
            _arr = LongArraySerializer.objectToBytesNoMarker(data);
        } else {
            String zoneId = zone.getId();
            short zoneNumber = TimeZones.map(zoneId);
            data[0] = nano;
            data[1] = epochSecondUTC;
            data[2] = zoneNumber | ZONE_ID_FLAG;
            _arr = LongArraySerializer.objectToBytesNoMarker(data);
        }
        return _arr;
    }

    public static ZonedDateTime decode(ByteBuffer buffer) {
        long nanoOfSecond = buffer.getLong();
        long epochSecondUTC = buffer.getLong();
        long encodedZone = buffer.getLong();
        if ((encodedZone & ZONE_ID_FLAG) != 0) {
            short zoneId = (short) (encodedZone & ZONE_ID_MASK);
            if (TimeZones.validZoneId(zoneId)) {
                return DateTimeValue.datetimeRaw(epochSecondUTC, nanoOfSecond, ZoneId.of(TimeZones.map(zoneId)));
            } else
                throw new IllegalStateException("Could not decode zone");

        } else {
            long zoneOffsetSeconds = (ZONE_ID_HIGH & encodedZone) == ZONE_ID_HIGH ? ZONE_ID_EXT | encodedZone : encodedZone;
            return DateTimeValue.datetimeRaw(epochSecondUTC, nanoOfSecond, ZoneOffset.ofTotalSeconds((int) zoneOffsetSeconds));
        }
    }
}
