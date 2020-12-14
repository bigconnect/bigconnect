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

import com.mware.ge.type.GeoLine;
import com.mware.ge.type.GeoPoint;
import com.mware.ge.values.storable.GeoLineValue;
import com.mware.ge.values.storable.Values;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class GeoLineValueSerializer implements QuickTypeSerializer<GeoLineValue> {
    @Override
    public byte[] objectToBytes(GeoLineValue value) {
        GeoLine geoLine = (GeoLine) value.asObjectCopy();
        GeoPoint[] points = geoLine.points();

        ByteBuffer buf = ByteBuffer.allocate(1 + Integer.BYTES + 4 * Double.BYTES * points.length)
                .order(ByteOrder.BIG_ENDIAN)
                .put(MARKER_GEOLINEVALUE)
                .putInt(points.length);

        for (int i = 0; i < points.length; i++) {
            GeoPoint gp = points[i];
            buf.putDouble(gp.getLatitude());
            buf.putDouble(gp.getLongitude());
            buf.putDouble(gp.getAltitude());
            buf.putDouble(gp.getAccuracy());
        }

        return buf.array();
    }

    @Override
    public GeoLineValue valueToObject(byte[] data) {
        final ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        buffer.get();

        int arrayLength = buffer.getInt();
        GeoPoint points[] = new GeoPoint[arrayLength];
        for (int i = 0; i < points.length; i++) {
            points[i] = new GeoPoint(buffer.getDouble(), buffer.getDouble(), buffer.getDouble(), buffer.getDouble());
        }
        return Values.geoLineValue(new GeoLine(points));
    }
}
