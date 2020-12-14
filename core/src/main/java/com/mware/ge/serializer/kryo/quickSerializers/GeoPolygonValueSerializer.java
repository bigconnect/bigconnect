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

import com.mware.ge.type.GeoPoint;
import com.mware.ge.type.GeoPolygon;
import com.mware.ge.values.storable.GeoPolygonValue;
import com.mware.ge.values.storable.Values;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class GeoPolygonValueSerializer implements QuickTypeSerializer<GeoPolygonValue> {
    @Override
    public byte[] objectToBytes(GeoPolygonValue value) {
        GeoPolygon geoPolygon = (GeoPolygon) value.asObjectCopy();
        final List<GeoPoint> outerBoundary = geoPolygon.getOuterBoundary();
        final List<List<GeoPoint>> holes = geoPolygon.getHoles();

        int bufferSize = // marker + outer boundary
                1 + Integer.BYTES + 4 * Double.BYTES * outerBoundary.size() +
                // number of holes
                Integer.BYTES;

        // calculate buffer size for each hole
        for (int i=0; i<holes.size(); i++) {
            bufferSize += (Integer.BYTES + 4 * Double.BYTES) * holes.get(i).size();
        }
        ByteBuffer buf = ByteBuffer.allocate(bufferSize).order(ByteOrder.BIG_ENDIAN);

        buf.put(MARKER_GEOPOLYGONVALUE);

        // write outer boundary
        buf.putInt(outerBoundary.size());

        for (int i = 0; i < outerBoundary.size(); i++) {
            GeoPoint gp = outerBoundary.get(i);
            buf.putDouble(gp.getLatitude());
            buf.putDouble(gp.getLongitude());
            buf.putDouble(gp.getAltitude());
            buf.putDouble(gp.getAccuracy());
        }

        // write holes
        buf.putInt(holes.size());

        for (int i = 0; i < holes.size(); i++) {
            List<GeoPoint> hole = holes.get(i);
            buf.putInt(hole.size());
            for (int j = 0; j < hole.size(); j++) {
                GeoPoint gp = hole.get(j);
                buf.putDouble(gp.getLatitude());
                buf.putDouble(gp.getLongitude());
                buf.putDouble(gp.getAltitude());
                buf.putDouble(gp.getAccuracy());
            }
        }

        return buf.array();
    }

    @Override
    public GeoPolygonValue valueToObject(byte[] data) {
        final ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        buffer.get();

        // read outer boundary
        int outerBoundaryLength = buffer.getInt();
        GeoPoint[] outerBoundaryPoints = new GeoPoint[outerBoundaryLength];
        for (int i = 0; i < outerBoundaryPoints.length; i++) {
            outerBoundaryPoints[i] = new GeoPoint(buffer.getDouble(), buffer.getDouble(), buffer.getDouble(), buffer.getDouble());
        }

        // read holes
        int noHoles = buffer.getInt();
        List<List<GeoPoint>> holes = new ArrayList<>(noHoles);
        for (int i = 0; i < noHoles; i++) {
            int noPoints = buffer.getInt();
            List<GeoPoint> holePoints = new ArrayList<>(noPoints);
            for (int j = 0; j < noPoints; j++) {
                holePoints.add(j, new GeoPoint(buffer.getDouble(), buffer.getDouble(), buffer.getDouble(), buffer.getDouble()));
            }
            holes.add(holePoints);
        }

        return Values.geoPolygonValue(new GeoPolygon(Arrays.asList(outerBoundaryPoints), holes));
    }
}
