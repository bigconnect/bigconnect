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
package com.mware.ge.type;

import com.mware.ge.GeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.mware.ge.util.Preconditions.checkNotNull;

public class GeoLine extends GeoShapeBase {
    private static final long serialVersionUID = 5523982042809683074L;
    private GeoPoint geoPoints[];

    public GeoLine(List<GeoPoint> geoPoints) {
        this(geoPoints.toArray(new GeoPoint[0]));
    }

    public GeoLine(GeoPoint[] value) {
        assert value != null;
        this.geoPoints = value;
        this.validate();
    }

    public GeoLine(GeoPoint start, GeoPoint end) {
        checkNotNull(start, "start is required");
        checkNotNull(end, "end is required");
        geoPoints = new GeoPoint[2];
        geoPoints[0] = start;
        geoPoints[1] = end;
    }

    @Override
    public void validate() {
        if (geoPoints.length < 2) {
            throw new GeException("A GeoLine must have at least two points.");
        }
    }

    public GeoLine addGeoPoint(GeoPoint point) {
        geoPoints = new GeoPoint[this.geoPoints.length + 1];
        geoPoints[geoPoints.length - 1] = point;
        return this;
    }

    public GeoLine addGeoPoints(List<GeoPoint> points) {
        int position = this.geoPoints.length;
        geoPoints = new GeoPoint[position + points.size()];
        GeoPoint geoPointToAdd[] = new GeoPoint[points.size()];
        points.toArray(geoPointToAdd);
        System.arraycopy(geoPointToAdd, 0, this.geoPoints, position, geoPointToAdd.length);
        return this;
    }

    public void setGeoPoints(List<GeoPoint> geoPoints) {
        this.geoPoints = geoPoints.toArray(new GeoPoint[0]);
    }

    public List<GeoPoint> getGeoPoints() {
        return Arrays.asList(geoPoints);
    }

    public GeoPoint[] points() {
        return this.geoPoints;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(geoPoints);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return Arrays.equals(geoPoints, ((GeoLine) obj).geoPoints);
    }

    @Override
    public String toString() {
        return "GeoLine[" + Arrays.stream(geoPoints).map(Object::toString).collect(Collectors.joining(", ")) + "]";
    }
}
