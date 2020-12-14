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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GeoPolygon extends GeoShapeBase {
    private List<GeoPoint> outerBoundary = new ArrayList<>();
    private List<List<GeoPoint>> holeBoundaries = new ArrayList<>();

    /**
     * @param outerBoundary A list of geopoints that make up the outer boundary of this shape. To avoid ambiguity,
     *                      the points must be specified in counter-clockwise order. In addition, the first and last
     *                      point specified must match.
     */
    public GeoPolygon(List<GeoPoint> outerBoundary) {
        this.outerBoundary.addAll(outerBoundary);
        this.validate();
    }

    /**
     * @param outerBoundary  A list of geopoints that make up the outer boundary of this shape. To avoid ambiguity,
     *                       the points must be specified in counter-clockwise order. In addition, the first and last
     *                       point specified must match.
     * @param holeBoundaries A list of geopoint lists that make up the holes in this shape. To avoid ambiguity,
     *                       the points must be specified in clockwise order. In addition, the first and last
     *                       point specified must match.
     */
    public GeoPolygon(List<GeoPoint> outerBoundary, List<List<GeoPoint>> holeBoundaries) {
        this.outerBoundary.addAll(outerBoundary);
        this.holeBoundaries.addAll(toArrayLists(holeBoundaries));
        this.validate();
    }

    @Override
    public void validate() {
        GeoUtils.toJtsPolygon(this.outerBoundary, this.holeBoundaries, false);
    }

    public static GeoShape createLenient(List<GeoPoint> outerBoundary) {
        return createLenient(outerBoundary, null);
    }

    public static GeoShape createLenient(List<GeoPoint> outerBoundary, List<List<GeoPoint>> holeBoundaries) {
        try {
            return new GeoPolygon(outerBoundary, holeBoundaries);
        } catch (GeInvalidShapeException ve) {
            return GeoUtils.toGeoShape(GeoUtils.toJtsPolygon(outerBoundary, holeBoundaries, true));
        }
    }

    public List<GeoPoint> getOuterBoundary() {
        return outerBoundary;
    }

    public GeoPolygon addHole(List<GeoPoint> geoPoints) {
        holeBoundaries.add(toArrayList(geoPoints));
        return this;
    }

    public List<List<GeoPoint>> getHoles() {
        return holeBoundaries;
    }

    @Override
    public int hashCode() {
        int hash = 19;
        hash = 61 * hash + outerBoundary.hashCode();
        hash = 61 * hash + holeBoundaries.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GeoPolygon other = (GeoPolygon) obj;
        return outerBoundary.equals(other.outerBoundary) && holeBoundaries.equals(other.holeBoundaries);
    }

    @Override
    public String toString() {
        return "GeoPolygon[" +
                "outerBoundary: [" + outerBoundary.stream().map(Object::toString).collect(Collectors.joining(", ")) + "]" +
                "holes: [" + holeBoundaries.stream().map(hole -> "[" + hole.stream().map(Object::toString).collect(Collectors.joining(", ")) + "]").collect(Collectors.joining(", ")) + "]" +
                "]";
    }
}
