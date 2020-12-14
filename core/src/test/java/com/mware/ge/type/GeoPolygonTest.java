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
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class GeoPolygonTest {
    private static final double[][] VALID_TRIANGLE = {{0, 0}, {2.5, 5}, {5, 0}, {0, 0}};
    private static final double[][] INVALID_UNCLOSED_SQUARE = {{0, 0}, {0, 5}, {5, 5}, {5, 0}};
    private static final double[][] INVALID_BOW_TIE_LINE_INTERSECTION = {{0, 0}, {0, 5}, {5, 0}, {5, 5}, {0, 0}};
    private static final double[][] INVALID_BOW_TIE_REUSED_POINT = {{0, 0}, {2.5, 2.5}, {0, 5}, {5, 5}, {2.5, 2.5}, {5, 0}, {0, 0}};
    private static final double[][] INVALID_SQUARE_WITH_HOLE_VIA_REUSED_POINT = {{0, 0}, {0, 5}, {5, 5}, {5, 2.5}, {4, 3}, {4, 2}, {5, 2.5}, {5, 0}, {0, 0}};
    private static final double[][] INVALID_CLOCKWISE_TRIANGLE = {{0, 0}, {5, 0}, {2.5, 5}, {0, 0}};

    @Test
    public void testValid() {
        GeoPolygon polygon = createPolygon(VALID_TRIANGLE);
        polygon.validate();
    }

    @Test
    public void testInvalidClockwiseOuterShell() {
        // Triangle that violates the right hand rule - fixable
        try {
            createPolygon(INVALID_CLOCKWISE_TRIANGLE);
            fail("Expected a validation error");
        } catch (GeException ve) {
            // expected
        }
        GeoShape lenientPolygon = createLenient(INVALID_CLOCKWISE_TRIANGLE);
        lenientPolygon.validate();
        assertEquals(GeoPolygon.class, lenientPolygon.getClass());
        double[][] counterClockwistTriangle = INVALID_CLOCKWISE_TRIANGLE.clone();
        ArrayUtils.reverse(counterClockwistTriangle);
        assertEquals(createGeoPoints(counterClockwistTriangle), ((GeoPolygon) lenientPolygon).getOuterBoundary());
    }

    @Test
    public void testInvalidReusedPointOnConcaveShape() {
        // Square with a cut out in a side via duplicate point - fixable - creates square with a hole
        try {
            createPolygon(INVALID_SQUARE_WITH_HOLE_VIA_REUSED_POINT);
            fail("Expected a validation error");
        } catch (GeException ve) {
            // expected
        }
        GeoShape lenientPolygon = createLenient(INVALID_SQUARE_WITH_HOLE_VIA_REUSED_POINT);
        lenientPolygon.validate();
        assertEquals(GeoPolygon.class, lenientPolygon.getClass());
        assertEquals(createGeoPoints(new double[][]{{0, 0}, {0, 5}, {5, 5}, {5, 2.5}, {5, 0}, {0, 0}}), ((GeoPolygon) lenientPolygon).getOuterBoundary());
        assertEquals(1, ((GeoPolygon) lenientPolygon).getHoles().size());
        assertEquals(createGeoPoints(new double[][]{{4, 2}, {5, 2.5}, {4, 3}, {4, 2}}), ((GeoPolygon) lenientPolygon).getHoles().get(0));
    }

    @Test
    public void testInvalidReusedPointOnConvexShape() {
        // Bow tie with duplicate point - fixable - creates two triangles
        try {
            createPolygon(INVALID_BOW_TIE_REUSED_POINT);
            fail("Expected a validation error");
        } catch (GeException ve) {
            // expected
        }
        GeoShape lenientPolygon = createLenient(INVALID_BOW_TIE_REUSED_POINT);
        lenientPolygon.validate();
        assertEquals(GeoCollection.class, lenientPolygon.getClass());
        List<GeoShape> geoCollection = ((GeoCollection) lenientPolygon).getGeoShapes();
        assertEquals(2, geoCollection.size());
        assertEquals(createGeoPoints(new double[][]{{0, 0}, {2.5, 2.5}, {5, 0}, {0, 0}}), ((GeoPolygon) geoCollection.get(0)).getOuterBoundary());
        assertEquals(0, ((GeoPolygon) geoCollection.get(0)).getHoles().size());
        assertEquals(createGeoPoints(new double[][]{{2.5, 2.5}, {0, 5}, {5, 5}, {2.5, 2.5}}), ((GeoPolygon) geoCollection.get(1)).getOuterBoundary());
        assertEquals(0, ((GeoPolygon) geoCollection.get(1)).getHoles().size());
    }

    @Test
    public void testInvalidLineIntersection() {
        //  Bow tie with intersecting lines - not fixable - produces a valid shape but it is not the complete shape that it was given
        try {
            createPolygon(INVALID_BOW_TIE_LINE_INTERSECTION);
            fail("Expected a validation error");
        } catch (GeException ve) {
            // expected
        }
        GeoShape lenientPolygon = createLenient(INVALID_BOW_TIE_LINE_INTERSECTION);
        lenientPolygon.validate();
        assertEquals(GeoPolygon.class, lenientPolygon.getClass());
        assertEquals(createGeoPoints(new double[][]{{5, 0}, {2.5, 2.5}, {5, 5}, {5, 0}}), ((GeoPolygon) lenientPolygon).getOuterBoundary());
        assertEquals(0, ((GeoPolygon) lenientPolygon).getHoles().size());
    }

    @Test
    public void testInvalidUnclosedOuterShell() {
        // Square that doesn't close - fixable - just go back to the start point
        try {
            createPolygon(INVALID_UNCLOSED_SQUARE);
            fail("Expected a validation error");
        } catch (GeException ve) {
            // expected
        }
        GeoShape lenientPolygon = createLenient(INVALID_UNCLOSED_SQUARE);
        lenientPolygon.validate();
        assertEquals(GeoPolygon.class, lenientPolygon.getClass());
        assertEquals(createGeoPoints(INVALID_UNCLOSED_SQUARE, new double[][]{INVALID_UNCLOSED_SQUARE[0]}), ((GeoPolygon) lenientPolygon).getOuterBoundary());
        assertEquals(0, ((GeoPolygon) lenientPolygon).getHoles().size());
    }

    private GeoPolygon createPolygon(double[][] latLons) {
        List<GeoPoint> points = createGeoPoints(latLons);
        return new GeoPolygon(points);
    }

    private GeoShape createLenient(double[][] latLons) {
        List<GeoPoint> points = createGeoPoints(latLons);
        return GeoPolygon.createLenient(points);
    }

    private List<GeoPoint> createGeoPoints(double[][] latLons, double[]... extraLatLons) {
        List<GeoPoint> points = new ArrayList<>();
        for (double[] latLon : latLons) {
            points.add(new GeoPoint(latLon[0], latLon[1]));
        }
        for (double[] extraLatLon : extraLatLons) {
            points.add(new GeoPoint(extraLatLon[0], extraLatLon[1]));
        }
        return points;
    }

    @Test
    public void testWithin() {
        GeoPolygon geoPolygonWithHole = new GeoPolygon(
                Arrays.asList(
                        new GeoPoint(0, 0),
                        new GeoPoint(0, 10),
                        new GeoPoint(5, 10),
                        new GeoPoint(10, 5),
                        new GeoPoint(5, 0),
                        new GeoPoint(0, 0)
                ),
                Arrays.asList(
                        Arrays.asList(
                                new GeoPoint(1, 3),
                                new GeoPoint(5, 3),
                                new GeoPoint(5, 7),
                                new GeoPoint(1, 7),
                                new GeoPoint(1, 3)
                        )
                ));

        // Test with a point
        assertTrue(new GeoPoint(6, 5).within(geoPolygonWithHole)); // point inside
        assertFalse(new GeoPoint(3, 5).within(geoPolygonWithHole)); // point in the hole
        assertFalse(new GeoPoint(11, 5).within(geoPolygonWithHole)); // point outside

        // Test with a line
        assertTrue(new GeoLine(new GeoPoint(6, 4), new GeoPoint(6, 6)).within(geoPolygonWithHole)); // line inside
        assertFalse(new GeoLine(new GeoPoint(3, 4), new GeoPoint(3, 6)).within(geoPolygonWithHole)); // line in the hole
        assertFalse(new GeoLine(new GeoPoint(11, 4), new GeoPoint(11, 6)).within(geoPolygonWithHole)); // line outside
        assertFalse(new GeoLine(new GeoPoint(9, 0), new GeoPoint(9, 10)).within(geoPolygonWithHole)); // line passes through
    }
}
