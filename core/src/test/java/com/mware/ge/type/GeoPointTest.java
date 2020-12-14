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

import com.google.common.collect.Lists;
import com.mware.ge.GeException;
import org.junit.Test;

import static org.junit.Assert.*;

public class GeoPointTest {
    @Test
    public void testParse() throws Exception {
        assertEquals(new GeoPoint(38.9283, -77.1753), GeoPoint.parse("38.9283, -77.1753"));
        assertEquals(new GeoPoint(38.9283, -77.1753, 500.0), GeoPoint.parse("38.9283, -77.1753, 500"));
        assertEquals(new GeoPoint(38.9283, -77.1753, 500.0, 25.0), GeoPoint.parse("38.9283, -77.1753, 500, ~25"));
        assertEquals(new GeoPoint(38.9283, -77.1753, 0.0, 25.0), GeoPoint.parse("38.9283, -77.1753, ~25"));
        assertEquals(new GeoPoint(38.9283, -77.1753), GeoPoint.parse("38° 55' 41.88\", -77° 10' 31.0794\""));

        try {
            GeoPoint.parse("38.9283");
            throw new RuntimeException("Expected an exception");
        } catch (GeException ex) {
            // expected
        }

        try {
            GeoPoint.parse("38.9283, -77.1753, 500, 10");
            throw new RuntimeException("Expected an exception");
        } catch (GeException ex) {
            // expected
        }

        try {
            GeoPoint.parse("38.9283, -77.1753, ~500, ~10");
            throw new RuntimeException("Expected an exception");
        } catch (GeException ex) {
            // expected
        }

        try {
            GeoPoint.parse("38.9283, -77.1753, 500, 10, 10");
            throw new RuntimeException("Expected an exception");
        } catch (GeException ex) {
            // expected
        }
    }

    @Test
    public void testParseWithDescription() throws Exception {
        GeoPoint pt = GeoPoint.parse("38.9283, -77.1753");
        assertEquals(38.9283, pt.getLatitude(), 0.001);
        assertEquals(-77.1753, pt.getLongitude(), 0.001);
    }

    @Test
    public void testDistanceFrom() {
        GeoPoint p1 = new GeoPoint(38.6270, -90.1994);
        GeoPoint p2 = new GeoPoint(39.0438, -77.4874);
        assertEquals(1101.13d, p1.distanceFrom(p2), 0.01d);
    }

    @Test
    public void testDistanceFromOppositeSidesOfEarth() {
        GeoPoint p1 = new GeoPoint(0.0, 0.0);
        GeoPoint p2 = new GeoPoint(0.0, 180.0);
        assertEquals(GeoUtils.EARTH_CIRCUMFERENCE / 2.0, p1.distanceFrom(p2), 0.01d);
    }

    @Test
    public void testLongitudinalDistanceTo() {
        GeoPoint topLeft = new GeoPoint(10.0, 0.0);
        GeoPoint bottomRight = new GeoPoint(0.0, 10.0);
        assertEquals(-10.0, topLeft.longitudinalDistanceTo(bottomRight), 0.01);
        assertEquals(10.0, bottomRight.longitudinalDistanceTo(topLeft), 0.01);

        topLeft = new GeoPoint(10.0, -170);
        bottomRight = new GeoPoint(0.0, 170);
        assertEquals(-20.0, topLeft.longitudinalDistanceTo(bottomRight), 0.01);
        assertEquals(20.0, bottomRight.longitudinalDistanceTo(topLeft), 0.01);
    }

    @Test
    public void testCalculateCenter1Points() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0, 100.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1));
        GeoPoint expected = new GeoPoint(10.0, 20.0, 100.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenter2Points() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0, 100.0);
        GeoPoint pt2 = new GeoPoint(10.1, 20.1, 200.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(10.05, 20.05, 150.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenter2PointsSameLongitude() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0, 100.0);
        GeoPoint pt2 = new GeoPoint(10.1, 20.0, 200.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(10.05, 20.0, 150.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenter2PointsSameLatitude() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0, 100.0);
        GeoPoint pt2 = new GeoPoint(10.0, 20.1, 200.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(10.0, 20.05, 150.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenter2PointsSamePoint() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0);
        GeoPoint pt2 = new GeoPoint(10.0, 20.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(10.0, 20.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenter4Points() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0);
        GeoPoint pt2 = new GeoPoint(20.0, 30.0);
        GeoPoint pt3 = new GeoPoint(0.0, 40.0);
        GeoPoint pt4 = new GeoPoint(40.0, 10.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(15.05467090, 24.88248913);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenterPointsAroundZero() {
        GeoPoint pt1 = new GeoPoint(10.0, 10.0);
        GeoPoint pt2 = new GeoPoint(350.0, 80.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(0.0, 45.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenterPointsOppositeSides() {
        GeoPoint pt1 = new GeoPoint(90.0, 0.0);
        GeoPoint pt2 = new GeoPoint(-90.0, 0.0);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(0.0, 0.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);

        pt1 = new GeoPoint(0.0, 0.0);
        pt2 = new GeoPoint(0.0, 180.0);
        center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        expected = new GeoPoint(0.0, 90.0);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testCalculateCenterWithMissingAltitude() {
        GeoPoint pt1 = new GeoPoint(10.0, 20.0, 10.0);
        GeoPoint pt2 = new GeoPoint(10.1, 20.1);
        GeoPoint center = GeoPoint.calculateCenter(Lists.newArrayList(pt1, pt2));
        GeoPoint expected = new GeoPoint(10.05, 20.05);
        assertEquals("distance " + expected.distanceFrom(center) + " > " + GeoPoint.EQUALS_TOLERANCE_KM, expected, center);
    }

    @Test
    public void testIsSouthEastOf() {
        GeoPoint topLeft = new GeoPoint(10.0, 0.0);
        GeoPoint bottomRight = new GeoPoint(0.0, 10.0);
        assertFalse(topLeft.isSouthEastOf(bottomRight));
        assertTrue(bottomRight.isSouthEastOf(topLeft));

        topLeft = new GeoPoint(10.0, -170);
        bottomRight = new GeoPoint(0.0, 170);
        assertFalse(topLeft.isSouthEastOf(bottomRight));
        assertTrue(bottomRight.isSouthEastOf(topLeft));
    }

    @Test
    public void testIsNorthWestOf() {
        GeoPoint topLeft = new GeoPoint(10.0, 0.0);
        GeoPoint bottomRight = new GeoPoint(0.0, 10.0);
        assertTrue(topLeft.isNorthWestOf(bottomRight));
        assertFalse(bottomRight.isNorthWestOf(topLeft));

        topLeft = new GeoPoint(10.0, -170);
        bottomRight = new GeoPoint(0.0, 170);
        assertTrue(topLeft.isNorthWestOf(bottomRight));
        assertFalse(bottomRight.isNorthWestOf(topLeft));
    }

    @Test
    public void testWithin() {
        GeoRect rect1 = new GeoRect(new GeoPoint(10, 0), new GeoPoint(0, 10));

        assertTrue(new GeoPoint(5, 5).within(rect1));
        assertFalse(new GeoPoint(11, 11).within(rect1));
    }
}
