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

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.*;

public class GeoCircleTest {
    @Test
    public void testBoundingBox() throws Exception {
        GeoCircle geoCircle = new GeoCircle(38.6270, -90.1994, 500);
        GeoRect boundingBox = (GeoRect) GeoUtils.getEnvelope(geoCircle);
        assertEquals(43.1236, boundingBox.getNorthWest().getLatitude(), 0.0001d);
        assertEquals(-95.9590, boundingBox.getNorthWest().getLongitude(), 0.0001d);
        assertEquals(34.1303, boundingBox.getSouthEast().getLatitude(), 0.0001d);
        assertEquals(-84.4397, boundingBox.getSouthEast().getLongitude(), 0.0001d);

        geoCircle = new GeoCircle(0, -179, 500);
        boundingBox = (GeoRect) GeoUtils.getEnvelope(geoCircle);
        assertEquals(4.4966, boundingBox.getNorthWest().getLatitude(), 0.0001d);
        assertEquals(176.5033, boundingBox.getNorthWest().getLongitude(), 0.0001d);
        assertEquals(-4.4966, boundingBox.getSouthEast().getLatitude(), 0.0001d);
        assertEquals(-174.5033, boundingBox.getSouthEast().getLongitude(), 0.0001d);
    }

    @Test
    public void testWithin() {
        GeoCircle geoCircle = new GeoCircle(5.0, 5.0, 500);

        assertTrue(new GeoRect(new GeoPoint(7, 2), new GeoPoint(2, 7)).within(geoCircle));
        assertFalse(new GeoRect(new GeoPoint(10, 0), new GeoPoint(0, 10)).within(geoCircle));

        assertTrue(new GeoCircle(5, 5, 400).within(geoCircle));
        assertFalse(new GeoCircle(5, 5, 600).within(geoCircle));
        assertTrue(new GeoCircle(3, 3, 100).within(geoCircle));
        assertFalse(new GeoCircle(3, 3, 300).within(geoCircle));

        assertTrue(new GeoLine(Arrays.asList(new GeoPoint(7, 2), new GeoPoint(2, 7))).within(geoCircle));
        assertFalse(new GeoLine(Arrays.asList(new GeoPoint(0, 2), new GeoPoint(2, 7))).within(geoCircle));
    }
}
