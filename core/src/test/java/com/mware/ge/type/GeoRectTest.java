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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GeoRectTest {
    @Test
    public void testIntersects() {
        GeoRect rect1 = new GeoRect(new GeoPoint(10, 0), new GeoPoint(0, 10));
        GeoRect rect2 = new GeoRect(new GeoPoint(5, 0), new GeoPoint(0, 5));
        assertTrue(rect1.intersects(rect2));
        assertTrue(rect2.intersects(rect1));

        // rect2 is outside and above rect1
        rect2 = new GeoRect(new GeoPoint(15, 0), new GeoPoint(11, 5));
        assertFalse(rect1.intersects(rect2));
        assertFalse(rect2.intersects(rect1));

        // rect2 is outside and to the left of rect1
        rect2 = new GeoRect(new GeoPoint(2, -4), new GeoPoint(-2, -2));
        assertFalse(rect1.intersects(rect2));
        assertFalse(rect2.intersects(rect1));

        // Test intersecting with a point
        assertTrue(rect1.intersects(new GeoPoint(5, 5)));
        assertFalse(rect1.intersects(new GeoPoint(11, 11)));

        // Test intersecting with a line
        assertTrue(rect1.intersects(new GeoLine(new GeoPoint(1, 1), new GeoPoint(9, 9))));
        assertTrue(rect1.intersects(new GeoLine(new GeoPoint(-1, -1), new GeoPoint(11, 11))));
        assertFalse(rect1.intersects(new GeoLine(new GeoPoint(11, 11), new GeoPoint(12, 12))));
    }

    @Test
    public void testWithin() {
        GeoRect rect1 = new GeoRect(new GeoPoint(10, 0), new GeoPoint(0, 10));
        GeoRect rect2 = new GeoRect(new GeoPoint(5, 1), new GeoPoint(1, 5));
        assertFalse(rect1.within(rect2));
        assertTrue(rect2.within(rect1));

        // rect2 is outside and above rect1
        rect2 = new GeoRect(new GeoPoint(15, 0), new GeoPoint(11, 5));
        assertFalse(rect1.within(rect2));
        assertFalse(rect2.within(rect1));

        // rect2 is outside and to the left of rect1
        rect2 = new GeoRect(new GeoPoint(2, -4), new GeoPoint(-2, -2));
        assertFalse(rect1.within(rect2));
        assertFalse(rect2.within(rect1));

        // Test with a point
        assertFalse(rect1.within(new GeoPoint(5, 5)));
        assertFalse(rect1.within(new GeoPoint(11, 11)));

        // Test with a line
        assertFalse(rect1.within(new GeoLine(new GeoPoint(1, 1), new GeoPoint(9, 9))));
        assertFalse(rect1.within(new GeoLine(new GeoPoint(-1, -1), new GeoPoint(11, 11))));
        assertFalse(rect1.within(new GeoLine(new GeoPoint(11, 11), new GeoPoint(12, 12))));
    }
}
