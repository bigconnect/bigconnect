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
package com.mware.core.model.schema;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.ge.type.GeoLine;
import com.mware.ge.type.GeoPoint;
import org.junit.Test;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaPropertyTest {
    @Test
    public void testConvertObject() throws ParseException {
        Date date = new Date();
        assertEquals(date, createOntologyProperty(PropertyType.DATE).convert(date));
        assertEquals(new GeoPoint(10, 20), createOntologyProperty(PropertyType.GEO_LOCATION).convert(new GeoPoint(10, 20)));
        assertEquals(new GeoLine(new GeoPoint(10, 20), new GeoPoint(30, 40)), createOntologyProperty(PropertyType.GEO_SHAPE).convert(new GeoLine(new GeoPoint(10, 20), new GeoPoint(30, 40))));
        assertEquals(new BigDecimal("1.23"), createOntologyProperty(PropertyType.CURRENCY).convert(new BigDecimal("1.23")));
        assertEquals(1.2345, createOntologyProperty(PropertyType.DOUBLE).convert(1.2345));
        assertEquals(123, createOntologyProperty(PropertyType.INTEGER).convert(123));
        assertEquals(true, createOntologyProperty(PropertyType.BOOLEAN).convert(true));
        assertEquals("test", createOntologyProperty(PropertyType.STRING).convert("test"));
    }

    @Test
    public void testConvertString() throws ParseException {
        assertEquals(
                SchemaProperty.DATE_TIME_WITH_SECONDS_FORMAT.parse("2016-03-15 04:12:13"),
                createOntologyProperty(PropertyType.DATE).convertString("2016-03-15 04:12:13")
        );
        assertEquals(new GeoPoint(10, 20), createOntologyProperty(PropertyType.GEO_LOCATION).convertString("POINT(10, 20)"));
        assertEquals(new BigDecimal("1.23"), createOntologyProperty(PropertyType.CURRENCY).convertString("1.23"));
        assertEquals(1.2345, createOntologyProperty(PropertyType.DOUBLE).convertString("1.2345"));
        assertEquals(123, createOntologyProperty(PropertyType.INTEGER).convertString("123"));
        assertEquals(true, createOntologyProperty(PropertyType.BOOLEAN).convertString("true"));
        assertEquals("test", createOntologyProperty(PropertyType.STRING).convertString("test"));
    }

    private SchemaProperty createOntologyProperty(PropertyType dataType) throws ParseException {
        SchemaProperty schemaProperty = mock(SchemaProperty.class);
        when(schemaProperty.getDataType()).thenReturn(dataType);
        when(schemaProperty.convert(any(Object.class))).thenCallRealMethod();
        when(schemaProperty.convertString(any(String.class))).thenCallRealMethod();
        return schemaProperty;
    }
}
