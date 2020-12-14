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
package com.mware.core.model.properties.types;

import com.mware.core.GraphTestBase;
import com.mware.ge.ExtendedDataRow;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.inmemory.InMemoryGraphFactory;
import com.mware.ge.values.storable.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DateBcExtendedDataTest extends GraphTestBase {
    private DateBcExtendedData property;

    @Mock
    private ExtendedDataRow row;

    @Before
    public void before() throws Exception {
        super.before();
        property = new DateBcExtendedData("table1", "prop1");
    }

    @Override
    protected TestGraphFactory graphFactory() {
        return new InMemoryGraphFactory();
    }

    @Test
    public void testGetValueDateTimeUtcNull() {
        when(row.getPropertyValue("prop1")).thenReturn(null);
        ZonedDateTime value = property.getValueDateTimeUtc(row);
        assertNull(value);
    }

    @Test
    public void testGetValueDateTimeUtc() {
        ZonedDateTime time = ZonedDateTime.of(2018, 1, 18, 21, 2, 10, 0, ZoneOffset.UTC);
        when(row.getPropertyValue("prop1")).thenReturn(Values.of(time));
        ZonedDateTime value = property.getValueDateTimeUtc(row);
        assertEquals(time, value);
    }
}
