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

import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.graph.GraphUpdateContext;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.user.User;
import com.mware.core.GraphTestBase;
import com.mware.ge.Authorizations;
import com.mware.ge.Element;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.inmemory.InMemoryGraphFactory;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class DateBcPropertyTest extends GraphTestBase {
    private User user;
    private Authorizations authorizations;
    private PropertyMetadata metadata;
    private DateBcProperty prop;

    @Before
    public void before() throws Exception {
        super.before();
        user = getUserRepository().getSystemUser();
        authorizations = getAuthorizationRepository().getGraphAuthorizations(user);
        metadata = new PropertyMetadata(user, new VisibilityJson(""), new Visibility(""));
        prop = new DateBcProperty("name");
    }

    @Override
    protected TestGraphFactory graphFactory() {
        return new InMemoryGraphFactory();
    }

    @Test
    public void testUpdatePropertyIfValueIsNewer_newer() {
        ZonedDateTime oldValue = ZonedDateTime.of(2017, 2, 6, 9, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime newValue = ZonedDateTime.of(2017, 2, 7, 9, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime expectedValue = ZonedDateTime.of(2017, 2, 7, 9, 30, 0, 0, ZoneId.of("UTC"));
        testUpdatePropertyIfValueIsNewer(oldValue, newValue, expectedValue);
    }

    @Test
    public void testUpdatePropertyIfValueIsNewer_older() {
        ZonedDateTime oldValue = ZonedDateTime.of(2017, 2, 6, 9, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime newValue = ZonedDateTime.of(2017, 2, 5, 9, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime expectedValue = ZonedDateTime.of(2017, 2, 6, 9, 30, 0, 0, ZoneId.of("UTC"));
        testUpdatePropertyIfValueIsNewer(oldValue, newValue, expectedValue);
    }

    @Test
    public void testUpdatePropertyIfValueIsNewer_newValue() {
        ZonedDateTime oldValue = null;
        ZonedDateTime newValue = ZonedDateTime.of(2017, 2, 5, 9, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime expectedValue = ZonedDateTime.of(2017, 2, 5, 9, 30, 0, 0, ZoneId.of("UTC"));
        testUpdatePropertyIfValueIsNewer(oldValue, newValue, expectedValue);
    }

    private void testUpdatePropertyIfValueIsNewer(ZonedDateTime oldValue, ZonedDateTime newValue, ZonedDateTime expectedValue) {
        testUpdateProperty(prop, expectedValue, oldValue, elemCtx ->
                prop.updatePropertyIfValueIsNewer(elemCtx, "key", newValue, metadata));
    }

    @Test
    public void testUpdatePropertyIfValueIsOlder_newer() {
        ZonedDateTime oldValue = ZonedDateTime.of(2017, 2, 6, 9, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime newValue = ZonedDateTime.of(2017, 2, 7, 9, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime expectedValue = ZonedDateTime.of(2017, 2, 6, 9, 30, 0, 0, ZoneId.of("UTC"));
        testUpdatePropertyIfValueIsOlder(oldValue, newValue, expectedValue);
    }

    @Test
    public void testUpdatePropertyIfValueIsOlder_older() {
        ZonedDateTime oldValue = ZonedDateTime.of(2017, 2, 6, 9, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime newValue = ZonedDateTime.of(2017, 2, 5, 9, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime expectedValue = ZonedDateTime.of(2017, 2, 5, 9, 30, 0, 0, ZoneId.of("UTC"));
        testUpdatePropertyIfValueIsOlder(oldValue, newValue, expectedValue);
    }

    @Test
    public void testUpdatePropertyIfValueIsOlder_newValue() {
        ZonedDateTime oldValue = null;
        ZonedDateTime newValue = ZonedDateTime.of(2017, 2, 5, 9, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime expectedValue = ZonedDateTime.of(2017, 2, 5, 9, 30, 0, 0, ZoneId.of("UTC"));
        testUpdatePropertyIfValueIsOlder(oldValue, newValue, expectedValue);
    }

    private void testUpdatePropertyIfValueIsOlder(ZonedDateTime oldValue, ZonedDateTime newValue, ZonedDateTime expectedValue) {
        testUpdateProperty(prop, expectedValue, oldValue, elemCtx ->
                prop.updatePropertyIfValueIsOlder(elemCtx, "key", newValue, metadata));
    }

    private void testUpdateProperty(DateBcProperty prop, ZonedDateTime expectedValue, ZonedDateTime oldValue, GraphUpdateContext.Update<Element> update) {
        Vertex v = getGraph().addVertex("v1", new Visibility(""), authorizations, SchemaConstants.CONCEPT_TYPE_THING);
        if (oldValue != null) {
            prop.addPropertyValue(v, "key", oldValue, new Visibility(""), authorizations);
        }

        v = getGraph().getVertex("v1", authorizations);
        try (GraphUpdateContext ctx = getGraphRepository().beginGraphUpdate(Priority.NORMAL, user, authorizations)) {
            ctx.update(v, update);
        }

        v = getGraph().getVertex("v1", authorizations);
        ZonedDateTime value = prop.getPropertyValue(v, "key");
        assertEquals(expectedValue, value);
    }
}
