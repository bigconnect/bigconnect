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
package com.mware.ge.inmemory;

import com.mware.core.model.schema.SchemaConstants;
import com.mware.ge.GeTypeException;
import com.mware.ge.GraphConfiguration;
import com.mware.ge.Vertex;
import com.mware.ge.base.GraphBaseTests;
import com.mware.ge.base.GraphTestSetup;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.values.storable.TextValue;
import org.junit.Test;

import java.util.Map;

import static com.mware.ge.values.storable.Values.stringValue;
import static org.junit.Assert.assertEquals;

public class InMemoryBaseTests extends GraphBaseTests implements GraphTestSetup {
    @Override
    public TestGraphFactory graphFactory() {
        return new InMemoryGraphFactory();
    }

    @Test
    public void testStrictTyping() {
        Map<String, String> config = InMemoryGraphFactory.createConfig();
        config.put(GraphConfiguration.STRICT_TYPING, "true");
        InMemoryGraph g = InMemoryGraph.create((Map) config);

        g.defineProperty("prop1").dataType(TextValue.class).define();

        Vertex v = g.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, SchemaConstants.CONCEPT_TYPE_THING);
        v.addPropertyValue("k1", "prop1", stringValue("value1"), VISIBILITY_A, AUTHORIZATIONS_A);
        try {
            v.addPropertyValue("k1", "prop2", stringValue("value1"), VISIBILITY_A, AUTHORIZATIONS_A);
            throw new RuntimeException("Expected a type exception");
        } catch (GeTypeException ex) {
            assertEquals("prop2", ex.getName());
            assertEquals("StringWrappingStringValue", ex.getValueClass().getSimpleName());
        }
    }
}
