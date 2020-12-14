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
package com.mware.ge.accumulo;

import com.mware.ge.FetchHints;
import com.mware.ge.Metadata;
import com.mware.ge.Vertex;
import com.mware.ge.base.GraphMetadataTests;
import com.mware.ge.base.GraphTestSetup;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.values.storable.Values.stringValue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class AccumuloMetadataTests extends GraphMetadataTests implements GraphTestSetup {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(AccumuloMetadataTests.class);

    @ClassRule
    public static AccumuloResource accumuloResource = new AccumuloResource();

    @Before
    @Override
    public void before() throws Exception {
        accumuloResource.resetAutorizations();
        super.before();
    }

    @Test
    public void testStoringEmptyMetadata() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);
        Metadata metadata = Metadata.create();
        v1.addPropertyValue("prop1", "prop1", stringValue("val1"), metadata, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);

        Vertex v2 = graph.addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);
        metadata = Metadata.create();
        metadata.add("meta1", stringValue("metavalue1"), VISIBILITY_EMPTY);
        v2.addPropertyValue("prop1", "prop1", stringValue("val1"), metadata, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);
        graph.flush();

        v1 = graph.getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_EMPTY);
        assertEquals(0, v1.getProperty("prop1", "prop1").getMetadata().entrySet().size());

        v2 = graph.getVertex("v2", FetchHints.ALL, AUTHORIZATIONS_EMPTY);
        metadata = v2.getProperty("prop1", "prop1").getMetadata();
        assertEquals(1, metadata.entrySet().size());
        assertEquals(stringValue("metavalue1"), metadata.getEntry("meta1", VISIBILITY_EMPTY).getValue());

        AccumuloGraph accumuloGraph = (AccumuloGraph) graph;
        ScannerBase vertexScanner = accumuloGraph.createVertexScanner(
                graph.getDefaultFetchHints(),
                AccumuloGraph.SINGLE_VERSION,
                null,
                null,
                new Range("V", "W"),
                AUTHORIZATIONS_EMPTY
        );
        RowIterator rows = new RowIterator(vertexScanner.iterator());
        while (rows.hasNext()) {
            Iterator<Map.Entry<Key, Value>> row = rows.next();
            while (row.hasNext()) {
                Map.Entry<Key, Value> col = row.next();
                if (col.getKey().getColumnFamily().equals(AccumuloElement.CF_PROPERTY_METADATA)) {
                    if (col.getKey().getRow().toString().equals("Vv1")) {
                        assertEquals("", col.getValue().toString());
                    } else if (col.getKey().getRow().toString().equals("Vv2")) {
                        assertNotEquals("", col.getValue().toString());
                    } else {
                        fail("invalid vertex");
                    }
                }
            }
        }
    }

    @Override
    public void addAuthorizations(String... authorizations) {
        accumuloResource.addAuthorizations((AccumuloGraph) getGraph(), authorizations);
    }

    @Override
    public TestGraphFactory graphFactory() {
        return new AccumuloGraphFactory()
                .withAccumuloResource(accumuloResource);
    }
}
