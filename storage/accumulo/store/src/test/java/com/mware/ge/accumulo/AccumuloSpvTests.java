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

import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.VertexBuilder;
import com.mware.ge.accumulo.util.DataInDataTableStreamingPropertyValueStorageStrategy;
import com.mware.ge.base.GraphSpvTests;
import com.mware.ge.base.GraphTestSetup;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.store.util.StorableKeyHelper;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.TextValue;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.util.IterableUtils.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class AccumuloSpvTests extends GraphSpvTests implements GraphTestSetup {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(AccumuloSpvTests.class);

    @ClassRule
    public static AccumuloResource accumuloResource = new AccumuloResource();

    @Before
    @Override
    public void before() throws Exception {
        accumuloResource.resetAutorizations();
        super.before();
    }

    @Test
    public void testDataTableStreamingPropertyValuesStoreRefInElementTable() throws Exception {
        assumeTrue(getGraph().getStreamingPropertyValueStorageStrategy() instanceof DataInDataTableStreamingPropertyValueStorageStrategy);

        VertexBuilder m = graph.prepareVertex("v1", 100L, VISIBILITY_A, CONCEPT_TYPE_THING);

        StreamingPropertyValue stringSpv = StreamingPropertyValue.create("This is a string SPV");
        m.addPropertyValue("key1", "author", stringSpv, null, 200L, VISIBILITY_A);

        StreamingPropertyValue inputStreamSpv = StreamingPropertyValue.create(new ByteArrayInputStream("This is an input stream SPV".getBytes()), TextValue.class);
        m.addPropertyValue("key2", "author", inputStreamSpv, null, 300L, VISIBILITY_A);

        m.save(AUTHORIZATIONS_A);
        getGraph().flush();

        // Check that the entries in Accumulo contain instances of StreamingPropertyValueTableDataRef
        Scanner scanner = null;
        try {
            scanner = getGraph().getConnector().createScanner(
                    getGraph().getVerticesTableName(),
                    getGraph().toAccumuloAuthorizations(AUTHORIZATIONS_ALL)
            );
            List<Map.Entry<Key, Value>> entries = toList(scanner.iterator());
            assertEquals(3, entries.size());

            Map.Entry<Key, Value> stringSpvEntry = entries.get(0);
            assertEquals("v1", stringSpvEntry.getKey().getRow().toString());
            assertEquals("PROP", stringSpvEntry.getKey().getColumnFamily().toString());
            assertEquals(
                    StorableKeyHelper.getColumnQualifierFromPropertyColumnQualifier("key1", "author", getGraph().getNameSubstitutionStrategy()),
                    stringSpvEntry.getKey().getColumnQualifier().toString());
            Object stringSpvValueEntry = getGraph().getGeSerializer().bytesToObject(stringSpvEntry.getValue().get());
            assertEquals(StreamingPropertyValueTableDataRef.class, stringSpvValueEntry.getClass());

            Map.Entry<Key, Value> inputStreamSpvEntry = entries.get(1);
            assertEquals("v1", inputStreamSpvEntry.getKey().getRow().toString());
            assertEquals("PROP", inputStreamSpvEntry.getKey().getColumnFamily().toString());
            assertEquals(
                    StorableKeyHelper.getColumnQualifierFromPropertyColumnQualifier("key2", "author", getGraph().getNameSubstitutionStrategy()),
                    inputStreamSpvEntry.getKey().getColumnQualifier().toString());
            Object inputStreamSpvValueEntry = getGraph().getGeSerializer().bytesToObject(inputStreamSpvEntry.getValue().get());
            assertEquals(StreamingPropertyValueTableDataRef.class, inputStreamSpvValueEntry.getClass());
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        Property stringProperty = v1.getProperty("key1", "author");
        assertEquals(StreamingPropertyValueTableData.class, stringProperty.getValue().getClass());
        assertEquals("This is a string SPV", ((StreamingPropertyValue) stringProperty.getValue()).readToString());

        Property inputStreamProperty = v1.getProperty("key2", "author");
        assertEquals(StreamingPropertyValueTableData.class, inputStreamProperty.getValue().getClass());
        assertEquals("This is an input stream SPV", ((StreamingPropertyValue) inputStreamProperty.getValue()).readToString());
    }

    @Override
    public AccumuloGraph getGraph() {
        return (AccumuloGraph) super.getGraph();
    }

    @Override
    public void addAuthorizations(String... authorizations) {
        accumuloResource.addAuthorizations(getGraph(), authorizations);
    }

    @Override
    public TestGraphFactory graphFactory() {
        return new AccumuloGraphFactory()
                .withAccumuloResource(accumuloResource);
    }

    @Override
    public boolean isInputStreamMarkResetSupported() {
        return false;
    }
}
