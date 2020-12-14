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

import com.mware.ge.*;
import com.mware.ge.accumulo.iterator.model.GeInvalidKeyException;
import com.mware.ge.base.GraphBaseTests;
import com.mware.ge.base.GraphTestSetup;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.values.storable.TextValue;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.accumulo.iterator.model.KeyBase.VALUE_SEPARATOR;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.stringValue;
import static org.junit.Assert.*;

public class AccumuloBaseTests extends GraphBaseTests implements GraphTestSetup {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(AccumuloBaseTests.class);

    @ClassRule
    public static AccumuloResource accumuloResource = new AccumuloResource();

    @Before
    @Override
    public void before() throws Exception {
        accumuloResource.resetAutorizations();
        super.before();
    }

    @Test
    public void testDefinePropertiesMultipleGraphs() {
        Graph graph1 = graph;
        Graph graph2 = AccumuloGraph.create(new AccumuloGraphConfiguration(accumuloResource.createConfig()));
        graph1.defineProperty("p1").dataType(TextValue.class).sortable(true).textIndexHint(TextIndexHint.ALL).define();
        StopWatch timeout = new StopWatch();
        timeout.start();
        while (timeout.getTime() < 5000) {
            assertNotNull("Property definition cache shouldn't clear", graph1.getPropertyDefinition("p1"));
            PropertyDefinition def = graph2.getPropertyDefinition("p1");
            if (def != null) {
                LOGGER.debug("Propagation to graph #2 took %d ms", timeout.getTime());
                break;
            }
        }
        assertNotNull("Property definition didn't propagate to graph #2", graph2.getPropertyDefinition("p1"));
        assertTrue(graph1.getPropertyDefinition("p1").isSortable());
        assertTrue(graph2.getPropertyDefinition("p1").isSortable());
        graph2.defineProperty("p1").dataType(TextValue.class).sortable(false).textIndexHint(TextIndexHint.ALL).define();
        assertFalse(graph2.getPropertyDefinition("p1").isSortable());
        timeout.reset();
        timeout.start();
        while (timeout.getTime() < 5000) {
            PropertyDefinition def = graph1.getPropertyDefinition("p1");
            if (def != null && !def.isSortable()) {
                LOGGER.debug("Propagation to graph #1 took %d ms", timeout.getTime());
                return;
            }
        }
        throw new RuntimeException("Timeout waiting for sortable update to propagate");
    }

    @Test
    public void testPropertyWithValueSeparator() {
        try {
            graph.prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                    .addPropertyValue("prop1" + VALUE_SEPARATOR, "name1", stringValue("test"), VISIBILITY_EMPTY)
                    .save(AUTHORIZATIONS_EMPTY);
            throw new RuntimeException("Should have thrown a bad character exception");
        } catch (GeException ex) {
            // ok
        }
    }

    @Test
    public void testListSplits() throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
        SortedSet<Text> keys = new TreeSet<>();
        keys.add(new Text("j"));
        getGraph().getConnector().tableOperations().addSplits(getGraph().getVerticesTableName(), keys);

        keys = new TreeSet<>();
        keys.add(new Text("k"));
        getGraph().getConnector().tableOperations().addSplits(getGraph().getEdgesTableName(), keys);

        keys = new TreeSet<>();
        keys.add(new Text("l"));
        getGraph().getConnector().tableOperations().addSplits(getGraph().getDataTableName(), keys);

        List<IdRange> verticesTableSplits = toList(getGraph().listVerticesTableSplits());
        assertEquals(2, verticesTableSplits.size());
        assertEquals(null, verticesTableSplits.get(0).getStart());
        assertEquals("j", verticesTableSplits.get(0).getEnd());
        assertEquals("j", verticesTableSplits.get(1).getStart());
        assertEquals(null, verticesTableSplits.get(1).getEnd());

        List<IdRange> edgesTableSplits = toList(getGraph().listEdgesTableSplits());
        assertEquals(2, edgesTableSplits.size());
        assertEquals(null, edgesTableSplits.get(0).getStart());
        assertEquals("k", edgesTableSplits.get(0).getEnd());
        assertEquals("k", edgesTableSplits.get(1).getStart());
        assertEquals(null, edgesTableSplits.get(1).getEnd());

        List<IdRange> dataTableSplits = toList(getGraph().listDataTableSplits());
        assertEquals(2, dataTableSplits.size());
        assertEquals(null, dataTableSplits.get(0).getStart());
        assertEquals("l", dataTableSplits.get(0).getEnd());
        assertEquals("l", dataTableSplits.get(1).getStart());
        assertEquals(null, dataTableSplits.get(1).getEnd());
    }

    @Test
    public void testTracing() {
        getGraph().traceOn("test");
        try {
            getGraph().getVertex("v1", AUTHORIZATIONS_A);
        } finally {
            getGraph().traceOff();
        }
    }

    /**
     * Add this into the watch window to print the vertices table in time sorted order
     *
     * ((AccumuloGraphTest)this).printVerticesTable(AUTHORIZATIONS_ALL)
     */
    public void printVerticesTable(Authorizations authorizations) {
        String tableName = getGraph().getVerticesTableName();
        printTable(tableName, authorizations);
    }

    public void printTable(String tableName, Authorizations authorizations) {
        System.out.println(tableName);
        try {
            Scanner scanner = getGraph().getConnector().createScanner(
                    tableName,
                    getGraph().toAccumuloAuthorizations(authorizations)
            );
            Text currentRow = null;
            List<Map.Entry<Key, Value>> rowEntries = new ArrayList<>();
            for (Map.Entry<Key, Value> entry : scanner) {
                if (!entry.getKey().getRow().equals(currentRow)) {
                    printRowEntries(rowEntries);
                    rowEntries.clear();
                    currentRow = entry.getKey().getRow();
                }
                rowEntries.add(entry);
            }
            printRowEntries(rowEntries);
        } catch (TableNotFoundException ex) {
            throw new GeException("Could not print table", ex);
        }
    }

    private void printRowEntries(List<Map.Entry<Key, Value>> rowEntries) {
        if (rowEntries.size() == 0) {
            return;
        }
        rowEntries.sort(Comparator.comparingLong(o -> o.getKey().getTimestamp()));
        for (Map.Entry<Key, Value> rowEntry : rowEntries) {
            printRow(rowEntry);
        }
    }

    private void printRow(Map.Entry<Key, Value> rowEntry) {
        System.out.println(String.format(
                "%s:%d:%s:%s[%s] => %s",
                rowEntry.getKey().getRow(),
                rowEntry.getKey().getTimestamp(),
                rowEntry.getKey().getColumnFamily(),
                rowEntry.getKey().getColumnQualifier(),
                rowEntry.getKey().getColumnVisibility(),
                rowEntry.getValue()
        ));
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

    protected String substitutionDeflate(String str) {
        return str;
    }
}
