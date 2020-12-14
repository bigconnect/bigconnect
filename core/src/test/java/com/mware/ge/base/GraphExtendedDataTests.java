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
package com.mware.ge.base;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.mware.ge.*;
import com.mware.ge.event.DeleteExtendedDataEvent;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.event.GraphEventListener;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.query.*;
import com.mware.ge.query.aggregations.TermsAggregation;
import com.mware.ge.query.aggregations.TermsResult;
import com.mware.ge.search.SearchIndex;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.values.storable.DateTimeValue;
import com.mware.ge.values.storable.TextValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.GeAssert.assertRowIdsAnyOrder;
import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.longValue;
import static com.mware.ge.values.storable.Values.stringValue;
import static org.junit.Assume.assumeTrue;

@RunWith(JUnit4.class)
public abstract class GraphExtendedDataTests implements GraphTestSetup {
    protected Graph graph;

    @Before
    public void before() throws Exception {
        graph = graphFactory().createGraph();
        clearGraphEvents();
        getGraph().addGraphEventListener(new GraphEventListener() {
            @Override
            public void onGraphEvent(GraphEvent graphEvent) {
                addGraphEvent(graphEvent);
            }
        });
    }

    @After
    public void after() throws Exception {
        if (getGraph() != null) {
            getGraph().drop();
            getGraph().shutdown();
            graph = null;
        }
    }

    @Override
    public Graph getGraph() {
        return graph;
    }

    @Test
    public void testAddExtendedDataRows() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value1"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", stringValue("value2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        if (getGraph() instanceof GraphWithSearchIndex) {
            SearchIndex searchIndex = ((GraphWithSearchIndex) getGraph()).getSearchIndex();
            searchIndex.truncate(getGraph());
            searchIndex.flush(getGraph());

            ElementMutation<? extends Element> mutation = getGraph().getVertex("v1", AUTHORIZATIONS_A).prepareMutation();
            Iterable<ExtendedDataRow> extendedData = getGraph().getExtendedData(ElementType.VERTEX, "v1", "table1", AUTHORIZATIONS_A);
            searchIndex.addExtendedData(getGraph(), mutation, extendedData, AUTHORIZATIONS_A);
            getGraph().flush();
        }

        QueryResultsIterable<ExtendedDataRow> rows = getGraph().query(AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(2, 2, rows);

        rows = getGraph().query(AUTHORIZATIONS_A)
                .has("name", stringValue("value1"))
                .extendedDataRows();
        assertResultsCount(1, 1, rows);

        ExtendedDataRow row = IterableUtils.single(rows);
        assertEquals("v1", row.getId().getElementId());
        assertEquals("table1", row.getId().getTableName());
        assertEquals("row1", row.getId().getRowId());
    }

    @Test
    public void testExtendedData() {
        DateTimeValue date1 = DateTimeValue.ofEpochMillis(longValue(1487083490000L));
        DateTimeValue date2 = DateTimeValue.ofEpochMillis(longValue(1487083480000L));
        DateTimeValue date3 = DateTimeValue.ofEpochMillis(longValue(1487083470000L));
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "date", date1, VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", stringValue("value1"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "date", date2, VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", stringValue("value2"), VISIBILITY_A)
                .addExtendedData("table1", "row3", "date", date3, VISIBILITY_A)
                .addExtendedData("table1", "row3", "name", stringValue("value3"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        AtomicInteger rowCount = new AtomicInteger();
        AtomicInteger rowPropertyCount = new AtomicInteger();
        getGraph().visitElements(new DefaultGraphVisitor() {
            @Override
            public void visitExtendedDataRow(Element element, String tableName, ExtendedDataRow row) {
                rowCount.incrementAndGet();
            }

            @Override
            public void visitProperty(Element element, String tableName, ExtendedDataRow row, Property property) {
                rowPropertyCount.incrementAndGet();
            }
        }, AUTHORIZATIONS_A);
        assertEquals(3, rowCount.get());
        assertEquals(6, rowPropertyCount.get());

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(ImmutableSet.of("table1"), v1.getExtendedDataTableNames());
        Iterator<ExtendedDataRow> rows = v1.getExtendedData("table1").iterator();

        ExtendedDataRow row = rows.next();
        assertEquals(date1, row.getPropertyValue("date"));
        assertEquals(stringValue("value1"), row.getPropertyValue("name"));

        row = rows.next();
        assertEquals(date2, row.getPropertyValue("date"));
        assertEquals(stringValue("value2"), row.getPropertyValue("name"));

        row = rows.next();
        assertEquals(date3, row.getPropertyValue("date"));
        assertEquals(stringValue("value3"), row.getPropertyValue("name"));

        assertFalse(rows.hasNext());

        row = getGraph().getExtendedData(new ExtendedDataRowId(ElementType.VERTEX, "v1", "table1", "row1"), AUTHORIZATIONS_A);
        assertEquals("row1", row.getId().getRowId());
        assertEquals(date1, row.getPropertyValue("date"));
        assertEquals(stringValue("value1"), row.getPropertyValue("name"));

        rows = getGraph().getExtendedData(
                Lists.newArrayList(
                        new ExtendedDataRowId(ElementType.VERTEX, "v1", "table1", "row1"),
                        new ExtendedDataRowId(ElementType.VERTEX, "v1", "table1", "row2")
                ),
                AUTHORIZATIONS_A
        ).iterator();

        row = rows.next();
        assertEquals(date1, row.getPropertyValue("date"));
        assertEquals(stringValue("value1"), row.getPropertyValue("name"));

        row = rows.next();
        assertEquals(date2, row.getPropertyValue("date"));
        assertEquals(stringValue("value2"), row.getPropertyValue("name"));

        assertFalse(rows.hasNext());

        rows = getGraph().getExtendedData(ElementType.VERTEX, "v1", "table1", AUTHORIZATIONS_A).iterator();

        row = rows.next();
        assertEquals(date1, row.getPropertyValue("date"));
        assertEquals(stringValue("value1"), row.getPropertyValue("name"));

        row = rows.next();
        assertEquals(date2, row.getPropertyValue("date"));
        assertEquals(stringValue("value2"), row.getPropertyValue("name"));

        row = rows.next();
        assertEquals(date3, row.getPropertyValue("date"));
        assertEquals(stringValue("value3"), row.getPropertyValue("name"));

        assertFalse(rows.hasNext());

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .addExtendedData("table1", "row4", "name", stringValue("value4"), VISIBILITY_A)
                .addExtendedData("table2", "row1", "name", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertTrue("table1 should exist", v1.getExtendedDataTableNames().contains("table1"));
        assertTrue("table2 should exist", v1.getExtendedDataTableNames().contains("table2"));

        List<ExtendedDataRow> rowsList = toList(v1.getExtendedData("table1"));
        assertEquals(4, rowsList.size());
        rowsList = toList(v1.getExtendedData("table2"));
        assertEquals(1, rowsList.size());

        assertEquals(5, count(getGraph().getExtendedData(ElementType.VERTEX, "v1", null, AUTHORIZATIONS_A)));
        assertEquals(5, count(getGraph().getExtendedData(ElementType.VERTEX, null, null, AUTHORIZATIONS_A)));
        assertEquals(5, count(getGraph().getExtendedData((ElementType) null, null, null, AUTHORIZATIONS_A)));
        try {
            count(getGraph().getExtendedData(null, null, "table1", AUTHORIZATIONS_A));
            fail("nulls to the left of a value is not allowed");
        } catch (Exception ex) {
            // expected
        }
        try {
            count(getGraph().getExtendedData((ElementType) null, "v1", null, AUTHORIZATIONS_A));
            fail("nulls to the left of a value is not allowed");
        } catch (Exception ex) {
            // expected
        }
    }

    @Test
    public void testExtendedDataQuery() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value1"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", stringValue("value1"), VISIBILITY_A)
                .addExtendedData("table2", "row3", "name", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row4", "name", stringValue("value1"), VISIBILITY_A)
                .addExtendedData("table1", "row5", "name", stringValue("value1"), VISIBILITY_A)
                .addExtendedData("table2", "row6", "name", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e1", "v1", "v2", "label1", VISIBILITY_A)
                .addExtendedData("table1", "row7", "name", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        QueryResultsIterable<ExtendedDataRow> rows = getGraph().query(AUTHORIZATIONS_A)
                .extendedDataRows();
        assertRowIdsAnyOrder(rows, "row1", "row2", "row3", "row4", "row5", "row6", "row7");

        rows = getGraph().query(AUTHORIZATIONS_A)
                .has(ExtendedDataRow.ROW_ID, stringValue("row1"))
                .extendedDataRows();
        assertRowIdsAnyOrder(rows, "row1");

        rows = getGraph().query(AUTHORIZATIONS_A)
                .has(ExtendedDataRow.ELEMENT_TYPE, stringValue(ElementType.VERTEX.name()))
                .extendedDataRows();
        assertRowIdsAnyOrder(rows, "row1", "row2", "row3", "row4", "row5", "row6");

        rows = getGraph().query(AUTHORIZATIONS_A)
                .has(ExtendedDataRow.ELEMENT_TYPE, stringValue("VERTEX"))
                .extendedDataRows();
        assertRowIdsAnyOrder(rows, "row1", "row2", "row3", "row4", "row5", "row6");

        rows = getGraph().query(AUTHORIZATIONS_A)
                .has(ExtendedDataRow.ELEMENT_ID, stringValue("v1"))
                .extendedDataRows();
        assertRowIdsAnyOrder(rows, "row1", "row2", "row3");

        rows = getGraph().query(AUTHORIZATIONS_A)
                .has(ExtendedDataRow.TABLE_NAME, stringValue("table1"))
                .extendedDataRows();
        assertRowIdsAnyOrder(rows, "row1", "row2", "row4", "row5", "row7");
    }

    @Test
    public void testExtendedDataInRange() {
        getGraph().prepareVertex("a", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("aa", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("az", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value3"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("b", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value4"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("aa", "a", "aa", "edge1", VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", stringValue("value5"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        List<ExtendedDataRow> rows = toList(getGraph().getExtendedDataInRange(ElementType.VERTEX, new IdRange(null, "a"), AUTHORIZATIONS_A));
        assertEquals(0, rows.size());

        rows = toList(getGraph().getExtendedDataInRange(ElementType.VERTEX, new IdRange(null, "b"), AUTHORIZATIONS_A));
        assertEquals(3, rows.size());
        List<String> rowValues = rows.stream().map(row -> ((TextValue) row.getPropertyValue("name")).stringValue()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value1", "value2", "value3");

        rows = toList(getGraph().getExtendedDataInRange(ElementType.VERTEX, new IdRange(null, "bb"), AUTHORIZATIONS_A));
        assertEquals(4, rows.size());
        rowValues = rows.stream().map(row -> ((TextValue) row.getPropertyValue("name")).stringValue()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value1", "value2", "value3", "value4");

        rows = toList(getGraph().getExtendedDataInRange(ElementType.VERTEX, new IdRange("aa", "b"), AUTHORIZATIONS_A));
        assertEquals(2, rows.size());
        rowValues = rows.stream().map(row -> ((TextValue) row.getPropertyValue("name")).stringValue()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value2", "value3");

        rows = toList(getGraph().getExtendedDataInRange(ElementType.VERTEX, new IdRange(null, null), AUTHORIZATIONS_A));
        assertEquals(4, rows.size());
        rowValues = rows.stream().map(row -> ((TextValue) row.getPropertyValue("name")).stringValue()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value1", "value2", "value3", "value4");

        rows = toList(getGraph().getExtendedDataInRange(ElementType.EDGE, new IdRange(null, "a"), AUTHORIZATIONS_A));
        assertEquals(0, rows.size());

        rows = toList(getGraph().getExtendedDataInRange(ElementType.EDGE, new IdRange(null, "b"), AUTHORIZATIONS_A));
        assertEquals(1, rows.size());
        rowValues = rows.stream().map(row -> ((TextValue) row.getPropertyValue("name")).stringValue()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value5");

        rows = toList(getGraph().getExtendedDataInRange(ElementType.EDGE, new IdRange("aa", "b"), AUTHORIZATIONS_A));
        assertEquals(1, rows.size());
        rowValues = rows.stream().map(row -> ((TextValue) row.getPropertyValue("name")).stringValue()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value5");

        rows = toList(getGraph().getExtendedDataInRange(ElementType.EDGE, new IdRange(null, null), AUTHORIZATIONS_A));
        assertEquals(1, rows.size());
        rowValues = rows.stream().map(row -> ((TextValue) row.getPropertyValue("name")).stringValue()).collect(Collectors.toList());
        assertIdsAnyOrder(rowValues, "value5");
    }

    @Test
    public void testExtendedDataDifferentValue() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value1"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        ArrayList<ExtendedDataRow> rows = Lists.newArrayList(v1.getExtendedData("table1"));
        assertEquals(stringValue("value1"), rows.get(0).getPropertyValue("name"));

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        rows = Lists.newArrayList(v1.getExtendedData("table1"));
        assertEquals(stringValue("value2"), rows.get(0).getPropertyValue("name"));
    }

    @Test
    public void testExtendedDataDeleteColumn() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        // delete with wrong visibility
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .deleteExtendedData("table1", "row1", "name", VISIBILITY_B)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        List<ExtendedDataRow> rows = Lists.newArrayList(getGraph().getVertex("v1", AUTHORIZATIONS_A).getExtendedData("table1"));
        assertEquals(1, rows.size());
        QueryResultsIterable<? extends GeObject> searchResults = getGraph().query("value", AUTHORIZATIONS_A)
                .search();
        assertEquals(1, searchResults.getTotalHits());

        // delete with correct visibility
        clearGraphEvents();
        getGraph().getVertex("v1", AUTHORIZATIONS_A).prepareMutation()
                .deleteExtendedData("table1", "row1", "name", VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        assertEvents(
                new DeleteExtendedDataEvent(getGraph(), v1, "table1", "row1", "name", null)
        );

        if (v1.getExtendedDataTableNames().size() == 0) {
            assertEquals("table names", 0, v1.getExtendedDataTableNames().size());
        } else {
            assertEquals("extended data rows", 0, Lists.newArrayList(v1.getExtendedData("table1")).size());
        }
        searchResults = getGraph().query("value", AUTHORIZATIONS_A)
                .search();
        List<GeObject> searchResultsList = toList(searchResults);
        assertEquals("search result items", 0, searchResultsList.size());
        assertEquals("total hits", 0, searchResults.getTotalHits());
    }

    @Test
    public void testExtendedDataDelete() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        QueryResultsIterable<? extends GeObject> searchResults = getGraph().query("value", AUTHORIZATIONS_A)
                .search();
        assertResultsCount(1, 1, searchResults);

        List<ExtendedDataRow> rows = toList(getGraph().getExtendedData(ElementType.VERTEX, "v1", "table1", AUTHORIZATIONS_A));
        assertEquals(1, rows.size());

        getGraph().deleteVertex("v1", AUTHORIZATIONS_A);
        getGraph().flush();

        searchResults = getGraph().query("value", AUTHORIZATIONS_A)
                .search();
        assertResultsCount(0, 0, searchResults);

        rows = toList(getGraph().getExtendedData(ElementType.VERTEX, "v1", "table1", AUTHORIZATIONS_A));
        assertEquals(0, rows.size());
    }

    @Test
    public void testExtendedDataQueryVerticesAfterVisibilityChange() {
        String nameColumnName = "name.column";
        String tableName = "table.one";
        String rowOneName = "row.one";
        String rowTwoName = "row.two";
        getGraph().defineProperty(nameColumnName).sortable(true).textIndexHint(TextIndexHint.values()).dataType(TextValue.class).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData(tableName, rowOneName, nameColumnName, stringValue("value 1"), VISIBILITY_A)
                .addExtendedData(tableName, rowTwoName, nameColumnName, stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        QueryResultsIterable<? extends GeObject> searchResults = getGraph().query("value", AUTHORIZATIONS_A)
                .search();
        assertResultsCount(2, 2, searchResults);
        assertRowIdsAnyOrder(Lists.newArrayList(rowOneName, rowTwoName), searchResults);

        getGraph().createAuthorizations(AUTHORIZATIONS_A_AND_B);
        getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A)
                .prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        searchResults = getGraph().query("value", AUTHORIZATIONS_A)
                .search();
        assertResultsCount(0, 0, searchResults);
    }

    @Test
    public void testExtendedDataQueryVertices() {
        DateTimeValue date1 = DateTimeValue.ofEpochMillis(longValue(1487083490000L));
        DateTimeValue date2 = DateTimeValue.ofEpochMillis(longValue(1487083480000L));
        String tableName = "table.one";
        String rowOneName = "row.one";
        String rowTwoName = "row.two";
        String dateColumnName = "date.column";
        String nameColumnName = "name.column";

        getGraph().defineProperty(dateColumnName).sortable(true).dataType(DateTimeValue.class).define();
        getGraph().defineProperty(nameColumnName).sortable(true).textIndexHint(TextIndexHint.values()).dataType(TextValue.class).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData(tableName, rowOneName, dateColumnName, date1, VISIBILITY_A)
                .addExtendedData(tableName, rowOneName, nameColumnName, stringValue("value 1"), VISIBILITY_A)
                .addExtendedData(tableName, rowTwoName, dateColumnName, date2, VISIBILITY_A)
                .addExtendedData(tableName, rowTwoName, nameColumnName, stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        // Should not come back when finding vertices
        QueryResultsIterable<Vertex> queryResults = getGraph().query(AUTHORIZATIONS_A)
                .has(dateColumnName, date1)
                .sort(dateColumnName, SortDirection.ASCENDING)
                .vertices();
        assertEquals(0, queryResults.getTotalHits());

        QueryResultsIterable<? extends GeObject> searchResults = getGraph().query(AUTHORIZATIONS_A)
                .has(dateColumnName, date1)
                .sort(dateColumnName, SortDirection.ASCENDING)
                .search();
        assertEquals(1, searchResults.getTotalHits());
        List<? extends GeObject> searchResultsList = toList(searchResults);
        assertEquals(1, searchResultsList.size());
        ExtendedDataRow searchResult = (ExtendedDataRow) searchResultsList.get(0);
        assertEquals("v1", searchResult.getId().getElementId());
        assertEquals(rowOneName, searchResult.getId().getRowId());

        searchResults = getGraph().query("value", AUTHORIZATIONS_A)
                .search();
        assertEquals(2, searchResults.getTotalHits());
        searchResultsList = toList(searchResults);
        assertEquals(2, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList(rowOneName, rowTwoName), searchResultsList);

        searchResults = getGraph().query("value", AUTHORIZATIONS_A)
                .hasExtendedData(ElementType.VERTEX, "v1", tableName)
                .search();
        assertEquals(2, searchResults.getTotalHits());
        searchResultsList = toList(searchResults);
        assertEquals(2, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList(rowOneName, rowTwoName), searchResultsList);

        searchResults = getGraph().query("value", AUTHORIZATIONS_A)
                .hasExtendedData(tableName)
                .search();
        assertEquals(2, searchResults.getTotalHits());
        searchResultsList = toList(searchResults);
        assertEquals(2, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList(rowOneName, rowTwoName), searchResultsList);
    }

    @Test
    public void testExtendedDataVertexQuery() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table2", "row3", "name", stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table2", "row4", "name", stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .addExtendedData("table1", "row5", "name", stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table1", "row6", "name", stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        List<ExtendedDataRow> searchResultsList = toList(
                v1.query(AUTHORIZATIONS_A)
                        .extendedDataRows()
        );
        assertRowIdsAnyOrder(Lists.newArrayList("row3", "row4", "row5", "row6"), searchResultsList);

        QueryResultsIterable<ExtendedDataRow> rows = getGraph().query(AUTHORIZATIONS_A).hasId("v1").extendedDataRows();
        assertResultsCount(2, 2, rows);

        rows = getGraph().query(AUTHORIZATIONS_A).hasId("v1", "v2").extendedDataRows();
        assertResultsCount(4, 4, rows);

        searchResultsList = toList(
                v1.query(AUTHORIZATIONS_A)
                        .sort(ExtendedDataRow.TABLE_NAME, SortDirection.ASCENDING)
                        .sort(ExtendedDataRow.ROW_ID, SortDirection.ASCENDING)
                        .extendedDataRows()
        );
        assertRowIds(Lists.newArrayList("row5", "row6", "row3", "row4"), searchResultsList);

        searchResultsList = toList(
                getGraph().query(AUTHORIZATIONS_A)
                        .sort(ExtendedDataRow.ELEMENT_ID, SortDirection.ASCENDING)
                        .sort(ExtendedDataRow.ROW_ID, SortDirection.ASCENDING)
                        .extendedDataRows()
        );
        assertRowIds(Lists.newArrayList("row5", "row6", "row1", "row2", "row3", "row4"), searchResultsList);
        searchResultsList = toList(
                getGraph().query(AUTHORIZATIONS_A)
                        .sort(ExtendedDataRow.ELEMENT_TYPE, SortDirection.ASCENDING)
                        .sort(ExtendedDataRow.ROW_ID, SortDirection.ASCENDING)
                        .extendedDataRows()
        );
        assertRowIds(Lists.newArrayList("row5", "row6", "row1", "row2", "row3", "row4"), searchResultsList);
    }

    @Test
    public void testExtendedDataVertexQueryAggregations() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table2", "row3", "name", stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table2", "row4", "name", stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .addExtendedData("table1", "row5", "name", stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table1", "row6", "name", stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        Query q = getGraph().query(AUTHORIZATIONS_A)
                .limit(0L);
        TermsAggregation agg = new TermsAggregation("agg", ExtendedDataRow.TABLE_NAME);
        assumeTrue("terms aggregation not supported", q.isAggregationSupported(agg));
        q.addAggregation(agg);
        QueryResultsIterable<ExtendedDataRow> rows = q.extendedDataRows();
        Map<Object, Long> aggResult = termsBucketToMap(rows.getAggregationResult("agg", TermsResult.class).getBuckets());
        assertEquals(2, aggResult.size());
        assertEquals(4L, (long) aggResult.get("table1"));
        assertEquals(2L, (long) aggResult.get("table2"));
        q = getGraph().query(AUTHORIZATIONS_A)
                .addAggregation(new TermsAggregation("agg", ExtendedDataRow.ELEMENT_ID))
                .limit(0L);
        rows = q.extendedDataRows();
        aggResult = termsBucketToMap(rows.getAggregationResult("agg", TermsResult.class).getBuckets());
        assertEquals(3, aggResult.size());
        assertEquals(2L, (long) aggResult.get("v1"));
        assertEquals(2L, (long) aggResult.get("v2"));
        assertEquals(2L, (long) aggResult.get("e1"));
        q = getGraph().query(AUTHORIZATIONS_A)
                .addAggregation(new TermsAggregation("agg", ExtendedDataRow.ROW_ID))
                .limit(0L);
        rows = q.extendedDataRows();
        aggResult = termsBucketToMap(rows.getAggregationResult("agg", TermsResult.class).getBuckets());
        assertEquals(6, aggResult.size());
        assertEquals(1L, (long) aggResult.get("row1"));
        assertEquals(1L, (long) aggResult.get("row2"));
        assertEquals(1L, (long) aggResult.get("row3"));
        assertEquals(1L, (long) aggResult.get("row4"));
        assertEquals(1L, (long) aggResult.get("row5"));
        assertEquals(1L, (long) aggResult.get("row6"));
        q = getGraph().query(AUTHORIZATIONS_A)
                .addAggregation(new TermsAggregation("agg", ExtendedDataRow.ELEMENT_TYPE))
                .limit(0L);
        rows = q.extendedDataRows();
        aggResult = termsBucketToMap(rows.getAggregationResult("agg", TermsResult.class).getBuckets());
        assertEquals(2, aggResult.size());
        assertEquals(4L, (long) aggResult.get(ElementType.VERTEX.name()));
        assertEquals(2L, (long) aggResult.get(ElementType.EDGE.name()));
    }

    @Test
    public void testExtendedDataEdgeQuery() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row3", "name", stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table1", "row4", "name", stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .addExtendedData("table1", "row5", "name", stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table1", "row6", "name", stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Edge e1 = getGraph().getEdge("e1", AUTHORIZATIONS_A);
        List<ExtendedDataRow> searchResultsList = toList(
                getGraph().query("*", AUTHORIZATIONS_A)
                        .hasExtendedData(ElementType.EDGE, e1.getId(), "table1")
                        .extendedDataRows()
        );
        assertRowIdsAnyOrder(Lists.newArrayList("row5", "row6"), searchResultsList);

        QueryResultsIterable<ExtendedDataRow> rows = getGraph().query(AUTHORIZATIONS_A).hasId("e1").extendedDataRows();
        assertResultsCount(2, 2, rows);

        rows = getGraph().query(AUTHORIZATIONS_A).hasId("v1", "e1").extendedDataRows();
        assertResultsCount(4, 4, rows);
    }

    @Test
    public void testExtendedDataQueryAfterDeleteForVertex() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "name", stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        List<ExtendedDataRow> searchResultsList = toList(getGraph().query(AUTHORIZATIONS_A).extendedDataRows());
        assertRowIdsAnyOrder(Lists.newArrayList("row1", "row2"), searchResultsList);

        getGraph().deleteVertex("v1", AUTHORIZATIONS_A);
        getGraph().flush();

        searchResultsList = toList(getGraph().query(AUTHORIZATIONS_A).extendedDataRows());
        assertRowIdsAnyOrder(Lists.newArrayList(), searchResultsList);
    }

    @Test
    public void testAddExtendedData() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "color", stringValue("red"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "color", stringValue("green"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        getGraph().dumpGraph();

        QueryResultsIterable<ExtendedDataRow> results = getGraph().query(AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(2, 2, results);

        results = getGraph().query("red", AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(1, 1, results);

        results = getGraph().query("green", AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(1, 1, results);

        results = getGraph().query("blue", AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(0, 0, results);

        getGraph().getVertex("v1", AUTHORIZATIONS_A).prepareMutation()
                .addExtendedData("table1", "row1", "othercolor", stringValue("blue"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "othercolor", stringValue("purple"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();
        getGraph().dumpGraph();

        results = getGraph().query(AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(2, 2, results);

        results = getGraph().query("red", AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(1, 1, results);

        results = getGraph().query("green", AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(1, 1, results);

        results = getGraph().query("blue", AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(1, 1, results);

        results = getGraph().query("purple", AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(1, 1, results);
    }

    @Test
    public void testExtendedDataQueryAuthorizations() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "color", "junit", stringValue("red"), VISIBILITY_B)
                .addExtendedData("table1", "row2", "color", "junit", stringValue("green"), VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().createAuthorizations(AUTHORIZATIONS_A_AND_B_AND_C);
        getGraph().flush();

        QueryResultsIterable<ExtendedDataRow> results = getGraph().query(AUTHORIZATIONS_A)
                .hasExtendedData(ElementType.VERTEX, "v1")
                .extendedDataRows();
        assertResultsCount(0, 0, results);

        results = getGraph().query("red", AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(0, 0, results);

        QueryResultsIterable<? extends GeObject> searchResults = getGraph().query("red", AUTHORIZATIONS_A).search();
        assertResultsCount(0, 0, searchResults);

        results = getGraph().query("red", AUTHORIZATIONS_B).extendedDataRows();
        assertResultsCount(0, 0, results);

        searchResults = getGraph().query("red", AUTHORIZATIONS_B).search();
        assertResultsCount(0, 0, searchResults);

        results = getGraph().query("red", AUTHORIZATIONS_A_AND_B).extendedDataRows();
        assertResultsCount(1, 1, results);

        searchResults = getGraph().query("red", AUTHORIZATIONS_A_AND_B).search();
        assertResultsCount(1, 1, searchResults);

        searchResults = getGraph().query(AUTHORIZATIONS_A).hasExtendedData(ElementType.VERTEX, "v1").search();
        assertResultsCount(0, 0, searchResults);

        results = getGraph().query(AUTHORIZATIONS_B).hasExtendedData(ElementType.VERTEX, "v1").extendedDataRows();
        assertResultsCount(0, 0, results);

        searchResults = getGraph().query(AUTHORIZATIONS_B).hasExtendedData(ElementType.VERTEX, "v1").search();
        assertResultsCount(0, 0, searchResults);

        results = getGraph().query(AUTHORIZATIONS_A_AND_B).hasExtendedData(ElementType.VERTEX, "v1").extendedDataRows();
        assertResultsCount(2, 2, results);

        searchResults = getGraph().query(AUTHORIZATIONS_A_AND_B).hasExtendedData(ElementType.VERTEX, "v1").search();
        assertResultsCount(2, 2, searchResults);

        results = getGraph().query(AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(0, 0, results);

        searchResults = getGraph().query(AUTHORIZATIONS_A).search();
        assertResultsCount(1, 1, searchResults);

        results = getGraph().query(AUTHORIZATIONS_B).extendedDataRows();
        assertResultsCount(0, 0, results);

        searchResults = getGraph().query(AUTHORIZATIONS_B).search();
        assertResultsCount(0, 0, searchResults);

        results = getGraph().query(AUTHORIZATIONS_A_AND_B).extendedDataRows();
        assertResultsCount(2, 2, results);

        searchResults = getGraph().query(AUTHORIZATIONS_A_AND_B).search();
        assertResultsCount(3, 3, searchResults);

        getGraph().getVertex("v1", AUTHORIZATIONS_A).prepareMutation()
                .deleteExtendedData("table1", "row1", "color", "junit", VISIBILITY_B)
                .deleteExtendedData("table1", "row2", "color", "junit", VISIBILITY_B)
                .addExtendedData("table1", "row1", "color", "junit2", stringValue("blue"), VISIBILITY_C)
                .save(AUTHORIZATIONS_A_AND_B_AND_C);
        getGraph().flush();

        Authorizations authorizationsAandC = new Authorizations("a", "c");

        results = getGraph().query(AUTHORIZATIONS_A).extendedDataRows();
        assertResultsCount(0, 0, results);

        searchResults = getGraph().query(AUTHORIZATIONS_A).search();
        assertResultsCount(1, 1, searchResults);

        results = getGraph().query(AUTHORIZATIONS_B).extendedDataRows();
        assertResultsCount(0, 0, results);

        searchResults = getGraph().query(AUTHORIZATIONS_B).search();
        assertResultsCount(0, 0, searchResults);

        results = getGraph().query(AUTHORIZATIONS_C).extendedDataRows();
        assertResultsCount(0, 0, results);

        searchResults = getGraph().query(AUTHORIZATIONS_C).search();
        assertResultsCount(0, 0, searchResults);

        results = getGraph().query(authorizationsAandC).extendedDataRows();
        assertResultsCount(1, 1, results);

        searchResults = getGraph().query(authorizationsAandC).search();
        assertResultsCount(2, 2, searchResults);

        results = getGraph().query(AUTHORIZATIONS_A_AND_B_AND_C).extendedDataRows();
        assertResultsCount(1, 1, results);

        searchResults = getGraph().query(AUTHORIZATIONS_A_AND_B_AND_C).search();
        assertResultsCount(2, 2, searchResults);

        searchResults = getGraph().query("blue", AUTHORIZATIONS_A).search();
        assertResultsCount(0, 0, searchResults);

        results = getGraph().query("blue", AUTHORIZATIONS_B).extendedDataRows();
        assertResultsCount(0, 0, results);

        searchResults = getGraph().query("blue", AUTHORIZATIONS_B).search();
        assertResultsCount(0, 0, searchResults);

        results = getGraph().query("blue", AUTHORIZATIONS_C).extendedDataRows();
        assertResultsCount(0, 0, results);

        searchResults = getGraph().query("blue", AUTHORIZATIONS_C).search();
        assertResultsCount(0, 0, searchResults);

        results = getGraph().query("blue", AUTHORIZATIONS_A_AND_B).extendedDataRows();
        assertResultsCount(0, 0, results);

        searchResults = getGraph().query("blue", AUTHORIZATIONS_A_AND_B).search();
        assertResultsCount(0, 0, searchResults);

        results = getGraph().query("blue", authorizationsAandC).extendedDataRows();
        assertResultsCount(1, 1, results);

        searchResults = getGraph().query("blue", authorizationsAandC).search();
        assertResultsCount(1, 1, searchResults);
    }

    @Test
    public void testExtendedDataQueryAfterDeleteForEdge() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        List<ExtendedDataRow> searchResultsList = toList(getGraph().query(AUTHORIZATIONS_A).extendedDataRows());
        assertRowIdsAnyOrder(Lists.newArrayList("row1", "row2"), searchResultsList);

        getGraph().deleteEdge("e1", AUTHORIZATIONS_A);
        getGraph().flush();

        searchResultsList = toList(getGraph().query(AUTHORIZATIONS_A).extendedDataRows());
        assertRowIdsAnyOrder(Lists.newArrayList(), searchResultsList);
    }

    @Test
    public void testExtendedDataQueryEdges() {
        DateTimeValue date1 = DateTimeValue.ofEpochMillis(longValue(1487083490000L));
        DateTimeValue date2 = DateTimeValue.ofEpochMillis(longValue(1487083480000L));
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e1", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .addExtendedData("table1", "row1", "date", date1, VISIBILITY_A)
                .addExtendedData("table1", "row1", "name", stringValue("value 1"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "date", date2, VISIBILITY_A)
                .addExtendedData("table1", "row2", "name", stringValue("value 2"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().prepareEdge("e2", "v1", "v2", LABEL_LABEL1, VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        // Should not come back when finding edges
        QueryResultsIterable<Edge> queryResults = getGraph().query(AUTHORIZATIONS_A)
                .has("date", date1)
                .edges();
        assertEquals(0, queryResults.getTotalHits());

        QueryResultsIterable<? extends GeObject> searchResults = getGraph().query(AUTHORIZATIONS_A)
                .has("date", date1)
                .search();
        assertEquals(1, searchResults.getTotalHits());
        List<? extends GeObject> searchResultsList = toList(searchResults);
        assertEquals(1, searchResultsList.size());
        ExtendedDataRow searchResult = (ExtendedDataRow) searchResultsList.get(0);
        assertEquals("e1", searchResult.getId().getElementId());
        assertEquals("row1", searchResult.getId().getRowId());

        searchResults = getGraph().query(AUTHORIZATIONS_A)
                .has("name", stringValue("value 1"))
                .search();
        assertEquals(1, searchResults.getTotalHits());
        searchResultsList = toList(searchResults);
        assertEquals(1, searchResultsList.size());
        searchResult = (ExtendedDataRow) searchResultsList.get(0);
        assertEquals("e1", searchResult.getId().getElementId());
        assertEquals("row1", searchResult.getId().getRowId());
        searchResults = getGraph().query(AUTHORIZATIONS_A)
                .has("name", TextPredicate.CONTAINS, stringValue("value"))
                .search();
        assertEquals(2, searchResults.getTotalHits());
        searchResultsList = toList(searchResults);
        assertEquals(2, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList("row1", "row2"), searchResultsList);

        searchResults = getGraph().query("value", AUTHORIZATIONS_A)
                .search();
        assertEquals(2, searchResults.getTotalHits());
        searchResultsList = toList(searchResults);
        assertEquals(2, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList("row1", "row2"), searchResultsList);
    }

    @Test
    public void testExtendedDataQueryWithMultiValue() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addExtendedData("table1", "row1", "col1", "key1", stringValue("joe"), VISIBILITY_A)
                .addExtendedData("table1", "row1", "col1", "key2", stringValue("bob"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "col1", "key1", stringValue("joe"), VISIBILITY_A)
                .addExtendedData("table1", "row2", "col1", "key2", stringValue("jane"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        QueryableIterable<ExtendedDataRow> rows = v1.getExtendedData("table1");
        for (ExtendedDataRow row : rows) {
            if (row.getId().getRowId().equals("row1")) {
                assertEquals(stringValue("joe"), row.getPropertyValue("key1", "col1"));
                assertEquals(stringValue("bob"), row.getPropertyValue("key2", "col1"));
            } else if (row.getId().getRowId().equals("row2")) {
                assertEquals(stringValue("joe"), row.getPropertyValue("key1", "col1"));
                assertEquals(stringValue("jane"), row.getPropertyValue("key2", "col1"));
            } else {
                throw new GeException("invalid row: " + row.getId());
            }
        }

        QueryResultsIterable<? extends GeObject> searchResults = getGraph().query("joe", AUTHORIZATIONS_A)
                .search();
        assertEquals(2, searchResults.getTotalHits());
        List<? extends GeObject> searchResultsList = toList(searchResults);
        assertEquals(2, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList("row1", "row2"), searchResultsList);

        searchResults = getGraph().query("bob", AUTHORIZATIONS_A)
                .search();
        assertEquals(1, searchResults.getTotalHits());
        searchResultsList = toList(searchResults);
        assertEquals(1, searchResultsList.size());
        assertRowIdsAnyOrder(Lists.newArrayList("row1"), searchResultsList);
    }
}
