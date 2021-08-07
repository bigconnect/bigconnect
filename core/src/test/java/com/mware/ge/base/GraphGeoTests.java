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

import com.mware.ge.FetchHints;
import com.mware.ge.Graph;
import com.mware.ge.TextIndexHint;
import com.mware.ge.Vertex;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.event.GraphEventListener;
import com.mware.ge.query.Compare;
import com.mware.ge.query.GeoCompare;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.query.builder.GeQueryBuilders;
import com.mware.ge.type.*;
import com.mware.ge.values.storable.GeoPointValue;
import com.mware.ge.values.storable.GeoShapeValue;
import com.mware.ge.values.storable.TextValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.query.builder.GeQueryBuilders.hasFilter;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.GeAssert.assertResultsCount;
import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.*;
import static com.mware.ge.values.storable.Values.geoShapeValue;
import static org.junit.Assume.assumeTrue;

@RunWith(JUnit4.class)
public abstract class GraphGeoTests implements GraphTestSetup {
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
    public void testStoreGeoPoint() {
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("location", geoPointValue(38.9186, -77.2297), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("location", geoPointValue(38.9544, -77.3464), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        List<Vertex> vertices = toList(getGraph().query(
                hasFilter("location", GeoCompare.WITHIN, geoCircleValue(38.9186, -77.2297, 1)),
                AUTHORIZATIONS_A
        ).vertices());
        assertEquals(1, count(vertices));
        GeoPointValue geoPoint = (GeoPointValue) vertices.get(0).getPropertyValue("location");
        assertEquals(38.9186, geoPoint.getLatitude().doubleValue(), 0.001);
        assertEquals(-77.2297, geoPoint.getLongitude().doubleValue(), 0.001);

        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A_AND_B, CONCEPT_TYPE_THING);
        v3.prepareMutation()
                .setProperty("location", geoPointValue(39.0299, -77.5121), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        vertices = toList(getGraph().query(
                hasFilter("location", GeoCompare.WITHIN, geoCircleValue(39.0299, -77.5121, 1)),
                AUTHORIZATIONS_A
        ).vertices());
        assertEquals(1, count(vertices));
        geoPoint = (GeoPointValue) vertices.get(0).getPropertyValue("location");
        assertEquals(39.0299, geoPoint.getLatitude().doubleValue(), 0.001);
        assertEquals(-77.5121, geoPoint.getLongitude().doubleValue(), 0.001);

        vertices = toList(getGraph().query(
                hasFilter("location", GeoCompare.WITHIN, geoCircleValue(38.9186, -77.2297, 25)),
                AUTHORIZATIONS_A
        ).vertices());
        assertEquals(2, count(vertices));

        vertices = toList(getGraph().query(
                hasFilter("location", GeoCompare.WITHIN, geoRectValue(new GeoPoint(39, -78), new GeoPoint(38, -77))),
                AUTHORIZATIONS_A
        ).vertices());
        assertEquals(2, count(vertices));

        vertices = toList(getGraph().query(
                hasFilter("location", GeoCompare.WITHIN, geoHashValue(new GeoHash(38.9186, -77.2297, 2))),
                AUTHORIZATIONS_A
        ).vertices());
        assertEquals(3, count(vertices));

        vertices = toList(getGraph().query(
                hasFilter("location", GeoCompare.WITHIN, geoHashValue(new GeoHash(38.9186, -77.2297, 3))),
                AUTHORIZATIONS_A
        ).vertices());
        assertEquals(1, count(vertices));
    }

    @Test
    public void testStoreGeoCircle() {
        assumeTrue("GeoCircle storage and queries are not supported", isAdvancedGeoQuerySupported());

        GeoCircle within = new GeoCircle(38.6270, -90.1994, 100);
        GeoCircle contains = new GeoCircle(38.6270, -90.1994, 800);
        GeoCircle intersects = new GeoCircle(38.6270, -80.0, 500);
        GeoCircle disjoint = new GeoCircle(38.6270, -70.0, 500);

        doALLGeoshapeTestQueries(intersects, disjoint, within, contains);
    }

    @Test
    public void testStoreGeoRect() {
        assumeTrue("GeoRect storage and queries are not supported", isAdvancedGeoQuerySupported());

        GeoRect within = new GeoRect(new GeoPoint(39.52632, -91.35059), new GeoPoint(37.72767, -89.0482));
        GeoRect contains = new GeoRect(new GeoPoint(45.82157, -99.42435), new GeoPoint(31.43242, -80.97444));
        GeoRect intersects = new GeoRect(new GeoPoint(43.1236, -85.75962), new GeoPoint(34.13039, -74.24038));
        GeoRect disjoint = new GeoRect(new GeoPoint(43.1236, -75.75962), new GeoPoint(34.13039, -64.24038));

        doALLGeoshapeTestQueries(intersects, disjoint, within, contains);
    }

    @Test
    public void testStoreGeoLine() {
        assumeTrue("GeoLine storage and queries are not supported", isAdvancedGeoQuerySupported());

        GeoLine within = new GeoLine(new GeoPoint(39.5, -90.1994), new GeoPoint(37.9, -90.1994));
        GeoLine contains = new GeoLine(new GeoPoint(35.0, -100.0), new GeoPoint(39.5, -80));
        GeoLine intersects = new GeoLine(new GeoPoint(38.67, -85), new GeoPoint(38.67, -80));
        GeoLine disjoint = new GeoLine(new GeoPoint(38.6, -74.0), new GeoPoint(38.6, -68));

        doALLGeoshapeTestQueries(intersects, disjoint, within, contains);
    }

    @Test
    public void testStoreGeoPolygon() {
        assumeTrue("GeoPolygon storage and queries are not supported", isAdvancedGeoQuerySupported());

        GeoPolygon within = new GeoPolygon(Arrays.asList(new GeoPoint(39.4, -91.0), new GeoPoint(38.1, -91.0), new GeoPoint(38.627, -89.0), new GeoPoint(39.4, -91.0)));
        GeoPolygon contains = new GeoPolygon(Arrays.asList(new GeoPoint(50.0, -98.0), new GeoPoint(26.0, -98.0), new GeoPoint(38.627, -75.0), new GeoPoint(50.0, -98.0)));
        GeoPolygon intersects = new GeoPolygon(Arrays.asList(new GeoPoint(43.0, -86.0), new GeoPoint(34.0, -86.0), new GeoPoint(38.627, -74.0), new GeoPoint(43.0, -86.0)));
        GeoPolygon disjoint = new GeoPolygon(Arrays.asList(new GeoPoint(43.0, -75.0), new GeoPoint(34.0, -75.0), new GeoPoint(38.627, -65.0), new GeoPoint(43.0, -75.0)));

        // put a hole in the within triangle to make sure it gets stored/retrieved properly
        within.addHole(Arrays.asList(new GeoPoint(39.0, -90.5), new GeoPoint(38.627, -89.5), new GeoPoint(38.5, -90.5), new GeoPoint(39.0, -90.5)));

        doALLGeoshapeTestQueries(intersects, disjoint, within, contains);
    }

    @Test
    public void testStoreGeoCollection() {
        assumeTrue("GeoCollection storage and queries are not supported", isAdvancedGeoQuerySupported());

        GeoCollection within = new GeoCollection().addShape(new GeoCircle(38.6270, -90.1994, 100));
        GeoCollection contains = new GeoCollection().addShape(new GeoCircle(38.6270, -90.1994, 800));
        GeoCollection intersects = new GeoCollection().addShape(new GeoCircle(38.6270, -80.0, 500));
        GeoCollection disjoint = new GeoCollection().addShape(new GeoCircle(38.6270, -70.0, 500));

        // Add another shape to within to make sure it stores/retrieves properly
        within.addShape(new GeoPoint(38.6270, -90.1994));

        doALLGeoshapeTestQueries(intersects, disjoint, within, contains);
    }

    // See https://jsfiddle.net/mwizeman/do5ufpa9/ for a handy way to visualize the layout of the inputs and all of the search areas
    private void doALLGeoshapeTestQueries(GeoShape intersects, GeoShape disjoint, GeoShape within, GeoShape contains) {
        getGraph().defineProperty("location").dataType(GeoShapeValue.class).define();
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING).setProperty("location", geoShapeValue(intersects), VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v2", VISIBILITY_A, CONCEPT_TYPE_THING).setProperty("location", geoShapeValue(disjoint), VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v3", VISIBILITY_A, CONCEPT_TYPE_THING).setProperty("location", geoShapeValue(within), VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        getGraph().prepareVertex("v4", VISIBILITY_A, CONCEPT_TYPE_THING).setProperty("location", geoShapeValue(contains), VISIBILITY_A).save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        // All of the different search areas to try
        GeoCircle circle = new GeoCircle(38.6270, -90.1994, 500);
        GeoRect rect = new GeoRect(new GeoPoint(43.1236, -95.9590), new GeoPoint(34.1303, -84.4397));
        GeoPolygon triangle = new GeoPolygon(Arrays.asList(new GeoPoint(43.1236, -95.9590), new GeoPoint(34.1303, -95.9590), new GeoPoint(38.6270, -84.4397), new GeoPoint(43.1236, -95.9590)));
        GeoLine line = new GeoLine(Arrays.asList(new GeoPoint(34.1303, -95.9590), new GeoPoint(43.1236, -84.4397), new GeoPoint(38.6270, -84.4397)));
        GeoCollection collection = new GeoCollection()
                .addShape(new GeoCircle(38.6270, -90.1994, 250))
                .addShape(new GeoLine(new GeoPoint(39.5, -84.0), new GeoPoint(38.5, -84.0)));

        Arrays.asList(circle, rect, triangle, line, collection).forEach(searchArea -> {
            QueryResultsIterable<Vertex> vertices = getGraph().query(hasFilter("location", GeoCompare.INTERSECTS, geoShapeValue(searchArea)), AUTHORIZATIONS_A_AND_B)
                    .vertices();
            assertEquals("Incorrect total hits match INTERSECTS for shape ", 3, vertices.getTotalHits());
            assertVertexIdsAnyOrder(vertices, "v1", "v3", "v4");

            vertices = getGraph().query(hasFilter("location", GeoCompare.DISJOINT, geoShapeValue(searchArea)), AUTHORIZATIONS_A_AND_B).vertices();
            assertEquals("Incorrect total hits match DISJOINT for shape ", 1, vertices.getTotalHits());
            assertVertexIdsAnyOrder(vertices, "v2");

            if (searchArea != line) {
                vertices = getGraph().query(hasFilter("location", GeoCompare.WITHIN, geoShapeValue(searchArea)), AUTHORIZATIONS_A_AND_B).vertices();
                assertEquals("Incorrect total hits match WITHIN for shape ", 1, vertices.getTotalHits());
                assertVertexIdsAnyOrder(vertices, "v3");

                vertices = getGraph().query(hasFilter("location", GeoCompare.CONTAINS, geoShapeValue(searchArea)), AUTHORIZATIONS_A_AND_B).vertices();
                if (intersects instanceof GeoLine) {
                    assertEquals("Incorrect total hits match CONTAINS for shape ", 0, vertices.getTotalHits());
                } else {
                    assertEquals("Incorrect total hits match CONTAINS for shape ", 1, vertices.getTotalHits());
                    assertVertexIdsAnyOrder(vertices, "v4");
                }
            }
        });

        // Punch a hole in the polygon around the "within" shape and make sure that the results look ok
        triangle.addHole(Arrays.asList(new GeoPoint(40, -92.5), new GeoPoint(40, -88.5), new GeoPoint(37.4, -88.5), new GeoPoint(37.4, -92.5), new GeoPoint(40, -92.5)));
        QueryResultsIterable<Vertex> vertices = getGraph().query(hasFilter("location", GeoCompare.INTERSECTS, geoShapeValue(triangle)), AUTHORIZATIONS_A_AND_B).vertices();
        assertEquals("Incorrect total hits match INTERSECTS for polygon with hole", 2, vertices.getTotalHits());
        assertVertexIdsAnyOrder(vertices, "v1", "v4");

        vertices = getGraph().query(hasFilter("location", GeoCompare.DISJOINT, geoShapeValue(triangle)), AUTHORIZATIONS_A_AND_B).vertices();
        assertEquals("Incorrect total hits match DISJOINT for polygon with hole", 2, vertices.getTotalHits());
        assertVertexIdsAnyOrder(vertices, "v2", "v3");

        vertices = getGraph().query(hasFilter("location", GeoCompare.WITHIN, geoShapeValue(triangle)), AUTHORIZATIONS_A_AND_B).vertices();
        assertEquals("Incorrect total hits match WITHIN for polygon with hole", 0, vertices.getTotalHits());

        vertices = getGraph().query(hasFilter("location", GeoCompare.CONTAINS, geoShapeValue(triangle)), AUTHORIZATIONS_A_AND_B).vertices();
        if (intersects instanceof GeoLine) {
            assertEquals("Incorrect total hits match CONTAINS for polygon with hole", 0, vertices.getTotalHits());
        } else {
            assertEquals("Incorrect total hits match CONTAINS for polygon with hole", 1, vertices.getTotalHits());
            assertVertexIdsAnyOrder(vertices, "v4");
        }
    }

    @Test
    public void testGeoLocationsWithDifferentKeys() {
        getGraph().defineProperty("prop1").dataType(GeoPointValue.class).textIndexHint(TextIndexHint.ALL).define();

        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "prop1", geoPointValue(38.9186, -77.2297), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        QueryResultsIterable<Vertex> vertices = getGraph().query(hasFilter("prop1", GeoCompare.WITHIN, geoCircleValue(38.9186, -77.2297, 1)), AUTHORIZATIONS_A).vertices();
        assertVertexIdsAnyOrder(vertices, "v1");
        assertResultsCount(1, 1, vertices);

        Vertex v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        v1.prepareMutation()
                .addPropertyValue("key2", "prop1", geoPointValue(38.6270, -90.1994), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        vertices = getGraph().query(hasFilter("prop1", GeoCompare.WITHIN, geoCircleValue(38.9186, -77.2297, 1)), AUTHORIZATIONS_A).vertices();
        if (multivalueGeopointQueryWithinMeansAny()) {
            assertResultsCount(0, 0, vertices);
        } else {
            assertResultsCount(1, 1, vertices);
        }

        vertices = getGraph().query(hasFilter("prop1", GeoCompare.INTERSECTS, geoCircleValue(38.9186, -77.2297, 1)), AUTHORIZATIONS_A).vertices();
        assertVertexIdsAnyOrder(vertices, "v1");
        assertResultsCount(1, 1, vertices);

        vertices = getGraph().query(hasFilter("prop1", GeoCompare.WITHIN, geoCircleValue(38.6270, -90.1994, 1)), AUTHORIZATIONS_A).vertices();
        if (multivalueGeopointQueryWithinMeansAny()) {
            assertResultsCount(0, 0, vertices);
        } else {
            assertResultsCount(1, 1, vertices);
        }

        vertices = getGraph().query(hasFilter("prop1", GeoCompare.INTERSECTS, geoCircleValue(38.6270, -90.1994, 1)), AUTHORIZATIONS_A).vertices();
        assertVertexIdsAnyOrder(vertices, "v1");
        assertResultsCount(1, 1, vertices);
    }

    @Test
    public void testDeleteGeoLocationProperty() {
        getGraph().defineProperty("prop1").dataType(TextValue.class).textIndexHint(TextIndexHint.ALL).define();
        getGraph().defineProperty("prop2").dataType(GeoPointValue.class).textIndexHint(TextIndexHint.ALL).define();
        getGraph().prepareVertex("v1", VISIBILITY_EMPTY, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "prop1", stringValue("value1"), VISIBILITY_A)
                .addPropertyValue("key1", "prop2", geoPointValue(38.9186, -77.2297), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        Vertex v1 = getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A);
        v1.prepareMutation()
                .deleteProperties("key1", "prop1")
                .deleteProperties("key1", "prop2")
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        QueryResultsIterable<Vertex> vertices = getGraph().query(hasFilter("prop1", Compare.EQUAL, stringValue("value1")), AUTHORIZATIONS_A).vertices();
        assertResultsCount(0, 0, vertices);
        vertices = getGraph().query(hasFilter("prop2", GeoCompare.WITHIN, geoCircleValue(38.9186, -77.2297, 1)), AUTHORIZATIONS_A).vertices();
        assertResultsCount(0, 0, vertices);
        QueryResultsIterable<String> vertexIds = getGraph().query(hasFilter("prop2", GeoCompare.WITHIN, geoCircleValue(38.9186, -77.2297, 1)), AUTHORIZATIONS_A).vertexIds();
        assertResultsCount(0, 0, vertexIds);
    }

    protected boolean isAdvancedGeoQuerySupported() {
        return true;
    }

    // In Elasticsearch 5, searching WITHIN a geo shape on a GeoPoint field meant that ALL points in a multi-valued field must fall inside the shape to be a match
    // In Elasticsearch 7, searching WITHIN a geo shape on a GeoPoint field means that ANY points in a multi-valued field that fall inside the shape are a match
    protected boolean multivalueGeopointQueryWithinMeansAny() {
        return true;
    }
}
