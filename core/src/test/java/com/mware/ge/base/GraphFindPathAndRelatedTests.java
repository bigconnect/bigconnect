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

import com.mware.ge.*;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.event.GraphEventListener;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.IterableUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.*;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.GeAssert.assertEquals;
import static com.mware.ge.util.IterableUtils.*;
import static com.mware.ge.util.IterableUtils.count;

@RunWith(JUnit4.class)
public abstract class GraphFindPathAndRelatedTests implements GraphTestSetup {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(GraphFindPathAndRelatedTests.class);

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
    public void testFindPaths() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v4 = getGraph().addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v5 = getGraph().addVertex("v5", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v6 = getGraph().addVertex("v6", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge(v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v2
        getGraph().addEdge(v2, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v4
        getGraph().addEdge(v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v3
        getGraph().addEdge(v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v4
        getGraph().addEdge(v3, v5, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v5
        getGraph().addEdge(v4, v6, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v4 -> v6
        getGraph().flush();

        Set<Path> paths = toSet(getGraph().findPaths(new FindPathOptions("v1", "v2", 2), AUTHORIZATIONS_A));
        Set<Path> pathsByLabels = toSet(getGraph().findPaths(new FindPathOptions("v1", "v2", 2).setLabels(LABEL_LABEL1), AUTHORIZATIONS_A));
        assertEquals(pathsByLabels, paths);
        Set<Path> pathsByBadLabel = toSet(getGraph().findPaths(new FindPathOptions("v1", "v2", 2).setLabels(LABEL_BAD), AUTHORIZATIONS_A));
        assertEquals(0, pathsByBadLabel.size());
        assertPaths(
                paths,
                new Path("v1", "v2")
        );

        paths = toSet(getGraph().findPaths(new FindPathOptions("v1", "v4", 2), AUTHORIZATIONS_A));
        pathsByLabels = toSet(getGraph().findPaths(new FindPathOptions("v1", "v4", 2).setLabels(LABEL_LABEL1), AUTHORIZATIONS_A));
        assertEquals(pathsByLabels, paths);
        pathsByBadLabel = toSet(getGraph().findPaths(new FindPathOptions("v1", "v4", 2).setLabels(LABEL_BAD), AUTHORIZATIONS_A));
        assertEquals(0, pathsByBadLabel.size());
        assertPaths(
                paths,
                new Path("v1", "v2", "v4"),
                new Path("v1", "v3", "v4")
        );

        paths = toSet(getGraph().findPaths(new FindPathOptions("v4", "v1", 2), AUTHORIZATIONS_A));
        pathsByLabels = toSet(getGraph().findPaths(new FindPathOptions("v4", "v1", 2).setLabels(LABEL_LABEL1), AUTHORIZATIONS_A));
        assertEquals(pathsByLabels, paths);
        pathsByBadLabel = toSet(getGraph().findPaths(new FindPathOptions("v4", "v1", 2).setLabels(LABEL_BAD), AUTHORIZATIONS_A));
        assertEquals(0, pathsByBadLabel.size());
        assertPaths(
                paths,
                new Path("v4", "v2", "v1"),
                new Path("v4", "v3", "v1")
        );

        paths = toSet(getGraph().findPaths(new FindPathOptions("v1", "v6", 3), AUTHORIZATIONS_A));
        pathsByLabels = toSet(getGraph().findPaths(new FindPathOptions("v1", "v6", 3).setLabels(LABEL_LABEL1), AUTHORIZATIONS_A));
        assertEquals(pathsByLabels, paths);
        pathsByBadLabel = toSet(getGraph().findPaths(new FindPathOptions("v1", "v6", 3).setLabels(LABEL_BAD), AUTHORIZATIONS_A));
        assertEquals(0, pathsByBadLabel.size());
        assertPaths(
                paths,
                new Path("v1", "v2", "v4", "v6"),
                new Path("v1", "v3", "v4", "v6")
        );
    }

    @Test
    public void testFindPathExcludeLabels() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v4 = getGraph().addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);

        getGraph().addEdge(v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge(v2, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);

        getGraph().addEdge(v1, v3, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge(v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);

        getGraph().flush();

        assertPaths(
                getGraph().findPaths(new FindPathOptions("v1", "v4", 2), AUTHORIZATIONS_A),
                new Path("v1", "v2", "v4"),
                new Path("v1", "v3", "v4")
        );

        assertPaths(
                getGraph().findPaths(new FindPathOptions("v1", "v4", 2).setExcludedLabels(LABEL_LABEL2), AUTHORIZATIONS_A),
                new Path("v1", "v2", "v4")
        );
        assertPaths(
                getGraph().findPaths(new FindPathOptions("v1", "v4", 3).setExcludedLabels(LABEL_LABEL2), AUTHORIZATIONS_A),
                new Path("v1", "v2", "v4")
        );
    }

    private void assertPaths(Iterable<Path> found, Path... expected) {
        List<Path> foundPaths = toList(found);
        List<Path> expectedPaths = new ArrayList<>();
        Collections.addAll(expectedPaths, expected);

        assertEquals(expectedPaths.size(), foundPaths.size());
        for (Path foundPath : foundPaths) {
            if (!expectedPaths.remove(foundPath)) {
                fail("Unexpected path: " + foundPath);
            }
        }
    }

    @Test
    public void testFindPathsWithSoftDeletedEdges() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_EMPTY, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge(v1, v2, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A); // v1 -> v2
        Edge v2ToV3 = getGraph().addEdge(v2, v3, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A); // v2 -> v3
        getGraph().flush();

        List<Path> paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A));
        assertPaths(
                paths,
                new Path("v1", "v2", "v3")
        );

        getGraph().softDeleteEdge(v2ToV3, AUTHORIZATIONS_A);
        getGraph().flush();

        assertNull(getGraph().getEdge(v2ToV3.getId(), AUTHORIZATIONS_A));
        paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A));
        assertEquals(0, paths.size());
    }

    @Test
    public void testFindPathsWithHiddenEdges() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B, CONCEPT_TYPE_THING);
        getGraph().addEdge(v1, v2, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B); // v1 -> v2
        Edge v2ToV3 = getGraph().addEdge(v2, v3, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B); // v2 -> v3
        getGraph().flush();

        List<Path> paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A_AND_B));
        assertPaths(
                paths,
                new Path("v1", "v2", "v3")
        );

        getGraph().markEdgeHidden(v2ToV3, VISIBILITY_A, AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        assertNull(getGraph().getEdge(v2ToV3.getId(), AUTHORIZATIONS_A_AND_B));
        paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A));
        assertEquals(0, paths.size());

        paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_B));
        assertEquals(1, paths.size());
    }

    @Test
    public void testFindPathsMultiplePaths() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v4 = getGraph().addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v5 = getGraph().addVertex("v5", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);

        getGraph().addEdge(v1, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v4
        getGraph().addEdge(v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v3
        getGraph().addEdge(v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v4
        getGraph().addEdge(v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v3
        getGraph().addEdge(v4, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v4 -> v2
        getGraph().addEdge(v2, v5, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v5
        getGraph().flush();

        List<Path> paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v2", 2), AUTHORIZATIONS_A));
        assertPaths(
                paths,
                new Path("v1", "v4", "v2"),
                new Path("v1", "v3", "v2")
        );

        paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v2", 3), AUTHORIZATIONS_A));
        assertPaths(
                paths,
                new Path("v1", "v4", "v2"),
                new Path("v1", "v3", "v2"),
                new Path("v1", "v3", "v4", "v2"),
                new Path("v1", "v4", "v3", "v2")
        );

        paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v5", 2), AUTHORIZATIONS_A));
        assertPaths(paths);

        paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v5", 3), AUTHORIZATIONS_A));
        assertPaths(
                paths,
                new Path("v1", "v4", "v2", "v5"),
                new Path("v1", "v3", "v2", "v5")
        );
    }

    @Test
    public void testFindPathsWithDifferentVisibilityData() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_EMPTY, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);

        getGraph().addEdge("v1v2", v1, v2, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A); // v1 -> v2
        getGraph().addEdge("v2v3", v2, v3, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A); // v2 -> v3
        getGraph().addEdge("v3v1", v3, v1, LABEL_LABEL1, VISIBILITY_EMPTY, AUTHORIZATIONS_A); // v3 -> v1
        getGraph().flush();

        List<Path> paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A));
        assertPaths(
                paths,
                new Path("v1", "v2", "v3"),
                new Path("v1", "v3")
        );

        getGraph().getEdge("v3v1", AUTHORIZATIONS_A)
                .prepareMutation()
                .alterElementVisibility(VISIBILITY_A)
                .save(AUTHORIZATIONS_A);

        getGraph().getVertex("v2", AUTHORIZATIONS_A)
                .prepareMutation()
                .alterElementVisibility(VISIBILITY_A)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_EMPTY));
        assertEquals(0, paths.size());

        paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v3", 3), AUTHORIZATIONS_EMPTY));
        assertEquals(0, paths.size());

        paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v3", 2), AUTHORIZATIONS_A));
        assertPaths(
                paths,
                new Path("v1", "v2", "v3"),
                new Path("v1", "v3")
        );

        getGraph().getVertex("v2", AUTHORIZATIONS_A)
                .prepareMutation()
                .alterElementVisibility(Visibility.EMPTY)
                .save(AUTHORIZATIONS_A);
        getGraph().flush();

        paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v2", 4), AUTHORIZATIONS_EMPTY));
        assertPaths(
                paths,
                new Path("v1", "v2")
        );
    }

    @Test
    public void testHasPath() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v4 = getGraph().addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v5 = getGraph().addVertex("v5", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);

        getGraph().addEdge(v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v3
        getGraph().addEdge(v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v4
        getGraph().addEdge(v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v3
        getGraph().addEdge(v4, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v4 -> v2
        getGraph().addEdge(v2, v5, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v5
        getGraph().flush();

        List<Path> paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v4", 2, true), AUTHORIZATIONS_A));
        assertEquals(1, paths.size());

        paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v4", 3, true), AUTHORIZATIONS_A));
        assertEquals(1, paths.size());

        paths = toList(getGraph().findPaths(new FindPathOptions("v1", "v5", 2, true), AUTHORIZATIONS_A));
        assertEquals(0, paths.size());
    }

    @Test
    public void testFindRelatedEdges() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v4 = getGraph().addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Edge ev1v2 = getGraph().addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev1v3 = getGraph().addEdge("e v1->v3", v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev2v3 = getGraph().addEdge("e v2->v3", v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev3v1 = getGraph().addEdge("e v3->v1", v3, v1, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e v3->v4", v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        vertexIds.add("v3");
        Iterable<String> edgeIds = toList(getGraph().findRelatedEdgeIds(vertexIds, AUTHORIZATIONS_A));
        assertEquals(4, count(edgeIds));
        IterableUtils.assertContains(ev1v2.getId(), edgeIds);
        IterableUtils.assertContains(ev1v3.getId(), edgeIds);
        IterableUtils.assertContains(ev2v3.getId(), edgeIds);
        IterableUtils.assertContains(ev3v1.getId(), edgeIds);
    }

    @Test
    public void testFindRelatedEdgeIdsForVertices() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v4 = getGraph().addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Edge ev1v2 = getGraph().addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev1v3 = getGraph().addEdge("e v1->v3", v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev2v3 = getGraph().addEdge("e v2->v3", v2, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev3v1 = getGraph().addEdge("e v3->v1", v3, v1, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e v3->v4", v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);

        List<Vertex> vertices = new ArrayList<>();
        vertices.add(v1);
        vertices.add(v2);
        vertices.add(v3);
        Iterable<String> edgeIds = toList(getGraph().findRelatedEdgeIdsForVertices(vertices, AUTHORIZATIONS_A));
        assertEquals(4, count(edgeIds));
        IterableUtils.assertContains(ev1v2.getId(), edgeIds);
        IterableUtils.assertContains(ev1v3.getId(), edgeIds);
        IterableUtils.assertContains(ev2v3.getId(), edgeIds);
        IterableUtils.assertContains(ev3v1.getId(), edgeIds);
    }

    @Test
    public void testFindRelatedEdgeSummary() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v3 = getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v4 = getGraph().addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e v1->v3", v1, v3, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e v2->v3", v2, v3, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e v3->v1", v3, v1, LABEL_LABEL2, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().addEdge("e v3->v4", v3, v4, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        vertexIds.add("v3");
        List<RelatedEdge> relatedEdges = toList(getGraph().findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(4, relatedEdges.size());
        IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v2", LABEL_LABEL1, v1.getId(), v2.getId()), relatedEdges);
        IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v3", LABEL_LABEL1, v1.getId(), v3.getId()), relatedEdges);
        IterableUtils.assertContains(new RelatedEdgeImpl("e v2->v3", LABEL_LABEL2, v2.getId(), v3.getId()), relatedEdges);
        IterableUtils.assertContains(new RelatedEdgeImpl("e v3->v1", LABEL_LABEL2, v3.getId(), v1.getId()), relatedEdges);
    }

    @Test
    public void testFindRelatedEdgeSummaryAfterSoftDelete() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Edge e1 = getGraph().addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        List<RelatedEdge> relatedEdges = toList(getGraph().findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(1, relatedEdges.size());
        IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v2", LABEL_LABEL1, v1.getId(), v2.getId()), relatedEdges);

        getGraph().softDeleteEdge(e1, AUTHORIZATIONS_A);
        getGraph().flush();

        relatedEdges = toList(getGraph().findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(0, relatedEdges.size());
    }

    @Test
    public void testFindRelatedEdgeSummaryAfterMarkedHidden() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        Edge e1 = getGraph().addEdge("e v1->v2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        List<String> vertexIds = new ArrayList<>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        List<RelatedEdge> relatedEdges = toList(getGraph().findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(1, relatedEdges.size());
        IterableUtils.assertContains(new RelatedEdgeImpl("e v1->v2", LABEL_LABEL1, v1.getId(), v2.getId()), relatedEdges);

        getGraph().markEdgeHidden(e1, VISIBILITY_A, AUTHORIZATIONS_A);
        getGraph().flush();

        relatedEdges = toList(getGraph().findRelatedEdgeSummary(vertexIds, AUTHORIZATIONS_A));
        assertEquals(0, relatedEdges.size());
    }
}
