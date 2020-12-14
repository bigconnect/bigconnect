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
package com.mware.ge.rocksdb;

import com.mware.ge.Direction;
import com.mware.ge.Edge;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.base.GraphBaseTests;
import com.mware.ge.base.GraphTestSetup;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.values.storable.Values;
import org.junit.Assert;
import org.junit.Test;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.util.GeAssert.assertEquals;
import static com.mware.ge.util.IterableUtils.count;

public class RocksDBBaseTests extends GraphBaseTests implements GraphTestSetup {
    @Override
    public TestGraphFactory graphFactory() {
        return new RocksDBGraphFactory();
    }

    @Test
    public void testAddVertexWithoutId() {
        getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().flush();;

        Iterable<Vertex> vertices = getGraph().getVertices(AUTHORIZATIONS_A);
        int count = IterableUtils.count(vertices);
        Assert.assertEquals(3, count);
    }

    @Test
    public void testRocksIterator() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        v1.addPropertyValue("k11", "p11", Values.intValue(11), Visibility.EMPTY, AUTHORIZATIONS_A);
        v1.addPropertyValue("k12", "p12", Values.intValue(12), Visibility.EMPTY, AUTHORIZATIONS_A);
        getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A, CONCEPT_TYPE_THING);
        getGraph().flush();;
    }

    @Override
    public void testMarkEdgeHidden() {
        Vertex v1 = getGraph().addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Vertex v2 = getGraph().addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_ALL, CONCEPT_TYPE_THING);
        Edge e1 = getGraph().addEdge("v1tov2", v1, v2, LABEL_LABEL1, VISIBILITY_A, AUTHORIZATIONS_ALL);
        getGraph().flush();
        getGraph().dumpGraph();

        getGraph().markEdgeHidden(e1, VISIBILITY_A_AND_B, AUTHORIZATIONS_A);
        getGraph().flush();
        getGraph().dumpGraph();

        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getEdges(Direction.BOTH, AUTHORIZATIONS_A)));

        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(0, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdgeIds(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        assertEquals(0, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdgeInfos(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
    }

    @Override
    public RocksDBGraph getGraph() {
        return (RocksDBGraph) super.getGraph();
    }
}
