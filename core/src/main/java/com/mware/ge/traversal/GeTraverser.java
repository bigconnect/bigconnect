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
package com.mware.ge.traversal;

import com.google.common.collect.ImmutableList;
import com.mware.ge.*;
import com.mware.ge.collection.Iterables;
import com.mware.ge.util.Preconditions;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;

public class GeTraverser {
    private final Graph graph;
    private final Authorizations authorizations;

    public static final long DEFAULT_CAPACITY = 10000000;
    public static final long DEFAULT_DEGREE = 10000;
    public static final long DEFAULT_SKIP_DEGREE = 100000;
    public static final int DEFAULT_MAX_DEPTH = 50;
    public static final long NO_LIMIT = -1L;

    public GeTraverser(Graph graph, Authorizations authorizations) {
        this.graph = graph;
        this.authorizations = authorizations;
    }

    protected void checkVertexExist(String vertexId) {
        if (!graph.doesVertexExist(vertexId, authorizations))
            throw new GeException(String.format("Vertex %s does not exist", vertexId));
    }

    protected Iterator<Edge> edgesOfVertex(String source, Direction dir, String label, long limit) {
        return Iterables.limit((int) limit, graph.getVertex(source, authorizations).getEdges(dir, label, authorizations))
                .iterator();
    }

    public static void checkCapacity(long capacity, long access,
                                     String traverse) {
        if (capacity != NO_LIMIT && access > capacity) {
            throw new GeException(String.format("Exceed capacity '%s' while finding %s", capacity, traverse));
        }
    }

    public static Iterator<Edge> skipSuperNodeIfNeeded(Iterator<Edge> edges,
                                                       long degree,
                                                       long skipDegree) {
        if (skipDegree <= 0L) {
            return edges;
        }
        List<Edge> edgeList = new ArrayList<>();
        for (int i = 1; edges.hasNext(); i++) {
            Edge edge = edges.next();
            if (i <= degree) {
                edgeList.add(edge);
            }
            if (i >= skipDegree) {
                return Collections.emptyIterator();
            }
        }
        return edgeList.iterator();
    }

    protected static <V> Set<V> newSet() {
        return new HashSet<>();
    }

    protected static <K, V> Map<K, V> newMap() {
        return new HashMap<>();
    }

    public static class Node {
        private String id;
        private Node parent;

        public Node(String id) {
            this(id, null);
        }

        public Node(String id, Node parent) {
            Preconditions.checkNotNull(id);
            this.id = id;
            this.parent = parent;
        }

        public String id() {
            return this.id;
        }

        public Node parent() {
            return this.parent;
        }

        public List<String> path() {
            List<String> ids = new ArrayList<>();
            Node current = this;
            do {
                ids.add(current.id);
                current = current.parent;
            } while (current != null);
            Collections.reverse(ids);
            return ids;
        }

        public List<String> joinPath(Node back) {
            // Get self path
            List<String> path = this.path();

            // Get reversed other path
            List<String> backPath = back.path();
            Collections.reverse(backPath);

            // Avoid loop in path
            if (CollectionUtils.containsAny(path, backPath)) {
                return ImmutableList.of();
            }

            // Append other path behind self path
            path.addAll(backPath);
            return path;
        }

        public boolean contains(String id) {
            Node node = this;
            do {
                if (node.id.equals(id)) {
                    return true;
                }
                node = node.parent;
            } while (node != null);
            return false;
        }

        @Override
        public int hashCode() {
            return this.id.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Node)) {
                return false;
            }
            Node other = (Node) object;
            return Objects.equals(this.id, other.id) &&
                    Objects.equals(this.parent, other.parent);
        }
    }

    public static class PathSet extends HashSet<Path> {

        private static final long serialVersionUID = -8237531948776524872L;

        public Set<String> vertices() {
            Set<String> vertices = new HashSet<>();
            for (Path path : this) {
                vertices.addAll(path.vertices());
            }
            return vertices;
        }
    }
}
