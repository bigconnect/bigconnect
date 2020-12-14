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

import com.mware.ge.*;
import com.mware.ge.collection.Iterators;
import com.mware.ge.util.Preconditions;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class ShortestPathGeTraverser extends GeTraverser {
    public ShortestPathGeTraverser(Graph graph, Authorizations authorizations) {
        super(graph, authorizations);
    }

    public Path shortestPath(String sourceV, String targetV) {
        return shortestPath(sourceV, targetV, Direction.BOTH, null, DEFAULT_MAX_DEPTH, DEFAULT_DEGREE, DEFAULT_SKIP_DEGREE, DEFAULT_CAPACITY);
    }
    public Path shortestPath(
            String sourceV,
            String targetV,
            Direction dir,
            String label,
            int depth,
            long degree,
            long skipDegree,
            long capacity
    ) {
        Preconditions.checkNotNull(sourceV);
        Preconditions.checkNotNull(targetV);
        checkVertexExist(sourceV);
        checkVertexExist(targetV);
        Preconditions.checkNotNull(dir);

        if (sourceV.equals(targetV)) {
            return new Path(sourceV);
        }

        InnerSpTraverser traverser = new InnerSpTraverser(sourceV, targetV, dir, label, degree, skipDegree, capacity);

        PathSet paths;

        while (true) {
            // Found, reach max depth or reach capacity, stop searching
            if (!(paths = traverser.forward(false)).isEmpty() || --depth <= 0) {
                break;
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");

            if (!(paths = traverser.backward(false)).isEmpty() ||
                    --depth <= 0) {
                if (!paths.isEmpty()) {
                    Path path = paths.iterator().next();
                    Collections.reverse(path.vertices());
                }
                break;
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");
        }
        return paths.isEmpty() ? Path.EMPTY_PATH : paths.iterator().next();
    }

    private class InnerSpTraverser {
        private Map<String, Node> sources = newMap();
        private Map<String, Node> targets = newMap();

        private final Direction direction;
        private final String label;
        private final long degree;
        private final long skipDegree;
        private final long capacity;
        private long size;

        public InnerSpTraverser(String sourceV, String targetV, Direction dir, String label,
                         long degree, long skipDegree, long capacity) {
            this.sources.put(sourceV, new Node(sourceV));
            this.targets.put(targetV, new Node(targetV));
            this.direction = dir;
            this.label = label;
            this.degree = degree;
            this.skipDegree = skipDegree;
            this.capacity = capacity;
            this.size = 0L;
        }

        /**
         * Search forward from source
         */
        public PathSet forward(boolean all) {
            PathSet paths = new PathSet();
            Map<String, Node> newVertices = newMap();
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;
            // Traversal vertices of previous level
            for (Node v : this.sources.values()) {
                Iterator<Edge> edges = edgesOfVertex(v.id(), this.direction, this.label, degree);
                edges = skipSuperNodeIfNeeded(edges, this.degree, this.skipDegree);
                while (edges.hasNext()) {
                    Edge edge = edges.next();
                    String target = edge.getOtherVertexId(v.id());

                    // If cross point exists, shortest path found, concat them
                    if (this.targets.containsKey(target)) {
                        if (this.superNode(target, this.direction)) {
                            continue;
                        }
                        paths.add(new Path(v.joinPath(this.targets.get(target)).toArray(new String[0])));
                        if (!all) {
                            return paths;
                        }
                    }

                    /*
                     * Not found shortest path yet, node is added to
                     * newVertices if:
                     * 1. not in sources and newVertices yet
                     * 2. path of node doesn't have loop
                     */
                    if (!newVertices.containsKey(target) &&
                            !this.sources.containsKey(target) &&
                            !v.contains(target)) {
                        newVertices.put(target, new Node(target, v));
                    }
                }
            }

            // Re-init sources
            this.sources = newVertices;
            this.size += newVertices.size();

            return paths;
        }

        /**
         * Search backward from target
         */
        public PathSet backward(boolean all) {
            PathSet paths = new PathSet();
            Map<String, Node> newVertices = newMap();
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;
            Direction opposite = this.direction.reverse();
            // Traversal vertices of previous level
            for (Node v : this.targets.values()) {
                Iterator<Edge> edges = edgesOfVertex(v.id(), opposite, this.label, degree);
                edges = skipSuperNodeIfNeeded(edges, this.degree, this.skipDegree);
                while (edges.hasNext()) {
                    Edge edge = edges.next();
                    String target = edge.getOtherVertexId(v.id());

                    // If cross point exists, shortest path found, concat them
                    if (this.sources.containsKey(target)) {
                        if (this.superNode(target, opposite)) {
                            continue;
                        }
                        paths.add(new Path(v.joinPath(this.sources.get(target)).toArray(new String[0])));
                        if (!all) {
                            return paths;
                        }
                    }

                    /*
                     * Not found shortest path yet, node is added to
                     * newVertices if:
                     * 1. not in targets and newVertices yet
                     * 2. path of node doesn't have loop
                     */
                    if (!newVertices.containsKey(target) &&
                            !this.targets.containsKey(target) &&
                            !v.contains(target)) {
                        newVertices.put(target, new Node(target, v));
                    }
                }
            }

            // Re-init targets
            this.targets = newVertices;
            this.size += newVertices.size();

            return paths;
        }

        private boolean superNode(String vertex, Direction direction) {
            if (this.skipDegree <= 0L) {
                return false;
            }
            Iterator<Edge> edges = edgesOfVertex(vertex, direction, this.label, this.skipDegree);
            return Iterators.count(edges) >= this.skipDegree;
        }
    }
}
