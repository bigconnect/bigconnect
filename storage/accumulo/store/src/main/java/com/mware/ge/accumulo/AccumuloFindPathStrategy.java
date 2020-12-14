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
import com.mware.ge.accumulo.util.RangeUtils;
import com.mware.ge.id.NameSubstitutionStrategy;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.IterableUtils;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;

import java.util.*;
import java.util.stream.Collectors;

import static com.mware.ge.util.StreamUtils.stream;

public class AccumuloFindPathStrategy {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(AccumuloFindPathStrategy.class);
    private final AccumuloGraph graph;
    private final FindPathOptions options;
    private final ProgressCallback progressCallback;
    private final Authorizations authorizations;
    private final Set<String> includeLabels;
    private final Set<String> excludeLabels;

    public AccumuloFindPathStrategy(
            AccumuloGraph graph,
            FindPathOptions options,
            ProgressCallback progressCallback,
            Authorizations authorizations
    ) {
        this.graph = graph;
        this.options = options;
        this.progressCallback = progressCallback;
        this.authorizations = authorizations;
        this.includeLabels = labelsToSet(graph.getNameSubstitutionStrategy(), options.getLabels());
        this.excludeLabels = labelsToSet(graph.getNameSubstitutionStrategy(), options.getExcludedLabels());
    }

    private static Set<String> labelsToSet(NameSubstitutionStrategy nameSubstitutionStrategy, String[] labels) {
        if (labels == null) {
            return null;
        }
        Set<String> results = new HashSet<>();
        for (int i = 0; i < labels.length; i++) {
            results.add(labels[i]);
        }
        return results;
    }

    public Iterable<Path> findPaths() {
        progressCallback.progress(0, ProgressCallback.Step.FINDING_PATH);

        List<Path> foundPaths = new ArrayList<>();
        if (options.getMaxHops() < 1) {
            throw new IllegalArgumentException("maxHops cannot be less than 1");
        } else if (options.getMaxHops() == 1) {
            Set<String> sourceConnectedVertexIds = getConnectedVertexIds(options.getSourceVertexId());
            if (sourceConnectedVertexIds.contains(options.getDestVertexId())) {
                foundPaths.add(new Path(options.getSourceVertexId(), options.getDestVertexId()));
            }
        } else if (options.getMaxHops() == 2) {
            findPathsSetIntersection(foundPaths);
        } else {
            findPathsBreadthFirst(foundPaths, options.getSourceVertexId(), options.getDestVertexId(), options.getMaxHops());
        }

        progressCallback.progress(1, ProgressCallback.Step.COMPLETE);
        return foundPaths;
    }

    private void findPathsSetIntersection(List<Path> foundPaths) {
        String sourceVertexId = options.getSourceVertexId();
        String destVertexId = options.getDestVertexId();

        Set<String> vertexIds = new HashSet<>();
        vertexIds.add(sourceVertexId);
        vertexIds.add(destVertexId);
        Map<String, Set<String>> connectedVertexIds = getConnectedVertexIds(vertexIds);

        progressCallback.progress(0.1, ProgressCallback.Step.SEARCHING_SOURCE_VERTEX_EDGES);
        Set<String> sourceVertexConnectedVertexIds = connectedVertexIds.get(sourceVertexId);
        if (sourceVertexConnectedVertexIds == null) {
            return;
        }

        progressCallback.progress(0.3, ProgressCallback.Step.SEARCHING_DESTINATION_VERTEX_EDGES);
        Set<String> destVertexConnectedVertexIds = connectedVertexIds.get(destVertexId);
        if (destVertexConnectedVertexIds == null) {
            return;
        }

        if (sourceVertexConnectedVertexIds.contains(destVertexId)) {
            foundPaths.add(new Path(sourceVertexId, destVertexId));
            if (options.isGetAnyPath()) {
                return;
            }
        }

        progressCallback.progress(0.6, ProgressCallback.Step.MERGING_EDGES);
        sourceVertexConnectedVertexIds.retainAll(destVertexConnectedVertexIds);

        progressCallback.progress(0.9, ProgressCallback.Step.ADDING_PATHS);
        foundPaths.addAll(
                sourceVertexConnectedVertexIds.stream()
                        .map(connectedVertexId -> new Path(sourceVertexId, connectedVertexId, destVertexId))
                        .collect(Collectors.toList())
        );
    }

    private void findPathsBreadthFirst(List<Path> foundPaths, String sourceVertexId, String destVertexId, int hops) {
        Map<String, Set<String>> connectedVertexIds = getConnectedVertexIds(sourceVertexId, destVertexId);
        // start at 2 since we already got the source and dest vertex connected vertex ids
        for (int i = 2; i < hops; i++) {
            progressCallback.progress((double) i / (double) hops, ProgressCallback.Step.FINDING_PATH);
            Set<String> vertexIdsToSearch = new HashSet<>();
            for (Map.Entry<String, Set<String>> entry : connectedVertexIds.entrySet()) {
                vertexIdsToSearch.addAll(entry.getValue());
            }
            vertexIdsToSearch.removeAll(connectedVertexIds.keySet());
            Map<String, Set<String>> r = getConnectedVertexIds(vertexIdsToSearch);
            connectedVertexIds.putAll(r);
        }
        progressCallback.progress(0.9, ProgressCallback.Step.ADDING_PATHS);
        Set<String> seenVertices = new HashSet<>();
        Path currentPath = new Path(sourceVertexId);
        findPathsRecursive(connectedVertexIds, foundPaths, sourceVertexId, destVertexId, hops, seenVertices, currentPath, progressCallback);
    }

    private void findPathsRecursive(
            Map<String, Set<String>> connectedVertexIds,
            List<Path> foundPaths,
            final String sourceVertexId,
            String destVertexId,
            int hops,
            Set<String> seenVertices,
            Path currentPath,
            @SuppressWarnings("UnusedParameters") ProgressCallback progressCallback
    ) {
        if (options.isGetAnyPath() && foundPaths.size() == 1) {
            return;
        }
        seenVertices.add(sourceVertexId);
        if (sourceVertexId.equals(destVertexId)) {
            foundPaths.add(currentPath);
        } else if (hops > 0) {
            Set<String> vertexIds = connectedVertexIds.get(sourceVertexId);
            if (vertexIds != null) {
                for (String childId : vertexIds) {
                    if (!seenVertices.contains(childId)) {
                        findPathsRecursive(connectedVertexIds, foundPaths, childId, destVertexId, hops - 1, seenVertices, new Path(currentPath, childId), progressCallback);
                    }
                }
            }
        }
        seenVertices.remove(sourceVertexId);
    }

    private Set<String> getConnectedVertexIds(String vertexId) {
        Set<String> vertexIds = new HashSet<>();
        vertexIds.add(vertexId);
        Map<String, Set<String>> results = getConnectedVertexIds(vertexIds);
        Set<String> vertexIdResults = results.get(vertexId);
        if (vertexIdResults == null) {
            return new HashSet<>();
        }
        return vertexIdResults;
    }

    private Map<String, Set<String>> getConnectedVertexIds(String vertexId1, String vertexId2) {
        Set<String> vertexIds = new HashSet<>();
        vertexIds.add(vertexId1);
        vertexIds.add(vertexId2);
        return getConnectedVertexIds(vertexIds);
    }

    private Map<String, Set<String>> getConnectedVertexIds(Set<String> vertexIds) {
        Span trace = Trace.start("getConnectedVertexIds");
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("getConnectedVertexIds:\n  %s", IterableUtils.join(vertexIds, "\n  "));
            }

            if (vertexIds.size() == 0) {
                return new HashMap<>();
            }

            List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
            for (String vertexId : vertexIds) {
                ranges.add(RangeUtils.createRangeFromString(vertexId));
            }

            FetchHints fetchHints = new FetchHintsBuilder(FetchHints.EDGE_REFS)
                    .setIncludeEdgeIds(false)
                    .build();

            ScannerBase scanner = graph.createElementScanner(
                    fetchHints,
                    ElementType.VERTEX,
                    1,
                    null,
                    null,
                    ranges,
                    true,
                    authorizations
            );


            final long timerStartTime = System.currentTimeMillis();
            try {
                Map<String, Set<String>> results = new HashMap<>();
                for (Map.Entry<Key, Value> row : scanner) {
                    graph.logLargeRow(row.getKey(), row.getValue());
                    Vertex vertex = AccumuloGraph.createVertexFromIteratorValue(graph, row.getKey(), row.getValue(), fetchHints, authorizations);
                    Iterable<String> otherVertexIds = stream(vertex.getEdgeInfos(Direction.BOTH, authorizations))
                            .filter(edgeInfo -> {
                                if (excludeLabels != null && excludeLabels.contains(edgeInfo.getLabel())) {
                                    return false;
                                }
                                return includeLabels == null || includeLabels.contains(edgeInfo.getLabel());

                            })
                            .map(EdgeInfo::getVertexId)
                            .collect(Collectors.toSet());
                    Map<String, Boolean> verticesExist = graph.doVerticesExist(otherVertexIds, authorizations);
                    Set<String> rowVertexIds = stream(verticesExist.keySet())
                            .filter(key -> verticesExist.getOrDefault(key, false))
                            .collect(Collectors.toSet());
                    results.put(row.getKey().getRow().toString(), rowVertexIds);
                }
                return results;
            } finally {
                scanner.close();
                AccumuloGraph.GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } finally {
            trace.stop();
        }
    }
}
