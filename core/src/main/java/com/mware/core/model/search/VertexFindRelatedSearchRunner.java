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
 *
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
package com.mware.core.model.search;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.model.schema.Concept;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.user.User;
import com.mware.core.util.ClientApiConverter;
import com.mware.ge.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Singleton
public class VertexFindRelatedSearchRunner extends SearchRunner {
    public static final String URI = "/vertex/find-related";
    private final SchemaRepository schemaRepository;
    private final Graph graph;

    @Inject
    public VertexFindRelatedSearchRunner(
            Graph graph,
            SchemaRepository schemaRepository
    ) {
        this.graph = graph;
        this.schemaRepository = schemaRepository;
    }

    @Override
    public String getUri() {
        return URI;
    }

    @Override
    public VertexFindRelatedSearchResults run(SearchOptions searchOptions, User user, Authorizations authorizations) {
        String[] graphVertexIds = searchOptions.getRequiredParameter("graphVertexIds[]", String[].class);
        String limitParentConceptId = searchOptions.getOptionalParameter("limitParentConceptId", String.class);
        String limitEdgeLabel = searchOptions.getOptionalParameter("limitEdgeLabel", String.class);
        long maxVerticesToReturn = searchOptions.getOptionalParameter("maxVerticesToReturn", 1000L);

        Set<String> limitConceptIds = new HashSet<>();

        if (limitParentConceptId != null) {
            Set<Concept> limitConcepts = schemaRepository.getConceptAndAllChildrenByName(limitParentConceptId, searchOptions.getWorkspaceId());
            if (limitConcepts == null) {
                throw new RuntimeException("Bad 'limitParentConceptId', no concept found for id: " + limitParentConceptId);
            }
            for (Concept con : limitConcepts) {
                limitConceptIds.add(con.getName());
            }
        }

        return getSearchResults(
                graphVertexIds,
                limitEdgeLabel,
                limitConceptIds,
                maxVerticesToReturn,
                authorizations
        );
    }

    private VertexFindRelatedSearchResults getSearchResults(
            String[] graphVertexIds,
            String limitEdgeLabel,
            Set<String> limitConceptIds,
            long maxVerticesToReturn,
            Authorizations authorizations
    ) {
        Set<String> visitedIds = new HashSet<>();
        long count = visitedIds.size();
        Iterable<Vertex> vertices = graph.getVertices(Lists.newArrayList(graphVertexIds), FetchHints.EDGE_REFS, authorizations);
        List<Vertex> elements = new ArrayList<>();
        for (Vertex v : vertices) {
            Iterable<Vertex> relatedVertices = v.getVertices(Direction.BOTH, limitEdgeLabel, ClientApiConverter.SEARCH_FETCH_HINTS, authorizations);
            for (Vertex vertex : relatedVertices) {
                if (!visitedIds.add(vertex.getId())) {
                    continue;
                }
                if (limitConceptIds.size() == 0 || !isLimited(vertex, limitConceptIds)) {
                    if (count < maxVerticesToReturn) {
                        elements.add(vertex);
                    }
                    count++;
                }
            }
        }
        return new VertexFindRelatedSearchResults(elements, count);
    }

    private boolean isLimited(Vertex vertex, Set<String> limitConceptIds) {
        return !limitConceptIds.contains(vertex.getConceptType());
    }
}

