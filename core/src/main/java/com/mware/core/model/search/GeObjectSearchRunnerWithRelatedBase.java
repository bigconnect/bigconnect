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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.mware.core.config.Configuration;
import com.mware.core.model.schema.Relationship;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.query.EmptyResultsGraphQuery;
import com.mware.ge.query.Query;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class GeObjectSearchRunnerWithRelatedBase extends GeObjectSearchRunnerBase {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(GeObjectSearchRunnerWithRelatedBase.class);

    protected GeObjectSearchRunnerWithRelatedBase(
            SchemaRepository schemaRepository,
            Graph graph,
            Configuration configuration
    ) {
        super(schemaRepository, graph, configuration);
    }

    @Override
    protected Query getQuery(SearchOptions searchOptions, Authorizations authorizations) {
        JSONArray filterJson = getFilterJson(searchOptions, searchOptions.getWorkspaceId());

        String queryStringParam = searchOptions.getOptionalParameter("q", String.class);
        String[] relatedToVertexIdsParam = searchOptions.getOptionalParameter("relatedToVertexIds[]", String[].class);
        String elementExtendedDataParam = searchOptions.getOptionalParameter("elementExtendedData", String.class);

        List<String> relatedToVertexIds = ImmutableList.of();
        ElementExtendedData elementExtendedData = null;
        if (relatedToVertexIdsParam == null && elementExtendedDataParam == null) {
            queryStringParam = searchOptions.getRequiredParameter("q", String.class);
        } else if (elementExtendedDataParam != null) {
            elementExtendedData = ElementExtendedData.fromJsonString(elementExtendedDataParam);
        } else {
            relatedToVertexIds = ImmutableList.copyOf(relatedToVertexIdsParam);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "search %s (relatedToVertexIds: %s, elementExtendedData: %s)\n%s",
                    queryStringParam,
                    Joiner.on(",").join(relatedToVertexIds),
                    elementExtendedData,
                    filterJson.toString(2)
            );
        }

        Query graphQuery = getGraph().query(queryStringParam, authorizations);

        if (elementExtendedData != null) {
            graphQuery = graphQuery.hasExtendedData(elementExtendedData.elementType, elementExtendedData.elementId, elementExtendedData.tableName);
        } else if (!relatedToVertexIds.isEmpty()) {
            String[] edgeLabels = getEdgeLabels(searchOptions);
            Set<String> allRelatedIds = relatedToVertexIds.stream()
                    .map(vertexId -> {
                        Vertex vertex = getGraph().getVertex(vertexId, FetchHints.EDGE_REFS, authorizations);
                        checkNotNull(vertex, "Could not find vertex: " + vertexId);
                        return vertex;
                    })
                    .flatMap(vertex -> {
                        Iterable<EdgeInfo> edgeInfos = vertex.getEdgeInfos(Direction.BOTH, edgeLabels, authorizations);
                        return StreamSupport.stream(edgeInfos.spliterator(), false).map(EdgeInfo::getVertexId);
                    })
                    .collect(Collectors.toSet());
            if (allRelatedIds.isEmpty()) {
                graphQuery = new EmptyResultsGraphQuery();
            } else {
                graphQuery = graphQuery.hasId(allRelatedIds);
            }
        }

        return graphQuery;
    }

    private String[] getEdgeLabels(SearchOptions searchOptions) {
        Collection<SchemaRepository.ElementTypeFilter> edgeLabelFilters = getEdgeLabelFilters(searchOptions);
        if (edgeLabelFilters == null || edgeLabelFilters.isEmpty()) {
            return null;
        }

        return edgeLabelFilters.stream()
                .flatMap(filter -> {
                    if (filter.includeChildNodes) {
                        return getSchemaRepository().getRelationshipAndAllChildrenByName(filter.iri, searchOptions.getWorkspaceId())
                                .stream().map(Relationship::getName);
                    }
                    return Stream.of(filter.iri);
                })
                .toArray(String[]::new);
    }

    private static class ElementExtendedData {
        public final ElementType elementType;
        public final String elementId;
        public final String tableName;

        private ElementExtendedData(
                ElementType elementType,
                String elementId,
                String tableName
        ) {
            this.elementType = elementType;
            this.elementId = elementId;
            this.tableName = tableName;
        }

        public static ElementExtendedData fromJsonString(String str) {
            JSONObject json = new JSONObject(str);
            ElementType elementType = null;
            String elementTypeString = json.optString("elementType");
            if (!Strings.isNullOrEmpty(elementTypeString)) {
                elementType = ElementType.valueOf(elementTypeString.toUpperCase());
            }
            String elementId = json.optString("elementId", null);
            String tableName = json.optString("tableName", null);
            return new ElementExtendedData(elementType, elementId, tableName);
        }

        @Override
        public String toString() {
            return "ElementExtendedData{" +
                    "elementType='" + elementType + '\'' +
                    ", elementId='" + elementId + '\'' +
                    ", tableName='" + tableName + '\'' +
                    '}';
        }
    }
}
