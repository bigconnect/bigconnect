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
package com.mware.core.model.workspace;

import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Authorizations;
import com.mware.ge.ElementId;
import com.mware.ge.Graph;
import com.mware.ge.Vertex;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.mware.core.util.StreamUtil.stream;

public class WorkspaceEntity implements Serializable {
    static long serialVersionUID = 1L;
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(WorkspaceEntity.class);
    private final String entityVertexId;
    private transient Vertex vertex;

    public WorkspaceEntity(
            String entityVertexId,
            Vertex vertex
    ) {
        this.entityVertexId = entityVertexId;
        this.vertex = vertex;
    }

    public String getEntityVertexId() {
        return entityVertexId;
    }

    public Vertex getVertex() {
        return vertex;
    }


    public static Iterable<Vertex> toVertices(final Iterable<WorkspaceEntity> workspaceEntities, final Graph graph, final Authorizations authorizations) {
        List<String> vertexIdsToFetch = stream(workspaceEntities)
                .filter(we -> we.getVertex() == null)
                .map(WorkspaceEntity::getEntityVertexId)
                .collect(Collectors.toList());
        Map<String, Vertex> fetchedVerticesMap = stream(graph.getVertices(vertexIdsToFetch, authorizations))
                .distinct()
                .collect(Collectors.toMap(ElementId::getId, v -> v));

        return stream(workspaceEntities)
                .map(workspaceEntity -> {
                    if (workspaceEntity.getVertex() == null) {
                        Vertex vertex = fetchedVerticesMap.get(workspaceEntity.getEntityVertexId());
                        if (vertex == null) {
                            LOGGER.trace("Could not find vertex for WorkspaceEntity: %s", workspaceEntity);
                            return null;
                        }
                    }
                    return workspaceEntity.getVertex();
                })
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "WorkspaceEntity{" +
                "entityVertexId='" + entityVertexId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WorkspaceEntity that = (WorkspaceEntity) o;

        return entityVertexId.equals(that.entityVertexId);

    }

    @Override
    public int hashCode() {
        return entityVertexId.hashCode();
    }
}
