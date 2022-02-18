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
package com.mware.core.ingest.structured.mapping;

import com.mware.core.ingest.database.model.ClientApiDataSource;
import com.mware.core.ingest.structured.model.ClientApiMappingErrors;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.security.VisibilityTranslator;
import com.mware.ge.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ParseMapping {
    public List<VertexMapping> vertexMappings = new ArrayList<>();
    public List<EdgeMapping> edgeMappings = new ArrayList<>();

    public ParseMapping(SchemaRepository schemaRepository, VisibilityTranslator visibilityTranslator, String workspaceId, String jsonMapping) {
        this(schemaRepository, visibilityTranslator, workspaceId, new JSONObject(jsonMapping));
    }

    public ParseMapping(SchemaRepository schemaRepository, VisibilityTranslator visibilityTranslator, String workspaceId, JSONObject jsonMapping) {
        JSONArray jsonVertexMappings = jsonMapping.getJSONArray("vertices");
        for (int i = 0; i < jsonVertexMappings.length(); i++) {
            vertexMappings.add(new VertexMapping(schemaRepository, visibilityTranslator, workspaceId, jsonVertexMappings.getJSONObject(i)));
        }

        JSONArray jsonEdgeMappings = jsonMapping.getJSONArray("edges");
        for (int i = 0; i < jsonEdgeMappings.length(); i++) {
            edgeMappings.add(new EdgeMapping(visibilityTranslator, workspaceId, jsonEdgeMappings.getJSONObject(i)));
        }
    }

    public ParseMapping(List<VertexMapping> vertexMappings, List<EdgeMapping> edgeMappings) {
        this.vertexMappings = vertexMappings;
        this.edgeMappings = edgeMappings;
    }

    public static ParseMapping fromDataSourceImport(SchemaRepository schemaRepository, VisibilityTranslator visibilityTranslator, ClientApiDataSource data) {
        Map<String, List<ClientApiDataSource.EntityMapping>> groupedByEntityId = data.getEntityMappings().stream()
                .filter(m -> {
                    return !StringUtils.isEmpty(m.getColEntityId()) && !StringUtils.isEmpty(m.getColConcept()) && !StringUtils.isEmpty(m.getColProperty());
                })
                .collect(Collectors.groupingBy(ClientApiDataSource.EntityMapping::getColEntityId));

        List<VertexMapping> vm = new ArrayList<>();
        groupedByEntityId.forEach((k, l) -> {
            vm.add(VertexMapping.fromDataSourceImport(schemaRepository, visibilityTranslator, data.getWorkspaceId(), k, l));
        });

        List<EdgeMapping> em = new ArrayList<>();
        for(ClientApiDataSource.RelMapping relMapping : data.getRelMappings()) {
            em.add(EdgeMapping.fromDataSourceImport(visibilityTranslator, relMapping, vm, data.getWorkspaceId()));
        }

        return new ParseMapping(vm, em);
    }

    public ClientApiMappingErrors validate(Authorizations authorizations) {
        ClientApiMappingErrors errors = new ClientApiMappingErrors();

        for (VertexMapping vertexMapping : vertexMappings) {
            ClientApiMappingErrors vertexMappingErrors = vertexMapping.validate(authorizations);
            errors.mappingErrors.addAll(vertexMappingErrors.mappingErrors);
        }

        for (EdgeMapping edgeMapping : edgeMappings) {
            ClientApiMappingErrors edgeMappingErrors = edgeMapping.validate(authorizations);
            errors.mappingErrors.addAll(edgeMappingErrors.mappingErrors);
        }

        return errors;
    }
}
