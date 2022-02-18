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
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.security.VisibilityTranslator;
import com.mware.ge.Authorizations;
import com.mware.ge.Visibility;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.util.List;

public class EdgeMapping {
    public static final String PROPERTY_MAPPING_VISIBILITY_KEY = "visibilitySource";
    public static final String PROPERTY_MAPPING_IN_VERTEX_KEY = "inVertex";
    public static final String PROPERTY_MAPPING_OUT_VERTEX_KEY = "outVertex";
    public static final String PROPERTY_MAPPING_LABEL_KEY = "label";

    public int inVertexIndex;
    public int outVertexIndex;

    public String sourceEntityId;
    public String sourceEntityType;
    public String targetEntityId;
    public String targetEntityType;


    public String label;
    public VisibilityJson visibilityJson;
    public Visibility visibility;

    public EdgeMapping() {
    }

    public EdgeMapping(VisibilityTranslator visibilityTranslator, String workspaceId, JSONObject edgeMapping) {
        this.inVertexIndex = edgeMapping.getInt(PROPERTY_MAPPING_IN_VERTEX_KEY);
        this.outVertexIndex = edgeMapping.getInt(PROPERTY_MAPPING_OUT_VERTEX_KEY);
        this.label = edgeMapping.getString(PROPERTY_MAPPING_LABEL_KEY);

        String visibilitySource = edgeMapping.optString(PROPERTY_MAPPING_VISIBILITY_KEY);
        if(!StringUtils.isBlank(visibilitySource)) {
            visibilityJson = new VisibilityJson(visibilitySource);
            visibilityJson.addWorkspace(workspaceId);
            visibility = visibilityTranslator.toVisibility(visibilityJson).getVisibility();
        }
    }

    public ClientApiMappingErrors validate(Authorizations authorizations) {
        ClientApiMappingErrors errors = new ClientApiMappingErrors();

        if(visibility != null && !authorizations.canRead(visibility)) {
            ClientApiMappingErrors.MappingError mappingError = new ClientApiMappingErrors.MappingError();
            mappingError.edgeMapping = this;
            mappingError.attribute = PROPERTY_MAPPING_VISIBILITY_KEY;
            mappingError.message = "Invalid visibility specified.";
            errors.mappingErrors.add(mappingError);
        }

        return errors;
    }

    public static EdgeMapping fromDataSourceImport(VisibilityTranslator visibilityTranslator, ClientApiDataSource.RelMapping relMapping, List<VertexMapping> vm, String workspaceId) {
        EdgeMapping em  = new EdgeMapping();
        em.sourceEntityId = relMapping.getSourceId();
        em.sourceEntityType = relMapping.getSourceType();
        em.targetEntityId = relMapping.getTargetId();
        em.targetEntityType = relMapping.getTargetType();

        if(!StringUtils.isBlank(relMapping.getRelVisibility())) {
            em.visibilityJson = new VisibilityJson(relMapping.getRelVisibility());
            em.visibilityJson.addWorkspace(workspaceId);
            em.visibility = visibilityTranslator.toVisibility(em.visibilityJson).getVisibility();
        }

        em.label = relMapping.getRel();

        return em;
    }
}
