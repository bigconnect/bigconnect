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
package com.mware.core.ingest.database;

import com.mware.core.ingest.database.model.ClientApiDataSource;
import com.mware.core.ingest.structured.mapping.EdgeMapping;
import com.mware.core.ingest.structured.mapping.ParseMapping;
import com.mware.core.ingest.structured.mapping.PropertyMapping;
import com.mware.core.ingest.structured.mapping.VertexMapping;
import com.mware.core.ingest.structured.model.ClientApiParseErrors;
import com.mware.core.ingest.structured.util.ProgressReporter;
import com.mware.core.ingest.structured.util.SkipRowException;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.properties.types.SingleValueBcProperty;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.user.PrivilegeRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mware.core.model.properties.BcSchema.VISIBILITY_JSON_METADATA;

public class DataLoadGraphBuilder {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(DataLoadGraphBuilder.class);
    private static final String SKIPPED_VERTEX_ID = "SKIPPED_VERTEX";
    private static final String MULTI_KEY = "SFIMPORT";

    private final VisibilityTranslator visibilityTranslator;
    private final PrivilegeRepository privilegeRepository;
    private final Authorizations authorizations;
    private final Visibility visibility;
    private final Authorizations bcUserAuths;
    private final Graph graph;
    private final User user;
    private final PropertyMetadata propertyMetadata;
    private final ClientApiDataSource params;
    private final long totalRows;
    private final ProgressReporter progressReporter;
    private final ParseMapping parseMapping;
    private final UserRepository userRepository;

    private VisibilityJson visibilityJson;
    public ClientApiParseErrors parseErrors = new ClientApiParseErrors();
    public int maxParseErrors = 10;

    public DataLoadGraphBuilder(
            Graph graph,
            User user,
            PrivilegeRepository privilegeRepository,
            Authorizations authorizations,
            VisibilityTranslator visibilityTranslator,
            ClientApiDataSource params,
            ParseMapping parseMapping,
            ProgressReporter progressReporter,
            UserRepository userRepository,
            long totalRows
    ) {
        this.graph = graph;
        this.user = user;
        this.visibilityTranslator = visibilityTranslator;
        this.privilegeRepository = privilegeRepository;
        this.authorizations = authorizations;
        this.params = params;
        this.progressReporter = progressReporter;
        this.parseMapping = parseMapping;
        this.totalRows = totalRows;
        this.userRepository = userRepository;

        bcUserAuths = graph.createAuthorizations(BcVisibility.SUPER_USER_VISIBILITY_STRING);
        visibility = visibilityTranslator.getDefaultVisibility();
        visibilityJson = new VisibilityJson("");

        propertyMetadata = new PropertyMetadata(
                ZonedDateTime.now(),
                user,
                visibilityJson,
                visibilityTranslator.getDefaultVisibility()
        );
    }

    public boolean addRow(ResultSet rs, long rowNum, boolean addEntitiesToDictionary, List<ElementMutation<? extends Element>> batchElementBuilders) throws SQLException {
        try {
            Map<String, Object> row = rowFromResultSet(rs);
            Map<String, String> newVertexIds = new HashMap<>();
            List<ElementMutation<? extends Element>> vertexBuilders = new ArrayList<>();
            long vertexNum = 0;
            for (VertexMapping vertexMapping : parseMapping.vertexMappings) {
                VertexBuilder vertexBuilder = createVertex(vertexMapping, row, rowNum, vertexNum);
                if (vertexBuilder != null) {
                    vertexBuilders.add(vertexBuilder);
                    newVertexIds.put(vertexMapping.entityId, vertexBuilder.getId());
                } else {
                    newVertexIds.put(vertexMapping.entityId, SKIPPED_VERTEX_ID);
                }
                vertexNum++;
            }

            List<ElementMutation<? extends Element>> edgeBuilders = new ArrayList<>();
            for (EdgeMapping edgeMapping : parseMapping.edgeMappings) {
                EdgeBuilderByVertexId edgeBuilder = createEdge(edgeMapping, newVertexIds);
                if (edgeBuilder != null) {
                    edgeBuilders.add(edgeBuilder);
                }
            }

            batchElementBuilders.addAll(vertexBuilders);
            batchElementBuilders.addAll(edgeBuilders);

        } catch (SkipRowException sre) {
            sre.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (progressReporter != null) {
            if (rowNum % 50 == 0) {
                progressReporter.finishedRow(rowNum, totalRows);
            }
        }

        return maxParseErrors <= 0 || parseErrors.errors.size() < maxParseErrors;
    }

    private Map<String,Object> rowFromResultSet(ResultSet rs) throws SQLException {
        Map<String,Object> row = new HashMap<>();

        ResultSetMetaData rsm = rs.getMetaData();
        for (int i = 1; i <= rsm.getColumnCount(); i++) {
            row.put(rsm.getColumnName(i), rs.getString(i));
        }

        return row;
    }

    private VertexBuilder createVertex(VertexMapping vertexMapping, Map<String, Object> row, long rowNum, long vertexNum) {
        VisibilityJson vertexVisibilityJson = visibilityJson;
        Visibility vertexVisibility = visibility;
        if (vertexMapping.visibilityJson != null) {
            vertexVisibilityJson = vertexMapping.visibilityJson;
            vertexVisibility = vertexMapping.visibility;
        }

        String vertexId = graph.getIdGenerator().nextId();
        VertexBuilder m = vertexId == null ? graph.prepareVertex(vertexVisibility, SchemaConstants.CONCEPT_TYPE_THING) : graph.prepareVertex(vertexId, vertexVisibility, SchemaConstants.CONCEPT_TYPE_THING);
        setPropertyValue(BcSchema.VISIBILITY_JSON, m, vertexVisibilityJson, vertexVisibility);

        RawObjectSchema.SOURCE.addPropertyValue(m, "", params.getImportConfig().getSource(), propertyMetadata.createMetadata(), vertexVisibility);

        for (PropertyMapping propertyMapping : vertexMapping.propertyMappings) {
            if ("conceptType".equals(propertyMapping.name)) {
                m.alterConceptType(propertyMapping.value);
                setPropertyValue(BcSchema.MODIFIED_DATE, m, propertyMetadata.getModifiedDate(), vertexVisibility);
                setPropertyValue(BcSchema.MODIFIED_BY, m, propertyMetadata.getModifiedBy().getUserId(), vertexVisibility);
            } else if(BcSchema.TEXT.getPropertyName().equals(propertyMapping.name)) {
                TextValue value = (TextValue) propertyMapping.decodeValue(row);
                if (value != null && !StringUtils.isEmpty(value.stringValue())) {
                    Metadata metadata = propertyMetadata.createMetadata();
                    BcSchema.MIME_TYPE_METADATA.setMetadata(metadata, "text/plain", Visibility.EMPTY);
                    BcSchema.TEXT_DESCRIPTION_METADATA.setMetadata(metadata, "Text", Visibility.EMPTY);
                    BcSchema.TEXT.addPropertyValue(m, "", DefaultStreamingPropertyValue.create(value.stringValue()), metadata, Visibility.EMPTY);
                }
            } else {
                Metadata metadata = propertyMetadata.createMetadata();
                try {
                    setPropertyValue(m, row, propertyMapping, vertexVisibility, metadata);
                } catch (Exception e) {
                    LOGGER.error("Error parsing property.", e);

                    ClientApiParseErrors.ParseError pe = new ClientApiParseErrors.ParseError();
                    pe.rawPropertyValue = propertyMapping.extractRawValue(row);
                    pe.propertyMapping = propertyMapping;
                    pe.message = e.getMessage();
                    pe.rowIndex = rowNum;

                    if (propertyMapping.errorHandlingStrategy == PropertyMapping.ErrorHandlingStrategy.SKIP_ROW) {
                        throw new SkipRowException("Error parsing property.", e);
                    } else if (propertyMapping.errorHandlingStrategy == PropertyMapping.ErrorHandlingStrategy.SKIP_VERTEX) {
                        return null;
                    } else if (propertyMapping.errorHandlingStrategy == null) {
                        parseErrors.errors.add(pe);
                    }
                }
            }
        }

        return m;
    }

    private EdgeBuilderByVertexId createEdge(EdgeMapping edgeMapping, Map<String, String> newVertexIds) {
        String inVertexId = newVertexIds.get(edgeMapping.targetEntityId);
        String outVertexId = newVertexIds.get(edgeMapping.sourceEntityId);

        if (inVertexId.equals(SKIPPED_VERTEX_ID) || outVertexId.equals(SKIPPED_VERTEX_ID)) {
            // TODO: handle edge errors properly?
            return null;
        }

        VisibilityJson edgeVisibilityJson = visibilityJson;
        Visibility edgeVisibility = visibility;
        if (edgeMapping.visibilityJson != null) {
            edgeVisibilityJson = edgeMapping.visibilityJson;
            edgeVisibility = edgeMapping.visibility;
        }

        String edgeLabel = edgeMapping.label;
        if (StringUtils.isBlank(edgeLabel)) edgeLabel = "linked";

        EdgeBuilderByVertexId m = graph.prepareEdge(outVertexId, inVertexId, edgeLabel, edgeVisibility);
        BcSchema.VISIBILITY_JSON.setProperty(m, edgeVisibilityJson, edgeVisibility);
        BcSchema.MODIFIED_DATE.setProperty(m, propertyMetadata.getModifiedDate(), edgeVisibility);
        BcSchema.MODIFIED_BY.setProperty(m, propertyMetadata.getModifiedBy().getUserId(), edgeVisibility);
        return m;
    }

    /**
     * If the user is creating an entity that is unpublished in different sandbox, this user won't be able to access
     * it since prepareVertex with same id won't change the visibility.
     */
    private boolean vertexExists(String vertexId) {
        boolean vertexExistsForUser = graph.doesVertexExist(vertexId, authorizations);
        if (!vertexExistsForUser) {
            boolean vertexExistsInSystem = graph.doesVertexExist(vertexId, bcUserAuths);
            if (vertexExistsInSystem) {
                return true;
            }
        }
        return false;
    }

    private void setPropertyValue(SingleValueBcProperty property, VertexBuilder m, Object value, Visibility vertexVisibility) {
        Metadata metadata = propertyMetadata.createMetadata();
        property.setProperty(m, value, metadata, vertexVisibility);
    }

    private void setPropertyValue(
            VertexBuilder m, Map<String, Object> row, PropertyMapping propertyMapping, Visibility vertexVisibility,
            Metadata metadata
    ) throws Exception {
        Visibility propertyVisibility = vertexVisibility;
        if (propertyMapping.visibility != null) {
            propertyVisibility = propertyMapping.visibility;
            VISIBILITY_JSON_METADATA.setMetadata(
                    metadata, propertyMapping.visibilityJson, visibilityTranslator.getDefaultVisibility());
        }

        Value propertyValue = propertyMapping.decodeValue(row);
        if (propertyValue != null) {
            m.addPropertyValue(MULTI_KEY, propertyMapping.name, propertyValue, metadata, propertyVisibility);
        }
    }
}
