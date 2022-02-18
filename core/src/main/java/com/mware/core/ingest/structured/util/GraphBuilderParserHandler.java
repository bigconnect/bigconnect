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
package com.mware.core.ingest.structured.util;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.structured.StructuredIngestSchema;
import com.mware.core.ingest.structured.mapping.*;
import com.mware.core.ingest.structured.model.ClientApiAnalysis;
import com.mware.core.ingest.structured.model.ClientApiIngestPreview;
import com.mware.core.ingest.structured.model.ClientApiParseErrors;
import com.mware.core.model.clientapi.dto.Privilege;
import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.properties.types.SingleValueBcProperty;
import com.mware.core.model.schema.Concept;
import com.mware.core.model.schema.SchemaFactory;
import com.mware.core.model.schema.SchemaProperty;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.user.PrivilegeRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workspace.Workspace;
import com.mware.core.model.workspace.WorkspaceHelper;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.SandboxStatusUtil;
import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.type.GeoPoint;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.StringValue;
import com.mware.ge.values.storable.Value;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.mware.core.model.properties.BcSchema.VISIBILITY_JSON_METADATA;
import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;

public class GraphBuilderParserHandler extends BaseStructuredFileParserHandler {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(GraphBuilderParserHandler.class);
    public static final Long MAX_DRY_RUN_ROWS = 50000L;
    private static final String MULTI_KEY = "SFIMPORT";
    private static final String SKIPPED_VERTEX_ID = "SKIPPED_VERTEX";

    private final Graph graph;
    private final User user;
    private final VisibilityTranslator visibilityTranslator;
    private final WorkspaceHelper workspaceHelper;
    private final SchemaRepository schemaRepository;
    private final Workspace workspace;
    private final Vertex structuredFileVertex;
    private final PropertyMetadata propertyMetadata;
    private final Visibility visibility;
    private final ParseMapping parseMapping;
    private final ProgressReporter progressReporter;
    private final Authorizations bcUserAuths;

    private VisibilityJson visibilityJson;
    private boolean publish;
    private int sheetNumber = -1;
    public int maxParseErrors = 10;
    public boolean dryRun = true;
    public ClientApiParseErrors parseErrors = new ClientApiParseErrors();
    public ClientApiIngestPreview clientApiIngestPreview;
    public List<String> createdVertexIds;
    public List<String> createdEdgeIds;
    public List<ClientApiAnalysis.Column> columns = new ArrayList<>();

    public GraphBuilderParserHandler(
            Graph graph,
            User user,
            VisibilityTranslator visibilityTranslator,
            PrivilegeRepository privilegeRepository,
            Authorizations authorizations,
            WorkspaceRepository workspaceRepository,
            WorkspaceHelper workspaceHelper,
            String workspaceId,
            boolean publish,
            Vertex structuredFileVertex,
            ParseMapping parseMapping,
            ProgressReporter progressReporter,
            SchemaRepository schemaRepository
    ) {
        super(authorizations);
        this.graph = graph;
        this.user = user;
        this.visibilityTranslator = visibilityTranslator;
        this.workspaceHelper = workspaceHelper;
        this.schemaRepository = schemaRepository;
        this.workspace = workspaceRepository.findById(workspaceId, user);
        this.structuredFileVertex = structuredFileVertex;
        this.parseMapping = parseMapping;
        this.progressReporter = progressReporter;
        this.publish = publish;

        bcUserAuths = graph.createAuthorizations(BcVisibility.SUPER_USER_VISIBILITY_STRING);

        if (workspace == null) {
            throw new BcException("Unable to find vertex with ID: " + workspaceId);
        }

        clientApiIngestPreview = new ClientApiIngestPreview();
        createdVertexIds = Lists.newArrayList();
        createdEdgeIds = Lists.newArrayList();
        visibilityJson = new VisibilityJson(visibilityTranslator.getDefaultVisibility().getVisibilityString());

        if (this.publish) {
            if (!privilegeRepository.hasPrivilege(user, Privilege.PUBLISH)) {
                this.publish = false;
            }
        }

        if (!this.publish) {
            visibilityJson.addWorkspace(workspaceId);
        }

        visibility = visibilityTranslator.toVisibility(visibilityJson).getVisibility();

        propertyMetadata = new PropertyMetadata(
                ZonedDateTime.now(),
                user,
                visibilityJson,
                visibilityTranslator.getDefaultVisibility()
        );
    }

    public void reset() {
        parseErrors.errors.clear();
        sheetNumber = -1;
        clientApiIngestPreview = new ClientApiIngestPreview();
        createdVertexIds.clear();
        createdEdgeIds.clear();
    }

    public boolean hasErrors() {
        return !parseErrors.errors.isEmpty();
    }

    @Override
    public void newSheet(String name) {
        // Right now, it will ingest all of the columns in the first sheet since that's
        // what the interface shows. In the future, if they can select a different sheet
        // this code will need to be updated.
        sheetNumber++;
    }

    @Override
    public void addColumn(String name, ColumnMappingType type) {
        ClientApiAnalysis.Column column = new ClientApiAnalysis.Column();
        column.name = name;
        column.type = type;
        columns.add(column);
    }

    @Override
    public boolean addRow(Map<String, Object> row, long rowNum, List<ElementMutation<? extends Element>> batchElementBuilders) {
        Long rowCount = rowNum + 1;
        if (dryRun && rowCount > MAX_DRY_RUN_ROWS) {
            clientApiIngestPreview.didTruncate = true;
            return false;
        }
        clientApiIngestPreview.processedRows = rowCount;

        Visibility defaultVisibility = visibilityTranslator.getDefaultVisibility();

        // Since we only handle the first sheet currently, bail if this isn't it.
        if (sheetNumber != 0) {
            return false;
        }

        try {
            List<String> newVertexIds = new ArrayList<>();
            List<VertexBuilder> vertexBuilders = new ArrayList<>();
            long vertexNum = 0;

            for (VertexMapping vertexMapping : parseMapping.vertexMappings) {
                VertexBuilder vertexBuilder = createVertex(vertexMapping, row, rowNum, vertexNum);
                if (vertexBuilder != null) {
                    boolean alreadyCreated = createdVertexIds.contains(vertexBuilder.getId());
                    vertexBuilders.add(vertexBuilder);
                    newVertexIds.add(vertexBuilder.getId());
                    createdVertexIds.add(vertexBuilder.getId());
                    if (!alreadyCreated) {
                        incrementConcept(vertexMapping, !graph.doesVertexExist(vertexBuilder.getId(), authorizations));
                    }
                } else {
                    newVertexIds.add(SKIPPED_VERTEX_ID);
                }
                vertexNum++;
            }

            List<EdgeBuilderByVertexId> edgeBuilders = new ArrayList<>();
            for (EdgeMapping edgeMapping : parseMapping.edgeMappings) {
                EdgeBuilderByVertexId edgeBuilder = createEdge(edgeMapping, newVertexIds);
                if (edgeBuilder != null) {
                    boolean alreadyCreated = createdEdgeIds.contains(edgeBuilder.getId());
                    createdEdgeIds.add(edgeBuilder.getId());
                    edgeBuilders.add(edgeBuilder);
                    if (!alreadyCreated) {
                        incrementEdges(edgeMapping, !graph.doesEdgeExist(edgeBuilder.getId(), authorizations));
                    }
                }
            }
            if (!dryRun) {
                batchElementBuilders.addAll(vertexBuilders);
                HashFunction hash = Hashing.sha256();
                for (VertexBuilder vertexBuilder : vertexBuilders) {
                    EdgeBuilderByVertexId hasSourceEdgeBuilder = graph.prepareEdge(
                            hash.newHasher()
                                    .putUnencodedChars(vertexBuilder.getId())
                                    .putUnencodedChars(structuredFileVertex.getId())
                                    .hash()
                                    .toString(),
                            vertexBuilder.getId(),
                            structuredFileVertex.getId(),
                            StructuredIngestSchema.ELEMENT_HAS_SOURCE_NAME,
                            visibility
                    );
                    BcSchema.VISIBILITY_JSON.setProperty(hasSourceEdgeBuilder, visibilityJson, defaultVisibility);
                    BcSchema.MODIFIED_BY.setProperty(hasSourceEdgeBuilder, user.getUserId(), defaultVisibility);
                    BcSchema.MODIFIED_DATE.setProperty(hasSourceEdgeBuilder, ZonedDateTime.now(), defaultVisibility);
                    batchElementBuilders.add(hasSourceEdgeBuilder);
                }

                batchElementBuilders.addAll(edgeBuilders);
            }
        } catch (SkipRowException sre) {
            // Skip the row and keep going
        }

        if (progressReporter != null) {
            progressReporter.finishedRow(rowNum, getTotalRows());
        }

        return !dryRun || maxParseErrors <= 0 || parseErrors.errors.size() < maxParseErrors;
    }

    private void incrementConcept(VertexMapping vertexMapping, boolean isNew) {
        for (PropertyMapping mapping : vertexMapping.propertyMappings) {
            if (VertexMapping.CONCEPT_TYPE.equals(mapping.name)) {
                clientApiIngestPreview.incrementVertices(mapping.value, isNew);
            }
        }
    }

    private void incrementEdges(EdgeMapping mapping, boolean isNew) {
        clientApiIngestPreview.incrementEdges(mapping.label, isNew);
    }

    public boolean cleanUpExistingImport() {
        Iterable<Vertex> vertices = structuredFileVertex.getVertices(
                Direction.IN,
                StructuredIngestSchema.ELEMENT_HAS_SOURCE_NAME,
                authorizations
        );

        for (Vertex vertex : vertices) {
            SandboxStatus sandboxStatus = SandboxStatusUtil.getSandboxStatus(vertex, workspace.getWorkspaceId());
            if (sandboxStatus != SandboxStatus.PUBLIC) {
                workspaceHelper.deleteVertex(
                        vertex,
                        workspace.getWorkspaceId(),
                        false,
                        Priority.HIGH,
                        authorizations,
                        user
                );
            }
        }

        return true;
    }

    private EdgeBuilderByVertexId createEdge(EdgeMapping edgeMapping, List<String> newVertexIds) {
        String inVertexId = newVertexIds.get(edgeMapping.inVertexIndex);
        String outVertexId = newVertexIds.get(edgeMapping.outVertexIndex);

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

        EdgeBuilderByVertexId m = graph.prepareEdge(outVertexId, inVertexId, edgeMapping.label, edgeVisibility);
        BcSchema.VISIBILITY_JSON.setProperty(m, edgeVisibilityJson, edgeVisibility);
        BcSchema.MODIFIED_DATE.setProperty(m, propertyMetadata.getModifiedDate(), edgeVisibility);
        BcSchema.MODIFIED_BY.setProperty(m, propertyMetadata.getModifiedBy().getUserId(), edgeVisibility);
        return m;
    }

    private void addAutoPropertyMappings(VertexMapping mapping, Map<String, Object> row) {
        Set<String> alreadyMappedKeys = mapping.propertyMappings
                .stream().map(p -> p.key)
                .collect(Collectors.toSet());

        Set<String> unmappedKeys = Sets.difference(row.keySet(), alreadyMappedKeys);
        for (String key : unmappedKeys) {
            String colName = this.columns.get(Integer.parseInt(key)).name;

            SchemaProperty existingProp = schemaRepository.getPropertyByName(colName, workspace.getWorkspaceId());
            if (existingProp == null) {
                // find concept type
                Concept concept = null;
                for (PropertyMapping propertyMapping : mapping.propertyMappings) {
                    if (VertexMapping.CONCEPT_TYPE.equals(propertyMapping.name)) {
                        concept = schemaRepository.getConceptByName(propertyMapping.value, workspace.getWorkspaceId());
                        break;
                    }
                }

                if (concept != null) {
                    SchemaFactory factory = new SchemaFactory(schemaRepository)
                            .forNamespace(workspace.getWorkspaceId());

                    existingProp = factory.newConceptProperty()
                            .concepts(concept)
                            .name(colName)
                            .displayName(StringUtils.capitalize(colName))
                            .type(tryDeterminePropertyType(row.get(key)))
                            .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                            .userVisible(true)
                            .searchable(true)
                            .systemProperty(false)
                            .save();

                    if (publish) {
                        schemaRepository.publishProperty(existingProp, user, workspace.getWorkspaceId());
                    }
                    schemaRepository.clearCache();
                } else {
                    throw new BcException("Could not find concept for VertexMapping: "+mapping);
                }
            }

            PropertyMapping propertyMapping;
            PropertyType detectedType = existingProp.getDataType();
            if (PropertyType.DATE.equals(detectedType))
                propertyMapping = new DatePropertyMapping();
            else if (PropertyType.INTEGER.equals(detectedType) || PropertyType.DOUBLE.equals(detectedType))
                propertyMapping = new NumericPropertyMapping(existingProp);
            else if (PropertyType.BOOLEAN.equals(detectedType))
                propertyMapping = new BooleanPropertyMapping();
            else if (PropertyType.GEO_LOCATION.equals(detectedType)) {
                propertyMapping = new GeoPointPropertyMapping();
                ((GeoPointPropertyMapping)propertyMapping).format = GeoPointPropertyMapping.Format.DEGREES_MINUTES_SECONDS;
            } else {
                propertyMapping = new PropertyMapping();
            }

            propertyMapping.name = existingProp.getName();
            propertyMapping.identifier = false;
            propertyMapping.key = key;
            mapping.propertyMappings.add(propertyMapping);
        }
    }

    private PropertyType tryDeterminePropertyType(Object value) {
        if (value == null || StringUtils.isEmpty(value.toString())) {
            return PropertyType.STRING;
        }

        String strValue = value.toString();
        if (NumberUtils.isParsable(value.toString())) {
            try {
                Number number = NumberUtils.createNumber(strValue);
                if (number instanceof Double || number instanceof Float)
                    return PropertyType.DOUBLE;
                else if (number instanceof Integer || number instanceof Short || number instanceof Long || number instanceof Byte)
                    return PropertyType.INTEGER;
            } catch (NumberFormatException ex) {
                // could not parse number, continue
            }
        }

        Boolean bool = BooleanUtils.toBooleanObject(strValue);
        if (bool != null) {
            return PropertyType.BOOLEAN;
        }

        try {
            GeoPoint.parse(strValue);
            return PropertyType.GEO_LOCATION;
        } catch (GeException ex) {
        }

        return PropertyType.STRING;
    }

    private VertexBuilder createVertex(VertexMapping vertexMapping, Map<String, Object> row, long rowNum, long vertexNum) {
        VisibilityJson vertexVisibilityJson = visibilityJson;
        Visibility vertexVisibility = visibility;
        if (vertexMapping.visibilityJson != null) {
            vertexVisibilityJson = vertexMapping.visibilityJson;
            vertexVisibility = vertexMapping.visibility;
        }

        String vertexId = generateVertexId(vertexMapping, row, rowNum, vertexNum);

        VertexBuilder m = vertexId == null ? graph.prepareVertex(vertexVisibility, CONCEPT_TYPE_THING) : graph.prepareVertex(vertexId, vertexVisibility, CONCEPT_TYPE_THING);
        setPropertyValue(BcSchema.VISIBILITY_JSON, m, vertexVisibilityJson, vertexVisibility);

        if (vertexMapping.automap) {
            addAutoPropertyMappings(vertexMapping, row);
        }

        for (PropertyMapping propertyMapping : vertexMapping.propertyMappings) {

            if (VertexMapping.CONCEPT_TYPE.equals(propertyMapping.name)) {
                m.alterConceptType(propertyMapping.value);
                setPropertyValue(BcSchema.MODIFIED_DATE, m, propertyMetadata.getModifiedDate(), vertexVisibility);
                setPropertyValue(BcSchema.MODIFIED_BY, m, propertyMetadata.getModifiedBy().getUserId(), vertexVisibility);
            } else {
                Metadata metadata = propertyMetadata.createMetadata();
                try {
                    RawObjectSchema.SOURCE_FILE_OFFSET_METADATA.setMetadata(metadata, Long.valueOf(rowNum), vertexVisibility);

                    if (BcSchema.TEXT.getPropertyName().equals(propertyMapping.name)) {
                        // special handling for text
                        BcSchema.MIME_TYPE_METADATA.setMetadata(metadata, "text/plain", visibilityTranslator.getDefaultVisibility());
                        BcSchema.TEXT_DESCRIPTION_METADATA.setMetadata(metadata, "Text", visibilityTranslator.getDefaultVisibility());
                        setPropertyValue(m, row, propertyMapping, vertexVisibility, metadata, "", true);

                    } else {
                        setPropertyValue(m, row, propertyMapping, vertexVisibility, metadata, MULTI_KEY, false);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error parsing property.", e);

                    ClientApiParseErrors.ParseError pe = new ClientApiParseErrors.ParseError();
                    pe.rawPropertyValue = propertyMapping.extractRawValue(row);
                    pe.propertyMapping = propertyMapping;
                    pe.message = e.getMessage();
                    pe.sheetIndex = sheetNumber;
                    pe.rowIndex = rowNum;

                    if (!dryRun) {
                        if (propertyMapping.errorHandlingStrategy == PropertyMapping.ErrorHandlingStrategy.SKIP_ROW) {
                            throw new SkipRowException("Error parsing property.", e);
                        } else if (propertyMapping.errorHandlingStrategy == PropertyMapping.ErrorHandlingStrategy.SKIP_VERTEX) {
                            return null;
                        } else if (propertyMapping.errorHandlingStrategy == PropertyMapping.ErrorHandlingStrategy.SET_CELL_ERROR_PROPERTY) {
                            String multiKey = sheetNumber + "_" + rowNum;
                            StructuredIngestSchema.ERROR_MESSAGE_PROPERTY.addPropertyValue(
                                    m,
                                    multiKey,
                                    pe.message,
                                    metadata,
                                    vertexVisibility
                            );
                            StructuredIngestSchema.RAW_CELL_VALUE_PROPERTY.addPropertyValue(
                                    m,
                                    multiKey,
                                    pe.rawPropertyValue.toString(),
                                    metadata,
                                    vertexVisibility
                            );
                            StructuredIngestSchema.TARGET_PROPERTY.addPropertyValue(
                                    m,
                                    multiKey,
                                    pe.propertyMapping.name,
                                    metadata,
                                    vertexVisibility
                            );
                            StructuredIngestSchema.SHEET_PROPERTY.addPropertyValue(
                                    m,
                                    multiKey,
                                    String.valueOf(sheetNumber),
                                    metadata,
                                    vertexVisibility
                            );
                            StructuredIngestSchema.ROW_PROPERTY.addPropertyValue(
                                    m,
                                    multiKey,
                                    String.valueOf(rowNum),
                                    metadata,
                                    vertexVisibility
                            );
                        } else if (propertyMapping.errorHandlingStrategy != PropertyMapping.ErrorHandlingStrategy.SKIP_CELL) {
                            throw new BcException("Unhandled mapping error. Please provide a strategy.");
                        }
                    } else if (propertyMapping.errorHandlingStrategy == null) {
                        parseErrors.errors.add(pe);
                    }
                }
            }
        }

        return m;
    }

    private String generateVertexId(VertexMapping vertexMapping, Map<String, Object> row, long rowNum, long vertexNum) {
        List<String> identifierParts = new ArrayList<>();

        // Find any mappings that designate identifier columns
        for (String key : row.keySet()) {
            for (PropertyMapping mapping : vertexMapping.propertyMappings) {
                if (mapping.key.equals(key) && mapping.identifier) {
                    Object val = row.get(key);
                    if (val != null && !val.toString().isEmpty()) {
                        identifierParts.add(key);
                    }
                }

            }
        }

        HashFunction sha1 = Hashing.sha256();
        Hasher hasher = sha1.newHasher();

        if (identifierParts.isEmpty()) {
            // By default just allow the same file to ingest without creating new entities
            String structuredFileVertexId = structuredFileVertex != null ? structuredFileVertex.getId() : graph.getIdGenerator().nextId();
            hasher
                    .putUnencodedChars(structuredFileVertexId).putUnencodedChars("|")
                    .putLong(rowNum).putUnencodedChars("|")
                    .putLong(vertexNum);
        } else {
            // Hash all the identifier values and the concept. Use delimiter to minimize collisions
            identifierParts
                    .stream()
                    .sorted(String::compareToIgnoreCase)
                    .forEach(s -> {
                        hasher.putString(row.get(s).toString(), Charsets.UTF_8).putUnencodedChars("|");
                    });

            for (PropertyMapping mapping : vertexMapping.propertyMappings) {
                if (VertexMapping.CONCEPT_TYPE.equals(mapping.name)) {
                    hasher.putUnencodedChars(mapping.value);
                }
            }
        }


        HashCode hash = hasher.hash();
        String vertexId = String.valueOf(hash.asLong());

        // We might need to also hash the workspace if this vertex exists in the system but not visible to user.
        if (shouldAddWorkspaceToId(vertexId)) {
            vertexId = sha1.newHasher()
                    .putUnencodedChars(vertexId)
                    .putUnencodedChars(workspace.getWorkspaceId())
                    .hash()
                    .toString();
        }

        return vertexId;
    }

    /**
     * If the user is creating an entity that is unpublished in different sandbox, this user won't be able to access
     * it since prepareVertex with same id won't change the visibility.
     */
    private boolean shouldAddWorkspaceToId(String vertexId) {
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
            VertexBuilder m, Map<String, Object> row,
            PropertyMapping propertyMapping,
            Visibility vertexVisibility,
            Metadata metadata,
            String multiKey,
            boolean spv
    ) throws Exception {
        Visibility propertyVisibility = vertexVisibility;
        if (propertyMapping.visibility != null) {
            propertyVisibility = propertyMapping.visibility;
            VISIBILITY_JSON_METADATA.setMetadata(
                    metadata, propertyMapping.visibilityJson, visibilityTranslator.getDefaultVisibility());
        }

        Value propertyValue = propertyMapping.decodeValue(row);
        if (propertyValue != null) {
            if (spv) {
                StreamingPropertyValue s = new DefaultStreamingPropertyValue(new ByteArrayInputStream(propertyMapping.decodeValue(row).toString().getBytes(StandardCharsets.UTF_8)), StringValue.class);
                m.addPropertyValue(multiKey, propertyMapping.name, s, metadata, propertyVisibility);
            } else {
                m.addPropertyValue(multiKey, propertyMapping.name, propertyValue, metadata, propertyVisibility);
            }
        }
    }
}
