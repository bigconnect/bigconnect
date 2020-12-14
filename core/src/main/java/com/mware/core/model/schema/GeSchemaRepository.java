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
package com.mware.core.model.schema;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.cache.CacheService;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.ClientApiSchema;
import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.graph.GraphRepository;
import com.mware.core.model.graph.GraphUpdateContext;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.model.properties.WorkspaceSchema;
import com.mware.core.model.properties.types.BcPropertyBase;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.JSONUtil;
import com.mware.ge.*;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.query.Compare;
import com.mware.ge.query.Contains;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.util.CloseableUtils;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.StreamUtils;
import com.mware.ge.values.storable.Values;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

@Singleton
public class GeSchemaRepository extends SchemaRepositoryBase {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(GeSchemaRepository.class);
    public static final String ID_PREFIX = "o_";
    public static final String ID_PREFIX_PROPERTY = ID_PREFIX + "p_";
    public static final String ID_PREFIX_RELATIONSHIP = ID_PREFIX + "r_";
    public static final String ID_PREFIX_CONCEPT = ID_PREFIX + "c_";

    private final Graph graph;
    private final GraphRepository graphRepository;
    private final VisibilityTranslator visibilityTranslator;
    private Authorizations publicOntologyAuthorizations;

    @Inject
    public GeSchemaRepository(
            Graph graph,
            GraphRepository graphRepository,
            VisibilityTranslator visibilityTranslator,
            Configuration config,
            GraphAuthorizationRepository graphAuthorizationRepository,
            CacheService cacheService
    ) throws Exception {
        super(config, graph, cacheService);
        try {
            this.graph = graph;
            this.graphRepository = graphRepository;
            this.visibilityTranslator = visibilityTranslator;

            graphAuthorizationRepository.addAuthorizationToGraph(VISIBILITY_STRING);
            graphAuthorizationRepository.addAuthorizationToGraph(WorkspaceRepository.VISIBILITY_STRING);

            defineRequiredProperties(graph);

            publicOntologyAuthorizations = graph.createAuthorizations(VISIBILITY_STRING, WorkspaceRepository.VISIBILITY_STRING);

            loadOntologies();
        } catch (Exception ex) {
            LOGGER.error("Could not initialize: %s", this.getClass().getName(), ex);
            throw ex;
        }
    }

    @Override
    public ClientApiSchema getClientApiObject(String namespace) {
        return super.getClientApiObject(namespace);
    }

    @Override
    public void clearCache() {
        LOGGER.debug("clearing ontology cache");
        super.clearCache();
        graph.flush();
    }

    public void clearCache(String namespace) {
        checkNotNull(namespace, "Workspace should not be null");
        LOGGER.debug("clearing ontology cache for workspace %s", namespace);
        super.clearCache(namespace);
        graph.flush();
    }

    @Override
    public Authorizations getAuthorizations() {
        if (this.authorizations != null) {
            return this.authorizations;
        }

        return publicOntologyAuthorizations;
    }

    @Override
    public Iterable<Relationship> getRelationships(Iterable<String> ids, String namespace) {
        Iterable<Vertex> vertices = graph.getVertices(ids, getAuthorizations(namespace));
        return transformRelationships(vertices, namespace);
    }

    @Override
    public Iterable<Relationship> getRelationships(String namespace) {
        Iterable<Vertex> vertices = graph.getVerticesWithPrefix(ID_PREFIX_RELATIONSHIP, getAuthorizations(namespace));
        return transformRelationships(vertices, namespace);
    }

    private Relationship toGeRelationship(String parentName, Vertex relationshipVertex, List<SchemaProperty> properties, Map<String, String> relatedVertexIdToNameMap, String namespace) {
        Authorizations authorizations = getAuthorizations(namespace);

        Set<String> domainVertexIds = IterableUtils.toSet(relationshipVertex.getVertexIds(Direction.IN, LabelName.HAS_EDGE.toString(), authorizations));
        List<String> domainNames = domainVertexIds.stream().map(relatedVertexIdToNameMap::get).collect(Collectors.toList());

        Set<String> rangeVertexIds = IterableUtils.toSet(relationshipVertex.getVertexIds(Direction.OUT, LabelName.HAS_EDGE.toString(), authorizations));
        List<String> rangeNames = rangeVertexIds.stream().map(relatedVertexIdToNameMap::get).collect(Collectors.toList());

        Set<String> inverseOfVertexIds = IterableUtils.toSet(relationshipVertex.getVertexIds(Direction.OUT, LabelName.INVERSE_OF.toString(), getAuthorizations(namespace)));
        List<String> inverseOfNames = inverseOfVertexIds.stream().map(relatedVertexIdToNameMap::get).collect(Collectors.toList());

        return createRelationship(parentName, relationshipVertex, inverseOfNames, domainNames, rangeNames, properties, namespace);
    }

    @Override
    public String getDisplayNameForLabel(String relationshipName, String namespace) {
        String displayName = null;
        if (relationshipName != null && !relationshipName.trim().isEmpty()) {
            try {
                Relationship relationship = getRelationshipByName(relationshipName, namespace);
                if (relationship != null) {
                    displayName = relationship.getDisplayName();
                }
            } catch (IllegalArgumentException iae) {
                throw new IllegalStateException(
                        String.format("Found multiple vertices for relationship label \"%s\"", relationshipName),
                        iae
                );
            }
        }
        return displayName;
    }

    @Override
    public Iterable<SchemaProperty> getProperties(Iterable<String> ids, String namespace) {
        Iterable<Vertex> vertices = graph.getVertices(ids, getAuthorizations(namespace));
        return transformProperties(vertices, namespace);
    }

    @Override
    public Iterable<SchemaProperty> getProperties(String namespace) {
        Iterable<Vertex> vertices = graph.getVerticesWithPrefix(ID_PREFIX_PROPERTY, getAuthorizations(namespace));
        return transformProperties(vertices, namespace);
    }

    protected ImmutableList<String> getDependentPropertyNames(Vertex vertex, String namespace) {
        List<Edge> dependentProperties = Lists.newArrayList(vertex.getEdges(Direction.OUT, LabelName.HAS_DEPENDENT_PROPERTY.toString(), getAuthorizations(namespace)));
        dependentProperties.sort((e1, e2) -> {
            Integer o1 = SchemaProperties.DEPENDENT_PROPERTY_ORDER_PROPERTY_NAME.getPropertyValue(e1, 0);
            Integer o2 = SchemaProperties.DEPENDENT_PROPERTY_ORDER_PROPERTY_NAME.getPropertyValue(e2, 0);
            return Integer.compare(o1, o2);
        });
        return ImmutableList.copyOf(dependentProperties.stream().map(e -> {
            String propertyId = e.getOtherVertexId(vertex.getId());
            Vertex v = graph.getVertex(propertyId, getAuthorizations(namespace));
            return SchemaProperties.ONTOLOGY_TITLE.getPropertyValue(v);
        }).collect(Collectors.toList()));
    }

    @Override
    public Iterable<Concept> getConceptsWithProperties(String namespace) {
        Authorizations authorizations = getAuthorizations(namespace);
        return transformConcepts(graph.getVerticesWithPrefix(ID_PREFIX_CONCEPT, authorizations), namespace);
    }

    @Override
    public Concept getRootConcept(String namespace) {
        return getConceptByName(GeSchemaRepository.ROOT_CONCEPT_NAME, namespace);
    }

    @Override
    public Concept getThingConcept(String namespace) {
        return getConceptByName(GeSchemaRepository.THING_CONCEPT_NAME, namespace);
    }

    @Override
    public List<Concept> getChildConcepts(Concept concept, String namespace) {
        Vertex conceptVertex = ((GeConcept) concept).getVertex();
        return toConcepts(conceptVertex.getVertices(Direction.IN, LabelName.IS_A.toString(), FetchHints.ALL_INCLUDING_HIDDEN, getAuthorizations(namespace)), namespace);
    }

    @Override
    protected List<Relationship> getChildRelationships(Relationship relationship, String namespace) {
        Vertex relationshipVertex = ((GeRelationship) relationship).getVertex();
        return transformRelationships(relationshipVertex.getVertices(Direction.IN, LabelName.IS_A.toString(), getAuthorizations(namespace)), namespace);
    }

    @Override
    public Relationship getParentRelationship(Relationship relationship, String namespace) {
        Vertex parentVertex = getParentVertex(((GeRelationship) relationship).getVertex(), namespace);
        if (parentVertex == null) {
            return null;
        }

        String parentName = SchemaProperties.ONTOLOGY_TITLE.getPropertyValue(parentVertex);
        return getRelationshipByName(parentName, namespace);
    }

    @Override
    public Concept getParentConcept(final Concept concept, String namespace) {
        Vertex parentConceptVertex = getParentVertex(((GeConcept) concept).getVertex(), namespace);
        if (parentConceptVertex == null) {
            return null;
        }

        String parentName = SchemaProperties.ONTOLOGY_TITLE.getPropertyValue(parentConceptVertex);
        return getConceptByName(parentName, namespace);
    }

    private List<Concept> toConcepts(Iterable<Vertex> vertices, String namespace) {
        ArrayList<Concept> concepts = new ArrayList<>();
        for (Vertex vertex : vertices) {
            concepts.add(createConcept(vertex, namespace));
        }
        return concepts;
    }

    private List<SchemaProperty> getPropertiesByVertexNoRecursion(Vertex vertex, String namespace) {
        return Lists.newArrayList(new ConvertingIterable<Vertex, SchemaProperty>(vertex.getVertices(Direction.OUT, LabelName.HAS_PROPERTY.toString(), FetchHints.ALL_INCLUDING_HIDDEN, getAuthorizations(namespace))) {
            @Override
            protected SchemaProperty convert(Vertex o) {
                return createOntologyProperty(o, getDependentPropertyNames(o, namespace), GeSchemaProperty.getDataType(o), namespace);
            }
        });
    }

    @Override
    public Iterable<Concept> getConcepts(Iterable<String> ids, String namespace) {
        return transformConcepts(graph.getVertices(ids, FetchHints.ALL, getAuthorizations(namespace)), namespace);
    }

    @Override
    public Iterable<Concept> getConceptsByName(List<String> conceptNames, String namespace) {
        try (QueryResultsIterable<Vertex> vertices = getGraph().query(getAuthorizations(namespace))
                .hasConceptType(SchemaRepository.TYPE_CONCEPT)
                .has(SchemaProperties.ONTOLOGY_TITLE.getPropertyName(), Contains.IN, Values.stringArray(conceptNames.toArray(new String[0])))
                .vertices()) {
            return transformConcepts(vertices, namespace);
        } catch (Exception e) {
            throw new GeException("Could not close scroll iterable: ", e);
        }
    }

    @Override
    public boolean hasConceptByName(String conceptName, String namespace) {
        try {
            try (QueryResultsIterable<String> results = getGraph().query(getAuthorizations(namespace))
                    .hasConceptType(SchemaRepository.TYPE_CONCEPT)
                    .has(SchemaProperties.ONTOLOGY_TITLE.getPropertyName(), Compare.EQUAL, Values.stringValue(conceptName))
                    .vertexIds()) {
                return results.getTotalHits() == 1;
            }
        } catch (IOException e) {
            throw new GeException("Could not close scroll iterable: ", e);
        }
    }

    @Override
    public Iterable<SchemaProperty> getPropertiesByName(List<String> propertyNames, String namespace) {
        try (QueryResultsIterable<Vertex> vertices = getGraph().query(getAuthorizations(namespace))
                .hasConceptType(SchemaRepository.TYPE_PROPERTY)
                .has(SchemaProperties.ONTOLOGY_TITLE.getPropertyName(), Contains.IN, Values.stringArray(propertyNames.toArray(new String[0])))
                .vertices()) {
            return transformProperties(vertices, namespace);
        } catch (Exception e) {
            throw new GeException("Could not close scroll iterable: ", e);
        }
    }

    @Override
    public Iterable<Relationship> getRelationshipsByName(List<String> relationshipNames, String namespace) {
        try (QueryResultsIterable<Vertex> vertices = getGraph().query(getAuthorizations(namespace))
                .hasConceptType(SchemaRepository.TYPE_RELATIONSHIP)
                .has(SchemaProperties.ONTOLOGY_TITLE.getPropertyName(), Contains.IN, Values.stringArray(relationshipNames.toArray(new String[0])))
                .vertices()) {
            return transformRelationships(vertices, namespace);
        } catch (Exception e) {
            throw new GeException("Could not close scroll iterable: ", e);
        }
    }

    @Override
    public List<SchemaProperty> getPropertiesByIntent(String intent, String namespace) {
        try (QueryResultsIterable<Vertex> vertices = getGraph().query(getAuthorizations(namespace))
                .hasConceptType(SchemaRepository.TYPE_PROPERTY)
                .has(SchemaProperties.INTENT.getPropertyName(), Values.stringValue(intent))
                .vertices()) {
            return transformProperties(vertices, namespace);
        } catch (Exception e) {
            throw new GeException("Could not close scroll iterable: ", e);
        }
    }

    private void internalDeleteObject(Vertex vertex, String namespace) {
        Authorizations authorizations = getAuthorizations(namespace);
        Iterable<EdgeInfo> edges = vertex.getEdgeInfos(Direction.BOTH, authorizations);
        for (EdgeInfo edge : edges) {
            graph.deleteEdge(edge.getEdgeId(), authorizations);
        }
        graph.deleteVertex(vertex.getId(), authorizations);
    }

    @Override
    protected void internalDeleteConcept(Concept concept, String namespace) {
        Vertex vertex = ((GeConcept) concept).getVertex();
        internalDeleteObject(vertex, namespace);
    }

    @Override
    protected void internalDeleteProperty(SchemaProperty property, String namespace) {
        Vertex vertex = ((GeSchemaProperty) property).getVertex();
        internalDeleteObject(vertex, namespace);
        graph.removePropertyDefinition(property.getName());
    }

    @Override
    protected void internalDeleteRelationship(Relationship relationship, String namespace) {
        Vertex vertex = ((GeRelationship) relationship).getVertex();
        internalDeleteObject(vertex, namespace);
    }

    @Override
    protected Concept internalGetOrCreateConcept(Concept parent, String conceptName, String displayName, String glyphIconHref, String color, boolean deleteChangeableProperties, boolean isCoreConcept, User user, String namespace) {
        Concept concept = getConceptByName(conceptName, namespace);
        if (concept != null) {
            if (deleteChangeableProperties) {
                deleteChangeableProperties(concept, getAuthorizations(namespace));
            }
            return concept;
        }

        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(getPriority(user), user, getAuthorizations(namespace))) {
            ctx.setPushOnQueue(false);

            Visibility visibility = VISIBILITY.getVisibility();
            VisibilityJson visibilityJson = new VisibilityJson(visibility.getVisibilityString());

            VertexBuilder builder = prepareVertex(ID_PREFIX_CONCEPT, conceptName, namespace, visibility, visibilityJson, TYPE_CONCEPT);

            ZonedDateTime modifiedDate = ZonedDateTime.now();
            Vertex vertex = ctx.update(builder, modifiedDate, visibilityJson, elemCtx -> {
                Metadata metadata = getMetadata(modifiedDate, user, visibility);
                SchemaProperties.ONTOLOGY_TITLE.updateProperty(elemCtx, conceptName, metadata, visibility);
                SchemaProperties.DISPLAY_NAME.updateProperty(elemCtx, displayName, metadata, visibility);
                SchemaProperties.CORE_CONCEPT.updateProperty(elemCtx, isCoreConcept, VISIBILITY.getVisibility());

                if (conceptName.equals(SchemaRepository.THING_CONCEPT_NAME)) {
                    SchemaProperties.TITLE_FORMULA.updateProperty(elemCtx, "prop('title') || ('Untitled ' + ontology && ontology.displayName) ", metadata, visibility);
                    SchemaProperties.SUBTITLE_FORMULA.updateProperty(elemCtx, "(ontology && ontology.displayName) || prop('source') || ''", metadata, visibility);
                    SchemaProperties.TIME_FORMULA.updateProperty(elemCtx, "prop('modifiedDate') || ''", metadata, visibility);
                }

                if (!StringUtils.isEmpty(glyphIconHref)) {
                    SchemaProperties.GLYPH_ICON_FILE_NAME.updateProperty(elemCtx, glyphIconHref, metadata, visibility);
                }
                if (!StringUtils.isEmpty(color)) {
                    SchemaProperties.COLOR.updateProperty(elemCtx, color, metadata, visibility);
                }
            }).get();

            if (parent == null) {
                concept = createConcept(vertex, namespace);
            } else {
                concept = createConcept(vertex, null, parent.getName(), namespace);
                findOrAddEdge(ctx, ((GeConcept) concept).getVertex(), ((GeConcept) parent).getVertex(), LabelName.IS_A.toString());
            }

            if (!isPublic(namespace)) {
                findOrAddEdge(ctx, namespace, ((GeConcept) concept).getVertex().getId(), WorkspaceSchema.WORKSPACE_TO_ONTOLOGY_RELATIONSHIP_NAME);
            }

            return concept;
        } catch (Exception e) {
            throw new BcException("Could not create concept: " + conceptName, e);
        }
    }

    private Metadata getMetadata(ZonedDateTime modifiedDate, User user, Visibility visibility) {
        Metadata metadata = Metadata.create();
        BcSchema.MODIFIED_DATE_METADATA.setMetadata(metadata, modifiedDate, visibility);
        if (user != null) {
            BcSchema.MODIFIED_BY_METADATA.setMetadata(metadata, user.getUserId(), visibility);
        }
        return metadata;
    }

    private VertexBuilder prepareVertex(String prefix, String name, String namespace, Visibility visibility, VisibilityJson visibilityJson, String conceptType) {
        if (isPublic(namespace)) {
            return graph.prepareVertex(prefix + Hashing.sha256().hashString(name, Charsets.UTF_8), visibility, conceptType);
        }

        String id = prefix + Hashing.sha256().hashString(namespace + name, Charsets.UTF_8).toString();
        visibilityJson.addWorkspace(namespace);
        return graph.prepareVertex(id, visibilityTranslator.toVisibility(visibilityJson).getVisibility(), conceptType);
    }

    protected void findOrAddEdge(Vertex fromVertex, final Vertex toVertex, String edgeLabel, User user, String namespace) {
        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(Priority.LOW, user, getAuthorizations(namespace))) {
            ctx.setPushOnQueue(false);
            findOrAddEdge(ctx, fromVertex, toVertex, edgeLabel);
        } catch (Exception e) {
            throw new BcException("Could not findOrAddEdge", e);
        }
    }

    protected void removeEdge(GraphUpdateContext ctx, String fromVertexId, String toVertexId) {
        String edgeId = fromVertexId + "-" + toVertexId;
        ctx.getGraph().deleteEdge(edgeId, ctx.getAuthorizations());
    }

    protected void findOrAddEdge(GraphUpdateContext ctx, String fromVertexId, String toVertexId, String edgeLabel) {
        String edgeId = fromVertexId + "-" + toVertexId;
        ctx.getOrCreateEdgeAndUpdate(edgeId, fromVertexId, toVertexId, edgeLabel, VISIBILITY.getVisibility(), elemCtx -> {
            if (elemCtx.isNewElement()) {
                VisibilityJson visibilityJson = new VisibilityJson(VISIBILITY.getVisibility().getVisibilityString());
                elemCtx.updateBuiltInProperties(ZonedDateTime.now(), visibilityJson);
            }
        });
    }

    protected void findOrAddEdge(GraphUpdateContext ctx, Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        findOrAddEdge(ctx, fromVertex.getId(), toVertex.getId(), edgeLabel);
    }

    @Override
    public void addDomainConceptsToRelationshipType(String relationshipName, List<String> conceptNames, User user, String namespace) {
        checkPrivileges(user, namespace);
        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(Priority.LOW, user, getAuthorizations(namespace))) {
            ctx.setPushOnQueue(false);
            GeRelationship relationship = (GeRelationship) getRelationshipByName(relationshipName, namespace);
            Vertex relationshipVertex = relationship.getVertex();
            if (!isPublic(namespace) && relationship.getSandboxStatus() != SandboxStatus.PRIVATE) {
                throw new UnsupportedOperationException("Sandboxed updating of domain names is not currently supported for published relationships");
            }

            Iterable<Concept> concepts = getConceptsByName(conceptNames, namespace);
            for (Concept concept : concepts) {
                checkNotNull(concept, "concepts cannot have null values");
                findOrAddEdge(ctx, ((GeConcept) concept).getVertex(), relationshipVertex, LabelName.HAS_EDGE.toString());
            }
        }
    }

    @Override
    public void addRangeConceptsToRelationshipType(String relationshipName, List<String> conceptNames, User user, String namespace) {
        checkPrivileges(user, namespace);
        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(Priority.LOW, user, getAuthorizations(namespace))) {
            ctx.setPushOnQueue(false);
            GeRelationship relationship = (GeRelationship) getRelationshipByName(relationshipName, namespace);
            Vertex relationshipVertex = relationship.getVertex();
            if (!isPublic(namespace) && relationship.getSandboxStatus() != SandboxStatus.PRIVATE) {
                throw new UnsupportedOperationException("Sandboxed updating of range names is not currently supported for published relationships");
            }
            Iterable<Concept> concepts = getConceptsByName(conceptNames, namespace);
            for (Concept concept : concepts) {
                checkNotNull(concept, "concepts cannot have null values");
                findOrAddEdge(ctx, relationshipVertex, ((GeConcept) concept).getVertex(), LabelName.HAS_EDGE.toString());
            }
        }
    }

    @Override
    public SchemaProperty addPropertyTo(
            List<Concept> concepts,
            List<Relationship> relationships,
            List<String> extendedDataTableNames,
            String propertyName,
            String displayName,
            PropertyType dataType,
            Map<String, String> possibleValues,
            Collection<TextIndexHint> textIndexHints,
            boolean userVisible,
            boolean searchFacet,
            boolean systemProperty,
            String aggType,
            int aggPrecision,
            String aggInterval,
            long aggMinDocumentCount,
            String aggTimeZone,
            String aggCalendarField,
            boolean searchable,
            boolean addable,
            boolean sortable,
            Integer sortPriority,
            String displayType,
            String propertyGroup,
            Double boost,
            String validationFormula,
            String displayFormula,
            ImmutableList<String> dependentPropertyNames,
            String[] intents,
            boolean deleteable,
            boolean updateable,
            User user,
            String namespace
    ) {
        if (CollectionUtils.isEmpty(extendedDataTableNames)
                && CollectionUtils.isEmpty(concepts)
                && CollectionUtils.isEmpty(relationships)) {
            throw new BcException("Must specify concepts or relationships to add property");
        }
        Vertex vertex = getOrCreatePropertyVertex(
                propertyName,
                dataType,
                textIndexHints,
                sortable,
                boost,
                possibleValues,
                concepts,
                relationships,
                extendedDataTableNames,
                user,
                namespace
        );
        checkNotNull(vertex, "Could not find property: " + propertyName);

        boolean finalSearchable = determineSearchable(propertyName, dataType, textIndexHints, searchable);

        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(Priority.LOW, getSystemUser(), getAuthorizations(namespace))) {
            ctx.setPushOnQueue(false);

            ZonedDateTime modifiedDate = ZonedDateTime.now();
            Visibility visibility = VISIBILITY.getVisibility();
            Metadata metadata = getMetadata(modifiedDate, user, visibility);

            vertex = ctx.update(vertex.prepareMutation(), elemCtx -> {
                SchemaProperties.SEARCHABLE.updateProperty(elemCtx, finalSearchable, metadata, visibility);
                SchemaProperties.SORTABLE.updateProperty(elemCtx, sortable, metadata, visibility);
                SchemaProperties.ADDABLE.updateProperty(elemCtx, addable, metadata, visibility);
                SchemaProperties.DELETEABLE.updateProperty(elemCtx, deleteable, metadata, visibility);
                SchemaProperties.UPDATEABLE.updateProperty(elemCtx, updateable, metadata, visibility);
                SchemaProperties.USER_VISIBLE.updateProperty(elemCtx, userVisible, metadata, visibility);
                SchemaProperties.SEARCH_FACET.updateProperty(elemCtx, searchFacet, visibility);
                SchemaProperties.SYSTEM_PROPERTY.updateProperty(elemCtx, systemProperty, visibility);

                if (sortPriority != null) {
                    SchemaProperties.SORT_PRIORITY.updateProperty(elemCtx, sortPriority, metadata, visibility);
                }

                if(aggType != null) {
                    SchemaProperties.AGG_TYPE.updateProperty(elemCtx, aggType, metadata, visibility);
                }
                if(aggInterval != null) {
                    SchemaProperties.AGG_INTERVAL.updateProperty(elemCtx, aggInterval, metadata, visibility);
                }

                SchemaProperties.AGG_MIN_DOCUMENT_COUNT.updateProperty(elemCtx, aggMinDocumentCount, metadata, visibility);

                if(aggTimeZone != null) {
                    SchemaProperties.AGG_TIMEZONE.updateProperty(elemCtx, aggTimeZone, metadata, visibility);
                }

                if(aggCalendarField != null) {
                    SchemaProperties.AGG_CALENDAR_FIELD.updateProperty(elemCtx, aggCalendarField, metadata, visibility);
                }

                if (boost != null) {
                    SchemaProperties.BOOST.updateProperty(elemCtx, boost, metadata, visibility);
                }
                if (displayName != null && !displayName.trim().isEmpty()) {
                    SchemaProperties.DISPLAY_NAME.updateProperty(elemCtx, displayName.trim(), metadata, visibility);
                }
                if (displayType != null && !displayType.trim().isEmpty()) {
                    SchemaProperties.DISPLAY_TYPE.updateProperty(elemCtx, displayType, metadata, visibility);
                }
                if (propertyGroup != null && !propertyGroup.trim().isEmpty()) {
                    SchemaProperties.PROPERTY_GROUP.updateProperty(elemCtx, propertyGroup, metadata, visibility);
                }
                if (validationFormula != null && !validationFormula.trim().isEmpty()) {
                    SchemaProperties.VALIDATION_FORMULA.updateProperty(elemCtx, validationFormula, metadata, visibility);
                }
                if (displayFormula != null && !displayFormula.trim().isEmpty()) {
                    SchemaProperties.DISPLAY_FORMULA.updateProperty(elemCtx, displayFormula, metadata, visibility);
                }
                if (possibleValues != null) {
                    SchemaProperties.POSSIBLE_VALUES.updateProperty(elemCtx, JSONUtil.toJson(possibleValues), metadata, visibility);
                }
                if (intents != null) {
                    for (String intent : intents) {
                        SchemaProperties.INTENT.updateProperty(elemCtx, intent, intent, metadata, visibility);
                    }
                }
            }).get();

            if (dependentPropertyNames != null) {
                saveDependentProperties(propertyName, vertex, dependentPropertyNames, user, namespace);
            }

            return createOntologyProperty(vertex, dependentPropertyNames, dataType, namespace);
        } catch (Exception e) {
            throw new BcException("Could not create property: " + propertyName, e);
        }
    }

    @Override
    public void getOrCreateInverseOfRelationship(Relationship fromRelationship, Relationship inverseOfRelationship) {
        checkNotNull(fromRelationship, "fromRelationship is required");
        checkNotNull(fromRelationship, "inverseOfRelationship is required");

        GeRelationship fromRelationshipSg = (GeRelationship) fromRelationship;
        GeRelationship inverseOfRelationshipSg = (GeRelationship) inverseOfRelationship;

        Vertex fromVertex = fromRelationshipSg.getVertex();
        checkNotNull(fromVertex, "fromVertex is required");

        Vertex inverseVertex = inverseOfRelationshipSg.getVertex();
        checkNotNull(inverseVertex, "inverseVertex is required");

        User user = getSystemUser();
        findOrAddEdge(fromVertex, inverseVertex, LabelName.INVERSE_OF.toString(), user, null);
        findOrAddEdge(inverseVertex, fromVertex, LabelName.INVERSE_OF.toString(), user, null);
    }

    @Override
    public void removeInverseOfRelationship(Relationship fromRelationship, Relationship inverseOfRelationship) {
        checkNotNull(fromRelationship, "fromRelationship is required");
        checkNotNull(fromRelationship, "inverseOfRelationship is required");

        GeRelationship fromRelationshipSg = (GeRelationship) fromRelationship;
        GeRelationship inverseOfRelationshipSg = (GeRelationship) inverseOfRelationship;

        Vertex fromVertex = fromRelationshipSg.getVertex();
        checkNotNull(fromVertex, "fromVertex is required");

        Vertex inverseVertex = inverseOfRelationshipSg.getVertex();
        checkNotNull(inverseVertex, "inverseVertex is required");

        Iterable<String> edgeIds1 = fromVertex.getEdgeIds(inverseVertex, Direction.OUT, LabelName.INVERSE_OF.toString(), authorizations);
        Iterable<String> edgeIds2 = inverseVertex.getEdgeIds(fromVertex, Direction.OUT, LabelName.INVERSE_OF.toString(), authorizations);

        User user = getSystemUser();
        try (GraphUpdateContext ctx =
                     graphRepository.beginGraphUpdate(Priority.NORMAL, user, getAuthorizations(SchemaRepository.PUBLIC))) {

            edgeIds1.forEach(eid -> ctx.getGraph().deleteEdge(eid, ctx.getAuthorizations()));
            edgeIds2.forEach(eid -> ctx.getGraph().deleteEdge(eid, ctx.getAuthorizations()));
        }
    }

    @Override
    protected Relationship internalGetOrCreateRelationshipType(
            Relationship parent,
            Iterable<Concept> domainConcepts,
            Iterable<Concept> rangeConcepts,
            String relationshipName,
            String displayName,
            boolean isDeclaredInOntology,
            boolean coreConcept,
            User user,
            String namespace
    ) {
        Relationship relationship = getRelationshipByName(relationshipName, namespace);
        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(getPriority(user), user, getAuthorizations(namespace))) {
            ctx.setPushOnQueue(false);

            if (relationship != null) {
                if (isDeclaredInOntology) {
                    deleteChangeableProperties(relationship, getAuthorizations(namespace));
                }
                Vertex relationshipVertex = ((GeRelationship) relationship).getVertex();
                for (Concept domainConcept : domainConcepts) {
                    if (!relationship.getSourceConceptNames().contains(domainConcept.getName())) {
                        findOrAddEdge(ctx, ((GeConcept) domainConcept).getVertex(), relationshipVertex, LabelName.HAS_EDGE.toString());
                    }
                }
                for (Concept rangeConcept : rangeConcepts) {
                    if (!relationship.getTargetConceptNames().contains(rangeConcept.getName())) {
                        findOrAddEdge(ctx, relationshipVertex, ((GeConcept) rangeConcept).getVertex(), LabelName.HAS_EDGE.toString());
                    }
                }
                return relationship;
            }

            Visibility visibility = VISIBILITY.getVisibility();
            VisibilityJson visibilityJson = new VisibilityJson(visibility.getVisibilityString());

            VertexBuilder builder = prepareVertex(ID_PREFIX_RELATIONSHIP, relationshipName, namespace, visibility, visibilityJson, TYPE_RELATIONSHIP);

            ZonedDateTime modifiedDate = ZonedDateTime.now();
            Vertex relationshipVertex = ctx.update(builder, modifiedDate, visibilityJson, elemCtx -> {
                Metadata metadata = getMetadata(modifiedDate, user, visibility);
                SchemaProperties.ONTOLOGY_TITLE.updateProperty(elemCtx, relationshipName, metadata, visibility);
                if (displayName != null) {
                    SchemaProperties.DISPLAY_NAME.updateProperty(elemCtx, displayName, metadata, visibility);
                }

                SchemaProperties.CORE_CONCEPT.updateProperty(elemCtx, coreConcept, metadata, visibility);
            }).get();

            validateRelationship(relationshipName, domainConcepts, rangeConcepts);

            for (Concept domainConcept : domainConcepts) {
                findOrAddEdge(ctx, ((GeConcept) domainConcept).getVertex(), relationshipVertex, LabelName.HAS_EDGE.toString());
            }

            for (Concept rangeConcept : rangeConcepts) {
                findOrAddEdge(ctx, relationshipVertex, ((GeConcept) rangeConcept).getVertex(), LabelName.HAS_EDGE.toString());
            }

            if (parent != null) {
                findOrAddEdge(ctx, relationshipVertex, ((GeRelationship) parent).getVertex(), LabelName.IS_A.toString());
            }

            List<String> inverseOfNames = new ArrayList<>(); // no inverse of because this relationship is new

            List<String> domainConceptNames = Lists.newArrayList(new ConvertingIterable<Concept, String>(domainConcepts) {
                @Override
                protected String convert(Concept o) {
                    return o.getName();
                }
            });

            List<String> rangeConceptNames = Lists.newArrayList(new ConvertingIterable<Concept, String>(rangeConcepts) {
                @Override
                protected String convert(Concept o) {
                    return o.getName();
                }
            });

            if (!isPublic(namespace)) {
                findOrAddEdge(ctx, namespace, relationshipVertex.getId(), WorkspaceSchema.WORKSPACE_TO_ONTOLOGY_RELATIONSHIP_NAME);
            }

            Collection<SchemaProperty> properties = new ArrayList<>();
            String parentName = parent == null ? null : parent.getName();
            return createRelationship(parentName, relationshipVertex, inverseOfNames, domainConceptNames, rangeConceptNames, properties, namespace);
        } catch (Exception ex) {
            throw new BcException("Could not create relationship: " + relationshipName, ex);
        }
    }

    private Vertex getOrCreatePropertyVertex(
            final String propertyName,
            final PropertyType dataType,
            Collection<TextIndexHint> textIndexHints,
            boolean sortable,
            Double boost,
            Map<String, String> possibleValues,
            List<Concept> concepts,
            List<Relationship> relationships,
            List<String> extendedDataTableNames,
            User user,
            String namespace
    ) {
        Authorizations authorizations = getAuthorizations(namespace);

        SchemaProperty typeProperty = getPropertyByName(propertyName, namespace);
        Vertex propertyVertex;
        if (typeProperty == null) {
            definePropertyOnGraph(graph, propertyName, PropertyType.getTypeClass(dataType), textIndexHints, boost, sortable);

            try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(getPriority(user), user, authorizations)) {
                ctx.setPushOnQueue(false);

                Visibility visibility = VISIBILITY.getVisibility();
                VisibilityJson visibilityJson = new VisibilityJson(visibility.getVisibilityString());

                VertexBuilder builder = prepareVertex(ID_PREFIX_PROPERTY, propertyName, namespace, visibility, visibilityJson, TYPE_PROPERTY);
                ZonedDateTime modifiedDate = ZonedDateTime.now();
                propertyVertex = ctx.update(builder, modifiedDate, visibilityJson, elemCtx -> {
                    Metadata metadata = getMetadata(modifiedDate, user, visibility);
                    SchemaProperties.ONTOLOGY_TITLE.updateProperty(elemCtx, propertyName, metadata, visibility);
                    SchemaProperties.DATA_TYPE.updateProperty(elemCtx, dataType.toString(), metadata, visibility);
                    if (possibleValues != null) {
                        SchemaProperties.POSSIBLE_VALUES.updateProperty(elemCtx, JSONUtil.toJson(possibleValues), metadata, visibility);
                    }
                    if (textIndexHints != null && textIndexHints.size() > 0) {
                        textIndexHints.forEach(i -> {
                            String textIndexHint = i.toString();
                            SchemaProperties.TEXT_INDEX_HINTS.updateProperty(elemCtx, textIndexHint, textIndexHint, metadata, visibility);
                        });
                    }
                }).get();

                for (Concept concept : concepts) {
                    checkNotNull(concept, "concepts cannot have null values");
                    findOrAddEdge(ctx, ((GeConcept) concept).getVertex(), propertyVertex, LabelName.HAS_PROPERTY.toString());
                }
                for (Relationship relationship : relationships) {
                    checkNotNull(relationship, "relationships cannot have null values");
                    findOrAddEdge(ctx, ((GeRelationship) relationship).getVertex(), propertyVertex, LabelName.HAS_PROPERTY.toString());
                }
                if (extendedDataTableNames != null) {
                    for (String extendedDataTableName : extendedDataTableNames) {
                        checkNotNull(extendedDataTableName, "extendedDataTableNames cannot have null values");
                        SchemaProperty tableProperty = getPropertyByName(extendedDataTableName, namespace);
                        checkNotNull(tableProperty, "Could not find extended data property: " + extendedDataTableName);
                        if (!(tableProperty instanceof GeExtendedDataTableSchemaProperty)) {
                            throw new BcException("Found property " + extendedDataTableName + " but was expecting "
                                    + "an extended data table property, check that the range is set to extended data property");
                        }
                        GeExtendedDataTableSchemaProperty extendedDataTableProperty = (GeExtendedDataTableSchemaProperty) tableProperty;
                        Vertex extendedDataTableVertex = extendedDataTableProperty.getVertex();
                        findOrAddEdge(ctx, extendedDataTableVertex, propertyVertex, LabelName.HAS_PROPERTY.toString());
                        extendedDataTableProperty.addProperty(propertyName);
                    }
                }
                if (!isPublic(namespace)) {
                    findOrAddEdge(ctx, namespace, propertyVertex.getId(), WorkspaceSchema.WORKSPACE_TO_ONTOLOGY_RELATIONSHIP_NAME);
                }
            } catch (Exception e) {
                throw new BcException("Could not getOrCreatePropertyVertex: " + propertyName, e);
            }
        } else {
            propertyVertex = ((GeSchemaProperty) typeProperty).getVertex();
            deleteChangeableProperties(typeProperty, authorizations);
        }
        return propertyVertex;
    }

    private Priority getPriority(User user) {
        return user == null ? Priority.LOW : Priority.NORMAL;
    }

    private void saveDependentProperties(String propertyName, Vertex propertyVertex, Collection<String> dependentPropertyNames, User user, String namespace) {
        Authorizations authorizations = getAuthorizations(namespace);

        Iterable<Edge> existingDependentProperties = propertyVertex.getEdges(Direction.OUT, LabelName.HAS_DEPENDENT_PROPERTY.toString(), authorizations);
        existingDependentProperties.forEach(e -> graph.deleteEdge(e, authorizations));
        graph.flush();

        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(getPriority(user), user, authorizations)) {
            ctx.setPushOnQueue(false);

            Visibility visibility = VISIBILITY.getVisibility();
            VisibilityJson visibilityJson = new VisibilityJson(visibility.getVisibilityString());
            ZonedDateTime modifiedDate = ZonedDateTime.now();
            Metadata metadata = getMetadata(modifiedDate, user, visibility);
            AtomicInteger indexCounter = new AtomicInteger();

            for (String dependentPropertyName : dependentPropertyNames) {
                Vertex dependentPropVertex = getPropertyVertex(dependentPropertyName, authorizations);
                if(dependentPropVertex == null)
                    throw new GeException("Could not add dependent property: "+dependentPropertyName+" to property: "+propertyName+" because the dependent property was not found.");

                int i = indexCounter.getAndIncrement();
                EdgeBuilder edgeBuilder = graph.prepareEdge(propertyVertex, dependentPropVertex, LabelName.HAS_DEPENDENT_PROPERTY.toString(), visibility);
                ctx.update(edgeBuilder, edgeCtx -> {
                    edgeCtx.updateBuiltInProperties(modifiedDate, visibilityJson);
                    SchemaProperties.DEPENDENT_PROPERTY_ORDER_PROPERTY_NAME.updateProperty(edgeCtx, i, metadata, visibility);
                });
            }
        }
    }

    private Vertex getPropertyVertex(String propertyName, Authorizations authorizations) {
        return graph.getVertex(ID_PREFIX_PROPERTY + Hashing.sha256().hashString(propertyName, Charsets.UTF_8), authorizations);
    }

    @Override
    public void updatePropertyDependentNames(SchemaProperty property, Collection<String> dependentPropertyNames, User user, String namespace) {
        GeSchemaProperty geOntologyProperty = (GeSchemaProperty) property;
        if (!isPublic(namespace) || property.getSandboxStatus() == SandboxStatus.PRIVATE) {
            throw new UnsupportedOperationException("Sandboxed updating of dependent names is not currently supported for properties");
        }

        saveDependentProperties(property.getName(), geOntologyProperty.getVertex(), dependentPropertyNames, user, namespace);
        graph.flush();
        geOntologyProperty.setDependentProperties(dependentPropertyNames);
    }

    @Override
    public void updatePropertyDomainNames(SchemaProperty property, Set<String> domainNames, User user, String namespace) {
        GeSchemaProperty geProperty = (GeSchemaProperty) property;
        if (!isPublic(namespace) && property.getSandboxStatus() != SandboxStatus.PRIVATE) {
            throw new UnsupportedOperationException("Sandboxed updating of domain names is not currently supported for published properties");
        }

        Iterable<EdgeVertexPair> existingConcepts = geProperty.getVertex().getEdgeVertexPairs(Direction.BOTH, LabelName.HAS_PROPERTY.toString(), getAuthorizations(namespace));
        for (EdgeVertexPair existingConcept : existingConcepts) {
            String conceptName = SchemaProperties.ONTOLOGY_TITLE.getPropertyValue(existingConcept.getVertex());
            if (!domainNames.remove(conceptName)) {
                getGraph().softDeleteEdge(existingConcept.getEdge(), getAuthorizations(namespace));
            }
        }

        for (String domainName : domainNames) {
            Vertex domainVertex;
            Concept concept = getConceptByName(domainName, namespace);
            if (concept != null) {
                domainVertex = ((GeConcept) concept).getVertex();
            } else {
                Relationship relationship = getRelationshipByName(domainName, namespace);
                if (relationship != null) {
                    domainVertex = ((GeRelationship) relationship).getVertex();
                } else {
                    throw new BcException("Could not find domain with name " + domainName);
                }
            }
            findOrAddEdge(domainVertex, ((GeSchemaProperty) property).getVertex(), LabelName.HAS_PROPERTY.toString(), user, namespace);
        }
    }

    private Vertex getParentVertex(Vertex vertex, String namespace) {
        try {
            return Iterables.getOnlyElement(vertex.getVertices(Direction.OUT, LabelName.IS_A.toString(), getAuthorizations(namespace)), null);
        } catch (IllegalArgumentException iae) {
            throw new IllegalStateException(String.format(
                    "Unexpected number of parents for concept %s",
                    SchemaProperties.TITLE.getPropertyValue(vertex)
            ), iae);
        }
    }

    public Authorizations getAuthorizations(String namespace, String... otherAuthorizations) {
        if (isPublic(namespace) && (otherAuthorizations == null || otherAuthorizations.length == 0)) {
            return publicOntologyAuthorizations;
        }

        if (isPublic(namespace)) {
            return graph.createAuthorizations(publicOntologyAuthorizations, otherAuthorizations);
        } else if (otherAuthorizations == null || otherAuthorizations.length == 0) {
            return graph.createAuthorizations(publicOntologyAuthorizations, namespace);
        }

        return graph.createAuthorizations(publicOntologyAuthorizations,
                (String[]) ArrayUtils.add(otherAuthorizations, namespace));
    }

    protected Graph getGraph() {
        return graph;
    }

    /**
     * Overridable so subclasses can supply a custom implementation of OntologyProperty.
     */
    protected SchemaProperty createOntologyProperty(
            Vertex propertyVertex,
            ImmutableList<String> dependentPropertyNames,
            PropertyType propertyType,
            String namespace
    ) {
        if (propertyType.equals(PropertyType.EXTENDED_DATA_TABLE)) {
            Authorizations authorizations = getAuthorizations(namespace);
            GeExtendedDataTableSchemaProperty result = new GeExtendedDataTableSchemaProperty(propertyVertex, dependentPropertyNames, namespace);
            Iterable<String> tablePropertyNames = propertyVertex.getVertexIds(Direction.OUT, LabelName.HAS_PROPERTY.toString(), authorizations);
            for (String tablePropertyName : tablePropertyNames) {
                result.addProperty(tablePropertyName.substring(GeSchemaRepository.ID_PREFIX_PROPERTY.length()));
            }
            return result;
        } else {
            return new GeSchemaProperty(propertyVertex, dependentPropertyNames, namespace);
        }
    }

    /**
     * Overridable so subclasses can supply a custom implementation of Relationship.
     */
    protected Relationship createRelationship(
            String parentName,
            Vertex relationshipVertex,
            List<String> inverseOfNames,
            List<String> domainConceptNames,
            List<String> rangeConceptNames,
            Collection<SchemaProperty> properties,
            String namespace
    ) {
        return new GeRelationship(
                parentName,
                relationshipVertex,
                domainConceptNames,
                rangeConceptNames,
                inverseOfNames,
                properties,
                namespace
        );
    }

    /**
     * Overridable so subclasses can supply a custom implementation of Concept.
     */
    protected Concept createConcept(Vertex vertex, List<SchemaProperty> conceptProperties, String parentConceptName, String namespace) {
        return new GeConcept(vertex, parentConceptName, conceptProperties, namespace);
    }

    /**
     * Overridable so subclasses can supply a custom implementation of Concept.
     */
    protected Concept createConcept(Vertex vertex, String namespace) {
        return new GeConcept(vertex, namespace);
    }

    @Override
    protected void deleteChangeableProperties(SchemaProperty property, Authorizations authorizations) {
        Vertex vertex = ((GeSchemaProperty) property).getVertex();
        deleteChangeableProperties(vertex, authorizations);
    }

    @Override
    protected void deleteChangeableProperties(SchemaElement element, Authorizations authorizations) {
        Vertex vertex = element instanceof GeConcept ? ((GeConcept) element).getVertex() : ((GeRelationship) element).getVertex();
        deleteChangeableProperties(vertex, authorizations);
    }

    private void deleteChangeableProperties(Vertex vertex, Authorizations authorizations) {
        for (Property property : vertex.getProperties()) {
            if (SchemaProperties.CHANGEABLE_PROPERTY_NAME.contains(property.getName())) {
                vertex.softDeleteProperty(property.getKey(), property.getName(), authorizations);
            }
        }
        graph.flush();
    }

    private List<SchemaProperty> transformProperties(Iterable<Vertex> vertices, String namespace) {
        return StreamSupport.stream(vertices.spliterator(), false)
                .filter(vertex -> TYPE_PROPERTY.equals(vertex.getConceptType()))
                .map(vertex -> {
                    ImmutableList<String> dependentPropertyNames = getDependentPropertyNames(vertex, namespace);
                    PropertyType dataType = GeSchemaProperty.getDataType(vertex);
                    return createOntologyProperty(vertex, dependentPropertyNames, dataType, namespace);
                })
                .collect(Collectors.toList());
    }

    private List<Concept> transformConcepts(Iterable<Vertex> vertices, String namespace) {
        Authorizations authorizations = getAuthorizations(namespace);

        List<Vertex> filtered = StreamSupport.stream(vertices.spliterator(), false)
                .filter(vertex -> TYPE_CONCEPT.equals(vertex.getConceptType()))
                .collect(Collectors.toList());

        Map<String, String> parentVertexIdToName = buildParentIdToNameMap(filtered, authorizations);

        List<String> allPropertyVertexIds = filtered.stream()
                .flatMap(vertex ->
                        StreamSupport.stream(vertex.getVertexIds(Direction.OUT, LabelName.HAS_PROPERTY.toString(), authorizations).spliterator(), false)
                ).distinct().collect(Collectors.toList());
        List<SchemaProperty> ontologyProperties = transformProperties(getGraph().getVertices(allPropertyVertexIds, authorizations), namespace);
        Map<String, SchemaProperty> ontologyPropertiesByVertexId = ontologyProperties.stream()
                .collect(Collectors.toMap(
                        ontologyProperty -> ((GeSchemaProperty) ontologyProperty).getVertex().getId(),
                        ontologyProperty -> ontologyProperty
                ));

        return filtered.stream().map(vertex -> {
            String parentVertexId = getParentVertexId(vertex, authorizations);
            String parentName = parentVertexId == null ? null : parentVertexIdToName.get(parentVertexId);

            Iterable<String> propertyVertexIds = vertex.getVertexIds(Direction.OUT, LabelName.HAS_PROPERTY.toString(), authorizations);
            List<SchemaProperty> conceptProperties = StreamSupport.stream(propertyVertexIds.spliterator(), false)
                    .map(ontologyPropertiesByVertexId::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            return createConcept(vertex, conceptProperties, parentName, namespace);
        }).collect(Collectors.toList());
    }

    private List<Relationship> transformRelationships(Iterable<Vertex> vertices, String namespace) {
        Authorizations authorizations = getAuthorizations(namespace);

        List<Vertex> filtered = StreamSupport.stream(vertices.spliterator(), false)
                .filter(vertex -> TYPE_RELATIONSHIP.equals(vertex.getConceptType()))
                .collect(Collectors.toList());

        Set<String> allRelatedVertexIds = filtered.stream()
                .flatMap(vertex ->
                        StreamUtils.stream(
                                vertex.getVertexIds(Direction.OUT, LabelName.IS_A.toString(), authorizations),
                                vertex.getVertexIds(Direction.IN, LabelName.HAS_EDGE.toString(), authorizations),
                                vertex.getVertexIds(Direction.OUT, LabelName.HAS_EDGE.toString(), authorizations),
                                vertex.getVertexIds(Direction.OUT, LabelName.INVERSE_OF.toString(), authorizations)
                        )
                ).filter(Objects::nonNull)
                .collect(Collectors.toSet());
        Map<String, String> relatedVertexIdToNameMap = buildVertexIdToNameMap(allRelatedVertexIds, authorizations);

        List<String> allPropertyVertexIds = filtered.stream()
                .flatMap(vertex ->
                        StreamSupport.stream(vertex.getVertexIds(Direction.OUT, LabelName.HAS_PROPERTY.toString(), authorizations).spliterator(), false)
                ).distinct().collect(Collectors.toList());
        List<SchemaProperty> ontologyProperties = transformProperties(getGraph().getVertices(allPropertyVertexIds, authorizations), namespace);
        Map<String, SchemaProperty> ontologyPropertiesByVertexId = ontologyProperties.stream()
                .collect(Collectors.toMap(
                        ontologyProperty -> ((GeSchemaProperty) ontologyProperty).getVertex().getId(),
                        ontologyProperty -> ontologyProperty
                ));

        return filtered.stream().map(vertex -> {
            String parentVertexId = getParentVertexId(vertex, authorizations);
            String parentName = parentVertexId == null ? null : relatedVertexIdToNameMap.get(parentVertexId);

            Iterable<String> propertyVertexIds = vertex.getVertexIds(Direction.OUT, LabelName.HAS_PROPERTY.toString(), authorizations);
            List<SchemaProperty> properties = StreamSupport.stream(propertyVertexIds.spliterator(), false)
                    .map(ontologyPropertiesByVertexId::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            return toGeRelationship(parentName, vertex, properties, relatedVertexIdToNameMap, namespace);
        }).collect(Collectors.toList());
    }

    private String getParentVertexId(Vertex vertex, Authorizations authorizations) {
        Iterable<String> parentIds = vertex.getVertexIds(Direction.OUT, LabelName.IS_A.toString(), authorizations);
        return parentIds == null ? null : Iterables.getOnlyElement(parentIds, null);
    }

    private Map<String, String> buildParentIdToNameMap(Iterable<Vertex> vertices, Authorizations authorizations) {
        Set<String> parentVertexIds = StreamSupport.stream(vertices.spliterator(), false)
                .map(vertex -> getParentVertexId(vertex, authorizations))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        return buildVertexIdToNameMap(parentVertexIds, authorizations);
    }

    private Map<String, String> buildVertexIdToNameMap(Iterable<String> vertexIds, Authorizations authorizations) {
        Iterable<Vertex> vertices = graph.getVertices(vertexIds, FetchHints.PROPERTIES, authorizations);
        try {
            return StreamSupport.stream(vertices.spliterator(), false)
                    .collect(Collectors.toMap(Vertex::getId, SchemaProperties.ONTOLOGY_TITLE::getPropertyValue));
        } finally {
            CloseableUtils.closeQuietly(vertices);
        }
    }

    @Override
    public void internalPublishConcept(Concept concept, User user, String namespace) {
        assert (concept instanceof GeConcept);
        if (concept.getSandboxStatus() != SandboxStatus.PUBLIC) {
            Vertex vertex = ((GeConcept) concept).getVertex();
            internalPublishVertex(vertex, user, namespace);
        }
    }

    @Override
    public void internalPublishRelationship(Relationship relationship, User user, String namespace) {
        assert (relationship instanceof GeRelationship);
        if (relationship.getSandboxStatus() != SandboxStatus.PUBLIC) {
            Vertex vertex = ((GeRelationship) relationship).getVertex();
            internalPublishVertex(vertex, user, namespace);
        }
    }

    @Override
    public void internalPublishProperty(SchemaProperty property, User user, String namespace) {
        assert (property instanceof GeSchemaProperty);
        if (property.getSandboxStatus() != SandboxStatus.PUBLIC) {
            Vertex vertex = ((GeSchemaProperty) property).getVertex();
            internalPublishVertex(vertex, user, namespace);
        }
    }

    private void internalPublishVertex(Vertex vertex, User user, String namespace) {
        VisibilityJson visibilityJson = BcSchema.VISIBILITY_JSON.getPropertyValue(vertex);
        if (visibilityJson != null && visibilityJson.getWorkspaces().contains(namespace)) {
            visibilityJson = VisibilityJson.removeFromAllWorkspace(visibilityJson);
            final BcVisibility bcVisibility = visibilityTranslator.toVisibility(visibilityJson);

            try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(Priority.NORMAL, user, getAuthorizations(namespace))) {
                ctx.update(vertex, ZonedDateTime.now(), visibilityJson, vertexUpdateCtx -> {
                    ExistingElementMutation<Vertex> mutation = (ExistingElementMutation<Vertex>)vertexUpdateCtx.getMutation();
                    mutation.alterElementVisibility(bcVisibility.getVisibility());
                });
                removeEdge(ctx, namespace, vertex.getId());
            }
        }
    }

    public Concept getConceptById(String id) {
        if(StringUtils.isEmpty(id))
            return null;

        Authorizations authorizations = getAuthorizations();
        Vertex v = graph.getVertex(id, authorizations);
        if(v == null)
            return null;
        else
            return IterableUtils.anyOrDefault(
                    transformConcepts(Collections.singletonList(v), PUBLIC)
            , null);
    }

    public Relationship getRelationshipById(String id) {
        if(StringUtils.isEmpty(id))
            return null;

        Authorizations authorizations = getAuthorizations();
        Vertex v = graph.getVertex(id, authorizations);
        if(v == null)
            return null;
        else
            return IterableUtils.anyOrDefault(
                    transformRelationships(Collections.singletonList(v), PUBLIC)
                    , null);
    }

    public SchemaProperty getPropertyById(String id) {
        if(StringUtils.isEmpty(id))
            return null;

        Authorizations authorizations = getAuthorizations();
        Vertex v = graph.getVertex(id, authorizations);
        if(v == null)
            return null;
        else
            return IterableUtils.anyOrDefault(
                    transformProperties(Collections.singletonList(v), PUBLIC)
                    , null);
    }
}
