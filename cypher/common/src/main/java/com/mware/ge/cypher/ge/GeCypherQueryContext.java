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
package com.mware.ge.cypher.ge;

import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.model.schema.*;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.StreamUtil;
import com.mware.ge.*;
import com.mware.ge.collection.Iterables;
import com.mware.ge.collection.Iterators;
import com.mware.ge.collection.Pair;
import com.mware.ge.collection.RawIterator;
import com.mware.ge.cypher.ProceduresSupport;
import com.mware.ge.cypher.Statement;
import com.mware.ge.cypher.exception.EntityNotFoundException;
import com.mware.ge.cypher.exception.IndexNotFoundKernelException;
import com.mware.ge.cypher.exception.ProcedureException;
import com.mware.ge.cypher.index.IndexReference;
import com.mware.ge.cypher.index.InternalIndexState;
import com.mware.ge.cypher.procedure.exec.Procedures;
import com.mware.ge.cypher.procedure.impl.QualifiedName;
import com.mware.ge.cypher.query.ClientConnectionInfo;
import com.mware.ge.cypher.query.ExecutingQuery;
import com.mware.ge.cypher.query.KernelStatement;
import com.mware.ge.cypher.schema.IndexDescriptorFactory;
import com.mware.ge.cypher.schema.SchemaDescriptor;
import com.mware.ge.cypher.schema.SchemaDescriptorFactory;
import com.mware.ge.cypher.security.SecurityContext;
import com.mware.ge.cypher.values.virtual.GeEdgeBuilderWrappingValue;
import com.mware.ge.cypher.values.virtual.GeEdgeWrappingValue;
import com.mware.ge.cypher.values.virtual.GeVertexMutationWrappingNodeValue;
import com.mware.ge.cypher.values.virtual.GeVertexWrappingNodeValue;
import com.mware.ge.dependencies.DependencyResolver;
import com.mware.ge.io.ResourceTracker;
import com.mware.ge.mutation.*;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.query.builder.GeQueryBuilders;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.StreamUtils;
import com.mware.ge.values.AnyValue;
import com.mware.ge.values.storable.*;
import com.mware.ge.values.virtual.MapValue;
import com.mware.ge.values.virtual.NodeValue;
import com.mware.ge.values.virtual.RelationshipValue;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mware.ge.collection.Pair.pair;
import static java.util.Collections.emptyMap;

public class GeCypherQueryContext {
    private final static BcLogger LOGGER = BcLoggerFactory.getLogger(GeCypherQueryContext.class);
    private final static Pattern META_PATTERN = Pattern.compile("^(.+).meta\\[(.+)\\]$");
    private static final int QUERY_THREADPOOL_SIZE = 20;

    private final String query;
    private final MapValue params;
    private final GraphWithSearchIndex graph;
    private final Authorizations authorizations;
    private final SchemaRepository schemaRepository;
    private final Procedures procedures;
    private WorkQueueRepository workQueueRepository;
    private final String workspaceId;
    private final DependencyResolver dependencyResolver;
    private volatile Map<String, Object> userMetaData;
    private final Statement statement;
    private final ExecutingQuery executingQuery;
    private final SecurityContext securityContext = SecurityContext.AUTH_DISABLED;

    private Concept thingConcept;
    private Relationship topRelationship;
    private Schema schema;
    private Map<String, SchemaProperty> propertiesByName = new HashMap<>();
    private final int esShards;
    private final ExecutorService executor;

    private final Map<String, ElementMutation<? extends Element>> elementBuilders = new HashMap<>();
    private final List<ElementId> deletedElements = new ArrayList<>();

    public GeCypherQueryContext(
            String query,
            MapValue params,
            GraphWithSearchIndex graph,
            Authorizations authorizations,
            SchemaRepository schemaRepository,
            Procedures procedures,
            DependencyResolver dependencyResolver,
            WorkQueueRepository workQueueRepository,
            String workspaceId
    ) {
        this.query = query;
        this.params = params;
        this.graph = graph;
        this.authorizations = authorizations;
        this.schemaRepository = schemaRepository;
        this.procedures = procedures;
        this.dependencyResolver = dependencyResolver;
        this.workQueueRepository = workQueueRepository;
        this.workspaceId = workspaceId;
        this.userMetaData = emptyMap();

        this.statement = new KernelStatement(this);
        this.executingQuery = this.statement.queryRegistration().startQueryExecution(
                ClientConnectionInfo.EMBEDDED_CONNECTION, query, params
        );

        this.thingConcept = schemaRepository.getThingConcept();
        this.topRelationship = schemaRepository.getOrCreateRootRelationship(schemaRepository.getAuthorizations());
        this.schema = schemaRepository.getOntology(workspaceId);
        this.propertiesByName.putAll(this.schema.getPropertiesByName());
        this.esShards = graph.getSearchIndex().getNumShards();
        this.executor = Executors.newFixedThreadPool(QUERY_THREADPOOL_SIZE);
    }

    public GraphWithSearchIndex getGraph() {
        return graph;
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    public SchemaRepository getSchemaRepository() {
        return schemaRepository;
    }

    public Procedures getProcedures() {
        return procedures;
    }

    public String getWorkspaceId() {
        return workspaceId;
    }

    public SecurityContext securityContext() {
        return securityContext;
    }

    public ProceduresSupport procedures() {
        throw new UnsupportedOperationException("not implemented");
    }

    public DependencyResolver getDependencyResolver() {
        return dependencyResolver;
    }

    public ResourceTracker resourceTracker() {
        return statement;
    }

    public RawIterator<Object[], ProcedureException> procedureCallDbms(QualifiedName name, Object[] input, DependencyResolver dependencyResolver, SecurityContext securityContext, ResourceTracker resourceTracker) throws ProcedureException {
        throw new UnsupportedOperationException("not implemented");
    }

    public RawIterator<Object[], ProcedureException> procedureCallDbms(int id, Object[] input, DependencyResolver dependencyResolver, SecurityContext securityContext, ResourceTracker resourceTracker) throws ProcedureException {
        throw new UnsupportedOperationException("not implemented");
    }

    public ExecutingQuery executingQuery() {
        return this.executingQuery;
    }

    public void setMetaData(Map<String, Object> data) {
        this.userMetaData = data;
    }

    public Map<String, Object> getMetaData() {
        return userMetaData;
    }

    public void commit() {
        final Collection<ElementMutation<? extends Element>> values = elementBuilders.values();
        LOGGER.debug("Committing transaction");

        graph.deleteElements(deletedElements.stream(), authorizations);
        Iterable<Element> elements = graph.saveElementMutations(values, authorizations);

        if (!values.isEmpty() && workQueueRepository != null) {
            for (ElementMutation<? extends Element> mutation : values) {
                Element savedElement = null;
                for (Element element : elements) {
                    if (element.getId().equals(mutation.getId())) {
                        savedElement = element;
                        break;
                    }
                }
                if (savedElement != null) {
                    for (Property property : mutation.getProperties()) {
                        workQueueRepository.pushGraphPropertyQueue(
                                savedElement,
                                property.getKey(),
                                property.getName(),
                                null,
                                null,
                                Priority.LOW,
                                ElementOrPropertyStatus.UPDATE,
                                null
                        );
                    }
                }
            }
        }

        Optional<ElementMutation<? extends Element>> aVertexMutation = values.parallelStream().filter(m -> ElementType.VERTEX.equals(m.getElementType())).findFirst();
        if (aVertexMutation.isPresent()) {
            GeStatisticsHolder.nodeByLabelCount.invalidateAll();
            GeStatisticsHolder.nodeAllCount.invalidateAll();
            GeStatisticsHolder.nodesByPropertiesCount.invalidateAll();
            GeStatisticsHolder.nodesByPropertiesDinctinctCount.invalidateAll();
        }

        Optional<ElementMutation<? extends Element>> aEdgeMutation = values.parallelStream().filter(m -> ElementType.EDGE.equals(m.getElementType())).findAny();
        if (aEdgeMutation.isPresent()) {
            GeStatisticsHolder.relByLabelCount.invalidateAll();
        }

        elementBuilders.clear();
        deletedElements.clear();

        graph.flush();
    }

    /**
     * Returns the property map for an element, considering its mutations
     * Note: Only properties with an empty key will be considered
     */
    public Map<String, Value> getElementProperties(String elementId, ElementType elementType) {
        Map<String, Value> result = new HashMap<>();
        Set<Pair<String, String>> propsBeingDeleted = new HashSet<>();
        Map<Pair<String, String>, Value> propsBeingChanged = new HashMap<>();

        if (elementBuilders.containsKey(elementId)) {
            ElementMutation mutation = elementBuilders.get(elementId);
            if (!(mutation instanceof ExistingElementMutation)) {
                // tis is a new element
                mutation.getProperties().forEach(p -> {
                    Property prop = (Property) p;
                    if (StringUtils.isEmpty(prop.getKey())) {
                        result.put(prop.getName(), prop.getValue());
                    }
                });
                return result;
            } else {
                // tis is an existing element being changed
                Iterable<PropertyDeleteMutation> deleteMutations = mutation.getPropertyDeletes();
                Iterable<PropertySoftDeleteMutation> softDeleteMutations = mutation.getPropertySoftDeletes();
                deleteMutations.forEach(dm -> propsBeingDeleted.add(pair(dm.getKey(), dm.getName())));
                softDeleteMutations.forEach(dm -> propsBeingDeleted.add(pair(dm.getKey(), dm.getName())));
                Iterable<Property> props = mutation.getProperties();
                props.forEach(p -> propsBeingChanged.put(pair(p.getKey(), p.getName()), p.getValue()));
            }
        }

        // get the original element as well
        Element e = graph.getElement(ElementId.create(elementType, elementId), FetchHints.PROPERTIES, authorizations);
        if (e == null)
            throw new EntityNotFoundException(ElementType.VERTEX, elementId);

        Map<Pair<String, String>, Value> finalProps = new HashMap<>(propsBeingChanged);

        for (Property p : e.getProperties()) {
            Pair<String, String> propId = pair(p.getKey(), p.getName());
            // remove deleted & soft deleted props
            if (propsBeingDeleted.contains(propId))
                continue;

            if (!propsBeingChanged.containsKey(propId)) {
                // it's not deleted or changed, so add the original property
                finalProps.put(propId, p.getValue());
            }
        }

        finalProps.forEach((propId, value) -> {
            if (StringUtils.isEmpty(propId.first())) {
                result.put(propId.other(), value);
            }
        });

        return result;
    }

    public void setConceptType(String vertexId, String conceptType) {
        if (elementBuilders.containsKey(vertexId)) {
            VertexMutation vm = (VertexMutation) elementBuilders.get(vertexId);
            vm.alterConceptType(conceptType);
        } else {
            VertexMutation vm = (VertexMutation) getOrPrepareMutation(vertexId, ElementType.VERTEX, FetchHints.NONE);
            vm.alterConceptType(conceptType);
        }
    }

    public Value getConceptType(String vertexId) {
        if (elementBuilders.containsKey(vertexId)) {
            VertexMutation vm = (VertexMutation) elementBuilders.get(vertexId);
            if (vm.hasChanges() && vm.getNewConceptType() != null)
                return Values.stringValue(vm.getNewConceptType());
            else
                return Values.stringValue(vm.getConceptType());
        } else {
            Vertex v = graph.getVertex(vertexId, FetchHints.NONE, authorizations);
            if (v == null)
                throw new EntityNotFoundException(ElementType.VERTEX, vertexId);

            return Values.stringValue(v.getConceptType());
        }
    }

    public Value getPropertyValue(String elementId, ElementType elementType, String propertyName) {
        return getElementProperties(elementId, elementType)
                .computeIfAbsent(propertyName, (k) -> Values.NO_VALUE);
    }

    public int vertexGetDegree(String vertexId, Direction direction, String edgeLabel) {
        if (elementBuilders.containsKey(vertexId)) {
            LOGGER.warn("This may need further attention !");
            return 0;
        } else {
            Vertex v = graph.getVertex(vertexId, FetchHints.EDGE_LABELS, authorizations);
            if (v == null)
                throw new EntityNotFoundException(ElementType.VERTEX, vertexId);

            EdgesSummary edgesSummary = v.getEdgesSummary(authorizations);

            switch (direction) {
                case OUT:
                    if (!StringUtils.isEmpty(edgeLabel)) {
                        return edgesSummary.getCountOfEdges(Direction.OUT);
                    } else {
                        if (edgesSummary.getOutEdgeCountsByLabels().containsKey(edgeLabel))
                            return edgesSummary.getOutEdgeCountsByLabels().get(edgeLabel);
                    }
                case IN:
                    if (!StringUtils.isEmpty(edgeLabel)) {
                        return edgesSummary.getCountOfEdges(Direction.IN);
                    } else {
                        if (edgesSummary.getInEdgeCountsByLabels().containsKey(edgeLabel))
                            return edgesSummary.getInEdgeCountsByLabels().get(edgeLabel);
                    }
                case BOTH:
                    if (!StringUtils.isEmpty(edgeLabel)) {
                        return edgesSummary.getCountOfEdges(Direction.BOTH);
                    } else {
                        int totalCount = 0;
                        if (edgesSummary.getInEdgeCountsByLabels().containsKey(edgeLabel)) {
                            totalCount += edgesSummary.getInEdgeCountsByLabels().get(edgeLabel);
                        }
                        if (edgesSummary.getOutEdgeCountsByLabels().containsKey(edgeLabel)) {
                            totalCount += edgesSummary.getOutEdgeCountsByLabels().get(edgeLabel);
                        }
                        return totalCount;
                    }
            }

            throw new IllegalStateException("How did we get here ?");
        }
    }

    public void deleteElement(String elementId, ElementType elementType) {
        if (elementBuilders.containsKey(elementId)) {
            elementBuilders.remove(elementId);
        } else {
            deletedElements.add(ElementId.create(elementType, elementId));
        }
    }

    private PropertyType bcTypeFromNeoValue(String propertyName, Object value) {
        if (!(value instanceof Value))
            return PropertyType.STRING;
        else {
            Value _value = (Value) value;
            if (Values.isArrayValue(_value)) {
                ArrayValue _arayValue = (ArrayValue) _value;
                if (_arayValue.length() > 0) {
                    return bcTypeFromNeoValue(propertyName, _arayValue.value(0));
                } else {
                    LOGGER.warn("Could not determine BigConnect PropertyType for property '" + propertyName
                            + "', because it's an empty Array. Defaulting to STRING");
                    return PropertyType.STRING;
                }
            } else {
                if (_value instanceof IntegralValue) {
                    return PropertyType.INTEGER;
                } else if (Values.isTextValue(_value)) {
                    return PropertyType.STRING;
                } else if (_value instanceof FloatingPointValue) {
                    return PropertyType.DOUBLE;
                } else if (Values.isBooleanValue(_value)) {
                    return PropertyType.BOOLEAN;
                } else if (Values.isTemporalValue(_value)) {
                    return PropertyType.DATE;
                } else if (_value instanceof GeoPointValue) {
                    return PropertyType.GEO_LOCATION;
                } else {
                    LOGGER.warn("Could not determine BigConnect PropertyType for property '" + propertyName + "', defaulting to STRING");
                    return PropertyType.STRING;
                }
            }
        }
    }

    public SchemaProperty getPropertyByName(final String propertyName) {
        return propertiesByName.get(propertyName);
    }

    public SchemaProperty getOrCreateSchemaProperty(final String propertyName, final Object value) {
        return propertiesByName.computeIfAbsent(propertyName, (k) -> createNewProperty(propertyName, workspaceId, bcTypeFromNeoValue(propertyName, value)));
    }

    public void setProperty(String elementId, ElementType elementType, String propertyName, Value value) {
        getOrCreateSchemaProperty(propertyName, value);
        ElementMutation mutation = getOrPrepareMutation(elementId, elementType, FetchHints.PROPERTIES_AND_METADATA);

        Matcher matcher = META_PATTERN.matcher(propertyName);
        if (matcher.matches()) {
            propertyName = matcher.group(1);
            String metaPropertyName = matcher.group(2);

            // the property should be in mutation already
            for (Object propObj : mutation.getProperties()) {
                Property prop = (Property) propObj;
                if (propertyName.equals(prop.getName())) {
                    // doing ((Value)value).asObjectCopy() is not ok, we should treat each value type individually
                    prop.getMetadata().add(metaPropertyName, value, Visibility.EMPTY);
                    return;
                }
            }
            throw new IllegalArgumentException("The provided property metadata refers to a property that was not yet created: " + propertyName);
        }

        // some properties require special handling
        if (BcSchema.TEXT.getPropertyName().equals(propertyName) && value instanceof TextValue) {
            Metadata metadata = Metadata.create();
            BcSchema.TEXT_DESCRIPTION_METADATA.setMetadata(
                    metadata,
                    "Text",
                    Visibility.EMPTY
            );
            BcSchema.MIME_TYPE_METADATA.setMetadata(metadata, "text/plain", Visibility.EMPTY);
            StreamingPropertyValue spv = DefaultStreamingPropertyValue.create(((TextValue) value).stringValue());
            BcSchema.TEXT.addPropertyValue(mutation, "", spv, metadata, Visibility.EMPTY);
        } else if (BcSchema.RAW.getPropertyName().equals(propertyName)) {
            if (value instanceof TextValue) {
                byte[] b = ((TextValue) value).stringValue().getBytes(StandardCharsets.UTF_8);
                StreamingPropertyValue spv = DefaultStreamingPropertyValue.create(new ByteArrayInputStream(b), ByteArray.class);
                spv.searchIndex(false);
                BcSchema.RAW.setProperty(mutation, spv, Visibility.EMPTY);
            } else if (value instanceof ByteArray) {
                byte[] b = ((ByteArray) value).asObjectCopy();
                StreamingPropertyValue spv = DefaultStreamingPropertyValue.create(new ByteArrayInputStream(b), ByteArray.class);
                spv.searchIndex(false);
                BcSchema.RAW.setProperty(mutation, spv, Visibility.EMPTY);
            }
        } else if (value instanceof Value) {
            mutation.setProperty(propertyName, value, Visibility.EMPTY);
        } else {
            throw new IllegalArgumentException("Unknown BigConnect value type: " + value.getClass());
        }
    }

    public void removeProperty(String elementId, ElementType elementType, String propertyName) {
        ElementMutation mutation = getOrPrepareMutation(elementId, elementType, FetchHints.PROPERTIES_AND_METADATA);
        mutation.deleteProperty("", propertyName, Visibility.EMPTY);
    }

    public boolean hasProperty(String elementId, ElementType elementType, String propertyName) {
        return getElementProperties(elementId, elementType).containsKey(propertyName);
    }


    private ElementMutation getOrPrepareMutation(String elementId, ElementType elementType, FetchHints fetchHints) {
        ElementMutation mutation;
        if (elementBuilders.containsKey(elementId)) {
            mutation = elementBuilders.get(elementId);
        } else {
            Element e = graph.getElement(ElementId.create(elementType, elementId), fetchHints, authorizations);
            if (e == null)
                throw new EntityNotFoundException(elementType, elementId);

            mutation = e.prepareMutation();
            elementBuilders.put(elementId, mutation);
        }
        return mutation;
    }

    public NodeValue getVertexById(String id, boolean throwException) {
        if (deletedElements.contains(ElementId.create(ElementType.VERTEX, id)))
            return null;

        if (elementBuilders.containsKey(id)) {
            return new GeVertexMutationWrappingNodeValue((VertexMutation) elementBuilders.get(id), this);
        }

        if (graph.doesVertexExist(id, authorizations)) {
            return new GeVertexWrappingNodeValue(id, graph, authorizations);
        } else {
            if (throwException)
                throw new EntityNotFoundException(ElementType.EDGE, id);

            return null;
        }
    }

    public RelationshipValue getEdgeById(String id, boolean throwException) {
        if (deletedElements.contains(ElementId.create(ElementType.VERTEX, id)))
            return null;

        if (elementBuilders.containsKey(id)) {
            return new GeEdgeBuilderWrappingValue((EdgeMutation) elementBuilders.get(id), this);
        } else {
            if (graph.doesEdgeExist(id, authorizations)) {
                return new GeEdgeWrappingValue(id, this);
            } else {
                if (throwException)
                    throw new EntityNotFoundException(ElementType.EDGE, id);

                return null;
            }
        }
    }

    public ElementBuilder createVertex(String conceptType, Optional<AnyValue> id) {
        ElementBuilder builder;

        if (id.isPresent()) {
            Value value = (Value) id.get();
            Object val = value.asObject();
            builder = graph.prepareVertex(val == null ? null : val.toString(), Visibility.EMPTY, conceptType);
        } else {
            builder = graph.prepareVertex(Visibility.EMPTY, conceptType);
        }

        elementBuilders.put(builder.getId(), builder);
        return builder;
    }

    public ElementBuilder createEdge(String startVertex, String endVertex, String edgeLabel, Optional<AnyValue> id) {
        ElementBuilder builder;
        if (id.isPresent()) {
            Value value = (Value) id.get();
            builder = graph.prepareEdge(value.asObjectCopy().toString(), startVertex, endVertex, edgeLabel, Visibility.EMPTY);
        } else {
            builder = graph.prepareEdge(startVertex, endVertex, edgeLabel, Visibility.EMPTY);
        }

        elementBuilders.put(builder.getId(), builder);
        return builder;
    }

    public Iterator<RelationshipValue> getEdgesForVertex(String vertexId, Direction direction, Optional<String[]> edgeLabels) {
        // search mutations for a relationship
        List<EdgeBuilderBase> edgeBuilders = elementBuilders.values().stream()
                .filter(b -> b instanceof EdgeBuilderBase)
                .map(b -> (EdgeBuilderBase) b)
                .collect(Collectors.toList());

        Stream<EdgeBuilderBase> edgeBuildersStream = edgeBuilders.stream()
                .filter(ebb -> vertexId.equals(ebb.getVertexId(direction)));
        if (edgeLabels.isPresent() && edgeLabels.get().length > 0) {
            edgeBuildersStream = edgeBuildersStream.
                    filter(ebb -> ArrayUtils.contains(edgeLabels.get(), ebb.hasChanges() ? ebb.getNewEdgeLabel() : ebb.getEdgeLabel()));
        }

        List<RelationshipValue> edgesCreated = edgeBuildersStream
                .map(ebb -> (RelationshipValue) new GeEdgeBuilderWrappingValue(ebb, this)).collect(Collectors.toList());

        Vertex v = graph.getVertex(vertexId, FetchHints.EDGE_REFS, authorizations);
        if (v != null) {
            Stream<String> edgesStream = edgeLabels.isPresent() ?
                    StreamUtils.stream(v.getEdgeIds(direction, edgeLabels.get(), authorizations))
                    : StreamUtils.stream(v.getEdgeIds(direction, authorizations));

            edgesCreated.addAll(
                    edgesStream
                            .collect(StreamUtil.unorderedBatches(100, Collectors.toList()))
                            .parallelStream()
                            .map(batch -> graph.getEdges(batch, FetchHints.PROPERTIES, authorizations))
                            .flatMap(StreamUtils::stream)
                            .map(e -> new GeEdgeWrappingValue(e, this))
                            .collect(Collectors.toList())
            );
        }

        return edgesCreated.iterator();
    }

    public Iterator<NodeValue> getVertices() {
        return Iterators.map(vertexId -> {
            if (elementBuilders.containsKey(vertexId))
                return new GeVertexMutationWrappingNodeValue((VertexMutation) elementBuilders.get(vertexId), GeCypherQueryContext.this);
            else
                return new GeVertexWrappingNodeValue(vertexId, authorizations, graph);
        }, graph.getVertexIds(authorizations).iterator());
    }

    public Iterable<NodeValue> getVertices(List<String> ids) {
        return Iterables.map(graph.getVertices(ids, FetchHints.ALL, authorizations), vertex -> {
            if (elementBuilders.containsKey(vertex.getId()))
                return new GeVertexMutationWrappingNodeValue((VertexMutation) elementBuilders.get(vertex.getId()), GeCypherQueryContext.this);
            else
                return new GeVertexWrappingNodeValue(vertex);
        });
    }

    public Iterator<RelationshipValue> getEdges() {
        return Iterators.map(edge -> {
            if (elementBuilders.containsKey(edge.getId()))
                return new GeEdgeBuilderWrappingValue((EdgeBuilderBase) elementBuilders.get(edge.getId()), GeCypherQueryContext.this);
            else
                return new GeEdgeWrappingValue(edge, GeCypherQueryContext.this);
        }, graph.getEdges(authorizations).iterator());
    }

    public Relationship createNewRelationship(String edgeLabel, String workspaceId) {
        SchemaFactory sf = new SchemaFactory(schemaRepository)
                .forNamespace(workspaceId);

        Relationship r = sf.newRelationship()
                .label(edgeLabel)
                .parent(topRelationship)
                .source(thingConcept)
                .target(thingConcept)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), Values.stringValue(edgeLabel))
                .property(SchemaProperties.COLOR.getPropertyName(), Values.stringValue("#000000"))
                .coreConcept(false)
                .save();

        schemaRepository.clearCache();
        return r;
    }


    public Concept createNewConcept(String conceptType, String workspaceId) {
        SchemaFactory sf = new SchemaFactory(schemaRepository)
                .forNamespace(workspaceId);

        Concept c = sf.newConcept()
                .parent(thingConcept)
                .conceptType(conceptType)
                .displayName(conceptType)
                .coreConcept(false)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .save();

        schemaRepository.clearCache();
        return c;
    }

    public SchemaProperty createNewProperty(String propertyName, String workspaceId, PropertyType propertyType) {
        SchemaFactory sf = new SchemaFactory(schemaRepository)
                .forNamespace(workspaceId);

        SchemaFactory.DefaultConceptProperty sp = sf.newConceptProperty()
                .name(propertyName)
                .concepts(thingConcept)
                .displayName(propertyName)
                .type(propertyType)
                .systemProperty(false)
                .userVisible(true)
                .searchable(true)
                .updatable(true)
                .addable(true);

        if (PropertyType.STRING.equals(propertyType)) {
            sp.textIndexHints(TextIndexHint.ALL);
        } else {
            sp.textIndexHints(TextIndexHint.EXACT_MATCH);
        }

        SchemaProperty schemaProperty = sp.save();
        schemaRepository.clearCache();
        return schemaProperty;
    }

    public Iterable<NodeValue> getNodesWithConceptType(String conceptType) {
        final List<Future<List<NodeValue>>> futures = new ArrayList<>();
        final List<NodeValue> result = new ArrayList<>();

        for (int i = 0; i < esShards; i++) {
            final int shard = i;
            futures.add(
                    executor.submit(() -> {
                        try (QueryResultsIterable<String> ids = graph.query(GeQueryBuilders.hasConceptType(conceptType), authorizations)
                                .setShard(String.valueOf(shard))
                                .vertexIds()) {

                            final Iterable<NodeValue> vertices = getVertices(IterableUtils.toList(ids));
                            return IterableUtils.toList(vertices);
                        } catch (IOException ex) {
                            throw new GeException("Could not load Accumulo elements", ex);
                        }
                    })
            );
        }

        futures.forEach(f -> {
            try {
                result.addAll(f.get());
            } catch (InterruptedException | ExecutionException ex) {
                throw new GeException("Interrupted while loading data", ex);
            }
        });

        elementBuilders.values().forEach(m -> {
            if (m instanceof VertexMutation) {
                VertexMutation vm = (VertexMutation) m;
                if (conceptType.equals(vm.getConceptType())) {
                    result.add(new GeVertexWrappingNodeValue(vm.getId(), graph, authorizations));

                }
            }
        });

        return result;
    }

    public Set<String> getIndexablePropertyKeys(String conceptType, String workspaceId) {
        Concept concept = schemaRepository.getConceptByName(conceptType, workspaceId);
        if (concept == null)
            return Collections.emptySet();

        Set<Concept> allConcepts = schemaRepository.getConceptAndAncestors(concept, workspaceId);
        Set<SchemaProperty> allProps = new HashSet<>();
        allConcepts.forEach(c -> allProps.addAll(c.getProperties()));

        return allProps.stream()
                .filter(p -> p.getTextIndexHints() != null && p.getTextIndexHints().length > 0)
                .map(SchemaProperty::getName)
                .collect(Collectors.toSet());
    }


    public Map<String, ElementMutation<? extends Element>> getElementBuilders() {
        return elementBuilders;
    }


    /**
     * Acquire a reference to the index mapping the given {@code label} and {@code properties}.
     *
     * @param label      the index label
     * @param properties the index properties
     * @return the IndexReference, or {@link IndexReference#NO_INDEX} if such an index does not exist.
     */
    public IndexReference index(String label, String... properties) {
        if (label == null)
            return IndexReference.NO_INDEX;

        Set<String> allIndexableProps = getIndexablePropertyKeys(label, workspaceId);
        String[] indexableProps = Arrays.stream(properties)
                .filter(allIndexableProps::contains)
                .toArray(String[]::new);

        SchemaDescriptor schema = SchemaDescriptorFactory.forLabel(label, indexableProps);
        return IndexDescriptorFactory.forSchema(schema);
    }

    /**
     * Retrieves the state of an index
     *
     * @param index the index which state to retrieve
     * @return The state of the provided index
     * @throws IndexNotFoundKernelException if the index was not found in the database
     */
    public InternalIndexState indexGetState(IndexReference index) throws IndexNotFoundKernelException {
        return null;
    }

    /**
     * Returns the failure description of a failed index.
     *
     * @param index the failed index
     * @return The failure message from the index
     * @throws IndexNotFoundKernelException if the index was not found in the database
     */
    public String indexGetFailure(IndexReference index) throws IndexNotFoundKernelException {
        return null;
    }

    public WorkQueueRepository getWorkQueueRepository() {
        return workQueueRepository;
    }
}
