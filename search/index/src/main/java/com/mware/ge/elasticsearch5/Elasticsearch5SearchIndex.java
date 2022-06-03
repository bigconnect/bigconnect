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
package com.mware.ge.elasticsearch5;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.mware.ge.*;
import com.mware.ge.elasticsearch5.bulk.BulkItem;
import com.mware.ge.elasticsearch5.bulk.BulkUpdateService;
import com.mware.ge.elasticsearch5.bulk.BulkUpdateServiceConfiguration;
import com.mware.ge.elasticsearch5.lucene.QueryStringTransformer;
import com.mware.ge.elasticsearch5.sidecar.Sidecar;
import com.mware.ge.metric.GeMetricRegistry;
import com.mware.ge.mutation.*;
import com.mware.ge.property.PropertyDescriptor;
import com.mware.ge.query.*;
import com.mware.ge.search.SearchIndex;
import com.mware.ge.search.SearchIndexWithVertexPropertyCountByValue;
import com.mware.ge.type.*;
import com.mware.ge.util.ExtendedDataMutationUtils;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.IOUtils;
import com.mware.ge.values.storable.*;
import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.mware.ge.elasticsearch5.ElasticsearchPropertyNameInfo.PROPERTY_NAME_PATTERN;
import static com.mware.ge.elasticsearch5.utils.SearchResponseUtils.checkForFailures;
import static com.mware.ge.util.Preconditions.checkNotNull;
import static com.mware.ge.util.StreamUtils.stream;

public class Elasticsearch5SearchIndex implements SearchIndex, SearchIndexWithVertexPropertyCountByValue {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(Elasticsearch5SearchIndex.class);
    protected static final GeLogger MUTATION_LOGGER = GeLoggerFactory.getMutationLogger(SearchIndex.class);

    public static final String ELEMENT_ID_FIELD_NAME = "__elementId";
    public static final String ELEMENT_TYPE_FIELD_NAME = "__elementType";
    public static final String VISIBILITY_FIELD_NAME = "__visibility";
    public static final String HIDDEN_VERTEX_FIELD_NAME = "__hidden";
    public static final String HIDDEN_PROPERTY_FIELD_NAME = "__hidden_property";
    public static final String OUT_VERTEX_ID_FIELD_NAME = "__outVertexId";
    public static final String IN_VERTEX_ID_FIELD_NAME = "__inVertexId";
    public static final String EXTENDED_DATA_TABLE_NAME_FIELD_NAME = "__extendedDataTableName";
    public static final String EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME = "__extendedDataRowId";
    public static final String EXTENDED_DATA_TABLE_COLUMN_VISIBILITIES_FIELD_NAME = "__extendedDataColumnVisibilities";
    public static final String EXACT_MATCH_FIELD_NAME = "exact";
    public static final String EXACT_MATCH_PROPERTY_NAME_SUFFIX = "." + EXACT_MATCH_FIELD_NAME;
    public static final String GEO_PROPERTY_NAME_SUFFIX = "_g";
    public static final String GEO_POINT_PROPERTY_NAME_SUFFIX = "_gp"; // Used for geo hash aggregation of geo points
    public static final String LOWERCASER_NORMALIZER_NAME = "ge_lowercaser";

    public static final int EXACT_MATCH_IGNORE_ABOVE_LIMIT = 10000;
    public static final String FIELDNAME_DOT_REPLACEMENT = "-_-";
    public static final int MAX_RETRIES = 10;

    private final Client client;
    private final ElasticsearchSearchIndexConfiguration config;
    private final Graph graph;
    private Map<String, IndexInfo> indexInfos;
    private final ReadWriteLock indexInfosLock = new ReentrantReadWriteLock();
    private int indexInfosLastSize = -1; // Used to prevent creating a index name array each time
    private String[] indexNamesAsArray;
    private IndexSelectionStrategy indexSelectionStrategy;
    public static final Pattern AGGREGATION_NAME_PATTERN = Pattern.compile("(.*?)_([0-9a-f]+)");
    private final PropertyNameVisibilitiesStore propertyNameVisibilitiesStore;
    private final BulkUpdateService bulkUpdateService;
    private final String geoShapePrecision;
    private final String geoShapeErrorPct;
    private final IdStrategy idStrategy = new IdStrategy();
    private final IndexRefreshTracker indexRefreshTracker;

    private final Elasticsearch5ExceptionHandler exceptionHandler;
    private final boolean refreshIndexOnFlush;
    private boolean bulkIngestEnabled = false;
    private final ExecutorService executorService;
    private Sidecar sidecar;

    public Elasticsearch5SearchIndex(Graph graph, GraphConfiguration config) {
        this.graph = graph;
        this.indexRefreshTracker = new IndexRefreshTracker(graph.getMetricsRegistry());
        this.config = new ElasticsearchSearchIndexConfiguration(graph, config);
        this.indexSelectionStrategy = this.config.getIndexSelectionStrategy();
        this.propertyNameVisibilitiesStore = this.config.createPropertyNameVisibilitiesStore(graph);
        this.client = createClient(this.config);
        this.geoShapePrecision = this.config.getGeoShapePrecision();
        this.geoShapeErrorPct = this.config.getGeoShapeErrorPct();
        this.exceptionHandler = this.config.getExceptionHandler(graph);
        this.refreshIndexOnFlush = this.config.getRefreshIndexOnFlush();
        BulkUpdateServiceConfiguration bulkUpdateServiceConfiguration = new BulkUpdateServiceConfiguration()
                .setPoolSize(this.config.getBulkPoolSize())
                .setBacklogSize(this.config.getBulkBacklogSize())
                .setBulkRequestTimeout(this.config.getBulkRequestTimeout())
                .setMaxBatchSize(this.config.getBulkMaxBatchSize())
                .setMaxBatchSizeInBytes(this.config.getBulkMaxBatchSizeInBytes())
                .setBatchWindowTime(this.config.getBulkBatchWindowTime())
                .setMaxFailCount(this.config.getBulkMaxFailCount())
                .setLogRequestSizeLimit(this.config.getLogRequestSizeLimit());
        this.bulkUpdateService = new BulkUpdateService(this, indexRefreshTracker, bulkUpdateServiceConfiguration);
        this.executorService = Executors.newFixedThreadPool(50);

        setupMaxOpenScrollContextsIfNeeded();
        storePainlessScript("updateFieldsOnDocumentScript", "update-fields-on-document.painless");
    }

    private void setupMaxOpenScrollContextsIfNeeded() {
        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest();
        req.persistentSettings(
                Settings.builder()
                        .put("search.max_open_scroll_context", Integer.MAX_VALUE)
                        .build()
        );
        try {
            client.admin().cluster().updateSettings(req).get();
        } catch (Exception e) {
            LOGGER.warn("Failed to set max_open_scroll_context: "+e.getMessage());
        }
    }

    private void storePainlessScript(String scriptId, String scriptSourceName) {
        try (
                InputStream scriptSource = getClass().getResourceAsStream(scriptSourceName);
                InputStream helperSource = getClass().getResourceAsStream("helper-functions.painless")
        ) {
            String source = IOUtils.toString(helperSource) + " " + IOUtils.toString(scriptSource);
            source = source.replaceAll("\\r?\\n", " ").replaceAll("\"", "\\\\\"");
            client.admin().cluster().preparePutStoredScript()
                    .setId(scriptId)
                    .setContent(new BytesArray("{\"script\": {\"lang\": \"painless\", \"source\": \"" + source + "\"}}"), XContentType.JSON)
                    .get();
        } catch (Exception ex) {
            throw new GeException("Could not load painless script: " + scriptId, ex);
        }
    }

    public PropertyNameVisibilitiesStore getPropertyNameVisibilitiesStore() {
        return propertyNameVisibilitiesStore;
    }

    protected Client createClient(ElasticsearchSearchIndexConfiguration config) {
        if (config.sidecarEnabled()) {
            sidecar = new Sidecar(config);
            return sidecar.getClient();
        } else {
            return createTransportClient(config);
        }
    }

    private static TransportClient createTransportClient(ElasticsearchSearchIndexConfiguration config) {
        Settings settings = tryReadSettingsFromFile(config);
        if (settings == null) {
            Settings.Builder settingsBuilder = Settings.builder();
            if (config.getClusterName() != null) {
                settingsBuilder.put("cluster.name", config.getClusterName());
            }
            for (Map.Entry<String, String> esSetting : config.getEsSettings().entrySet()) {
                settingsBuilder.put(esSetting.getKey(), esSetting.getValue());
            }
            settings = settingsBuilder.build();
        }

        System.setProperty("es.set.netty.runtime.available.processors", "false");
        Collection<Class<? extends Plugin>> plugins = loadTransportClientPlugins(config);
        TransportClient transportClient = new PreBuiltTransportClient(settings, plugins);
        for (String esLocation : config.getEsLocations()) {
            String[] locationSocket = esLocation.split(":");
            String hostname;
            int port;
            if (locationSocket.length == 2) {
                hostname = locationSocket[0];
                port = Integer.parseInt(locationSocket[1]);
            } else if (locationSocket.length == 1) {
                hostname = locationSocket[0];
                port = config.getPort();
            } else {
                throw new GeException("Invalid elastic search location: " + esLocation);
            }
            InetAddress host;
            try {
                host = InetAddress.getByName(hostname);
            } catch (UnknownHostException ex) {
                throw new GeException("Could not resolve host: " + hostname, ex);
            }
            transportClient.addTransportAddress(new TransportAddress(host, port));
        }
        return transportClient;
    }

    @SuppressWarnings("unchecked")
    private static Collection<Class<? extends Plugin>> loadTransportClientPlugins(ElasticsearchSearchIndexConfiguration config) {
        return config.getEsPluginClassNames().stream()
                .map(pluginClassName -> {
                    try {
                        return (Class<? extends Plugin>) Class.forName(pluginClassName);
                    } catch (ClassNotFoundException ex) {
                        throw new GeException("Could not load transport client plugin: " + pluginClassName, ex);
                    }
                })
                .collect(Collectors.toList());
    }

    private static Settings tryReadSettingsFromFile(ElasticsearchSearchIndexConfiguration config) {
        File esConfigFile = config.getEsConfigFile();
        if (esConfigFile == null) {
            return null;
        }
        if (!esConfigFile.exists()) {
            throw new GeException(esConfigFile.getAbsolutePath() + " does not exist");
        }
        try (FileInputStream fileIn = new FileInputStream(esConfigFile)) {
            return Settings.builder().loadFromStream(esConfigFile.getAbsolutePath(), fileIn, false).build();
        } catch (IOException e) {
            throw new GeException("Could not read ES config file: " + esConfigFile.getAbsolutePath(), e);
        }
    }

    public Set<String> getIndexNamesFromElasticsearch() {
        return client.admin().indices().prepareStats().execute().actionGet().getIndices().keySet();
    }

    @Override
    public void clearCache() {
        indexInfosLock.writeLock().lock();
        try {
            this.indexInfos = null;
            this.indexInfosLastSize = -1;
        } finally {
            indexInfosLock.writeLock().unlock();
        }
    }

    private Map<String, IndexInfo> getIndexInfos() {
        indexInfosLock.readLock().lock();
        try {
            if (this.indexInfos != null) {
                return new HashMap<>(this.indexInfos);
            }
        } finally {
            indexInfosLock.readLock().unlock();
        }
        return loadIndexInfos();
    }

    private Map<String, IndexInfo> loadIndexInfos() {
        indexInfosLock.writeLock().lock();
        try {
            this.indexInfos = new HashMap<>();

            Set<String> indices = getIndexNamesFromElasticsearch();
            for (String indexName : indices) {
                if (!indexSelectionStrategy.isIncluded(this, indexName)) {
                    LOGGER.debug("skipping index %s, not in indicesToQuery", indexName);
                    continue;
                }

                LOGGER.debug("loading index info for %s", indexName);
                IndexInfo indexInfo = createIndexInfo(indexName);
                loadExistingMappingIntoIndexInfo(graph, indexInfo, indexName);
                indexInfo.setElementTypeDefined(indexInfo.isPropertyDefined(ELEMENT_TYPE_FIELD_NAME));
                addPropertyNameVisibility(graph, indexInfo, ELEMENT_ID_FIELD_NAME, null);
                addPropertyNameVisibility(graph, indexInfo, ELEMENT_TYPE_FIELD_NAME, null);
                addPropertyNameVisibility(graph, indexInfo, VISIBILITY_FIELD_NAME, null);
                addPropertyNameVisibility(graph, indexInfo, OUT_VERTEX_ID_FIELD_NAME, null);
                addPropertyNameVisibility(graph, indexInfo, IN_VERTEX_ID_FIELD_NAME, null);
                addPropertyNameVisibility(graph, indexInfo, EDGE_LABEL_FIELD_NAME, null);
                addPropertyNameVisibility(graph, indexInfo, CONCEPT_TYPE_FIELD_NAME, null);
                indexInfos.put(indexName, indexInfo);
            }
            return new HashMap<>(this.indexInfos);
        } finally {
            indexInfosLock.writeLock().unlock();
        }
    }

    private void loadExistingMappingIntoIndexInfo(Graph graph, IndexInfo indexInfo, String indexName) {
        indexRefreshTracker.refresh(client, indexName);
        GetMappingsResponse mapping = client.admin().indices().prepareGetMappings(indexName).get();
        for (ObjectCursor<String> mappingIndexName : mapping.getMappings().keys()) {
            ImmutableOpenMap<String, MappingMetaData> typeMappings = mapping.getMappings().get(mappingIndexName.value);
            for (ObjectCursor<String> typeName : typeMappings.keys()) {
                MappingMetaData typeMapping = typeMappings.get(typeName.value);
                Map<String, Map<String, String>> properties = getPropertiesFromTypeMapping(typeMapping);
                if (properties == null) {
                    continue;
                }

                for (Map.Entry<String, Map<String, String>> propertyEntry : properties.entrySet()) {
                    String rawPropertyName = propertyEntry.getKey().replace(FIELDNAME_DOT_REPLACEMENT, ".");
                    loadExistingPropertyMappingIntoIndexInfo(graph, indexInfo, rawPropertyName);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, String>> getPropertiesFromTypeMapping(MappingMetaData typeMapping) {
        return (Map<String, Map<String, String>>) typeMapping.getSourceAsMap().get("properties");
    }

    private void loadExistingPropertyMappingIntoIndexInfo(Graph graph, IndexInfo indexInfo, String rawPropertyName) {
        ElasticsearchPropertyNameInfo p = ElasticsearchPropertyNameInfo.parse(graph, propertyNameVisibilitiesStore, rawPropertyName);
        if (p == null) {
            return;
        }
        addPropertyNameVisibility(graph, indexInfo, rawPropertyName, p.getPropertyVisibility());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void addElement(Graph graph, Element element, Authorizations authorizations) {
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("addElement: %s", element.getId());
        }

        if (!getConfig().isIndexEdges() && element instanceof Edge) {
            return;
        }

        IndexInfo indexInfo = addPropertiesToIndex(graph, element, element.getProperties());
        Map<String, String> source = buildSourceFromElement(graph, element);
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace(
                    "addElement json: %s: %s",
                    element.getId(),
                    Joiner.on(",").withKeyValueSeparator("=").join(source)
            );
        }

        Map<String, Object> fieldsToSet = getPropertiesAsFields(graph, element.getProperties());

        if (element instanceof Edge) {
            Edge edge = (Edge) element;
            fieldsToSet.put(IN_VERTEX_ID_FIELD_NAME, edge.getVertexId(Direction.IN));
            fieldsToSet.put(OUT_VERTEX_ID_FIELD_NAME, edge.getVertexId(Direction.OUT));
            fieldsToSet.put(EDGE_LABEL_FIELD_NAME, edge.getLabel());
        }

        if (element instanceof Vertex) {
            Vertex vertex = (Vertex) element;
            fieldsToSet.put(CONCEPT_TYPE_FIELD_NAME, vertex.getConceptType());
        }

        for (Property property : element.getProperties()) {
            for (Visibility hiddenVisibility : property.getHiddenVisibilities()) {
                String hiddenVisibilityPropertyName = addVisibilityToPropertyName(graph, HIDDEN_PROPERTY_FIELD_NAME, hiddenVisibility);
                if (!isPropertyInIndex(graph, HIDDEN_PROPERTY_FIELD_NAME, hiddenVisibility)) {
                    addPropertyToIndex(graph, indexInfo, hiddenVisibilityPropertyName, hiddenVisibility, BooleanValue.class, false, false, false);
                }
                fieldsToSet.put(hiddenVisibilityPropertyName, true);
            }
        }

        for (Visibility hiddenVisibility : element.getHiddenVisibilities()) {
            String hiddenVisibilityPropertyName = addVisibilityToPropertyName(graph, HIDDEN_VERTEX_FIELD_NAME, hiddenVisibility);
            if (!isPropertyInIndex(graph, HIDDEN_VERTEX_FIELD_NAME, hiddenVisibility)) {
                addPropertyToIndex(graph, indexInfo, hiddenVisibilityPropertyName, hiddenVisibility, BooleanValue.class, false, false, false);
            }
            fieldsToSet.put(hiddenVisibilityPropertyName, true);
        }


        fieldsToSet = fieldsToSet == null ? Collections.emptyMap() : fieldsToSet.entrySet().stream()
                .collect(Collectors.toMap(e -> replaceFieldnameDots(e.getKey()), Map.Entry::getValue));

        bulkUpdateService.addElementUpdate(
                indexInfo.getIndexName(),
                getIdStrategy().getType(),
                getIdStrategy().createElementDocId(element),
                element,
                source,
                fieldsToSet,
                Collections.emptyList(),
                Collections.emptyMap(),
                false
        );

        if (getConfig().isAutoFlush()) {
            flush(graph);
        }
    }

    @Override
    public <TElement extends Element> void updateElement(
            Graph graph,
            ExistingElementMutation<TElement> elementMutation,
            Authorizations authorizations
    ) {
        TElement element = elementMutation.getElement();

        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("updateElement: %s", elementMutation.getId());
        }

        if (!getConfig().isIndexEdges() && elementMutation.getElementType() == ElementType.EDGE) {
            return;
        }

        addMutationPropertiesToIndex(graph, elementMutation);
        addUpdateForMutationToBulk(graph, elementMutation);

        if (elementMutation.getNewElementVisibility() != null &&
                element.getFetchHints().isIncludeExtendedDataTableNames()) {
            ImmutableSet<String> extendedDataTableNames = element.getExtendedDataTableNames();
            if (extendedDataTableNames != null && !extendedDataTableNames.isEmpty()) {
                extendedDataTableNames.forEach(tableName ->
                        alterExtendedDataElementTypeVisibility(
                                graph,
                                elementMutation,
                                element.getExtendedData(tableName),
                                elementMutation.getOldElementVisibility(),
                                elementMutation.getNewElementVisibility()
                        ));
            }
        }

        if (getConfig().isAutoFlush()) {
            flush(graph);
        }
    }

    private <TElement extends Element> void addMutationPropertiesToIndex(Graph graph, ExistingElementMutation<TElement> mutation) {
        TElement element = mutation.getElement();
        IndexInfo indexInfo = addPropertiesToIndex(graph, element, mutation.getProperties());
        mutation.getAlterPropertyVisibilities().stream()
                .filter(p -> p.getExistingVisibility() != null && !p.getExistingVisibility().equals(p.getVisibility()))
                .forEach(p -> {
                    PropertyDefinition propertyDefinition = getPropertyDefinition(graph, p.getName());
                    if (propertyDefinition != null) {
                        try {
                            addPropertyDefinitionToIndex(graph, indexInfo, p.getName(), p.getVisibility(), propertyDefinition);
                        } catch (Exception e) {
                            throw new GeException("Unable to add property to index: " + p, e);
                        }
                    }
                });
        if (mutation.getNewElementVisibility() != null) {
            try {
                String newFieldName = addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, mutation.getNewElementVisibility());
                addPropertyToIndex(graph, indexInfo, newFieldName, element.getVisibility(), TextValue.class, false, false, false);
            } catch (Exception e) {
                throw new GeException("Unable to add new element type visibility to index", e);
            }
        }
    }

    private <TElement extends Element> void addUpdateForMutationToBulk(
            Graph graph,
            ExistingElementMutation<TElement> mutation
    ) {
        TElement element = mutation.getElement();
        Map<String, String> fieldVisibilityChanges = getFieldVisibilityChanges(graph, mutation);
        List<String> fieldsToRemove = getFieldsToRemove(graph, mutation);
        Map<String, Object> fieldsToSet = getFieldsToSet(graph, mutation);

        String documentId = getIdStrategy().createElementDocId(element);
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        addUpdateToBulk(
                indexInfo.getIndexName(),
                documentId,
                element,
                fieldsToSet,
                fieldsToRemove,
                fieldVisibilityChanges
        );
    }

    private <TElement extends Element> Map<String, Object> getFieldsToSet(
            Graph graph,
            ExistingElementMutation<TElement> mutation
    ) {
        TElement element = mutation.getElement();
        Map<String, Object> fieldsToSet = new HashMap<>();
        mutation.getProperties().forEach(p ->
                addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet));
        mutation.getPropertyDeletes().forEach(p ->
                addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet));
        mutation.getPropertySoftDeletes().forEach(p ->
                addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet));

        if (mutation instanceof VertexMutation) {
            VertexMutation vertexMutation = (VertexMutation) mutation;
            if (vertexMutation.getNewConceptType() != null)
                fieldsToSet.put(CONCEPT_TYPE_FIELD_NAME, vertexMutation.getNewConceptType());
        }

        if (mutation instanceof EdgeMutation) {
            EdgeMutation edgeMutation = (EdgeMutation) mutation;
            if (edgeMutation.getNewEdgeLabel() != null) {
                fieldsToSet.put(EDGE_LABEL_FIELD_NAME, edgeMutation.getNewEdgeLabel());
            }
        }

        return fieldsToSet;
    }

    private <TElement extends Element> List<String> getFieldsToRemove(Graph graph, ExistingElementMutation<TElement> mutation) {
        List<String> fieldsToRemove = new ArrayList<>();
        mutation.getPropertyDeletes().forEach(p -> fieldsToRemove.addAll(getFieldsToRemove(graph, p.getName(), p.getVisibility())));
        mutation.getPropertySoftDeletes().forEach(p -> fieldsToRemove.addAll(getFieldsToRemove(graph, p.getName(), p.getVisibility())));
        return fieldsToRemove;
    }

    private List<String> getFieldsToRemove(Graph graph, String name, Visibility visibility) {
        List<String> fieldsToRemove = new ArrayList<>();
        String propertyName = addVisibilityToPropertyName(graph, name, visibility);
        fieldsToRemove.add(propertyName);

        PropertyDefinition propertyDefinition = getPropertyDefinition(graph, name);
        if (GeoShapeValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
            fieldsToRemove.add(propertyName + GEO_PROPERTY_NAME_SUFFIX);

            if (GeoPointValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
                fieldsToRemove.add(propertyName + GEO_POINT_PROPERTY_NAME_SUFFIX);
            }
        }
        return fieldsToRemove;
    }

    private <TElement extends Element> Map<String, String> getFieldVisibilityChanges(Graph graph, ExistingElementMutation<TElement> mutation) {
        Map<String, String> fieldVisibilityChanges = new HashMap<>();

        mutation.getAlterPropertyVisibilities().stream()
                .filter(p -> p.getExistingVisibility() != null && !p.getExistingVisibility().equals(p.getVisibility()))
                .forEach(p -> {
                    String oldFieldName = addVisibilityToPropertyName(graph, p.getName(), p.getExistingVisibility());
                    String newFieldName = addVisibilityToPropertyName(graph, p.getName(), p.getVisibility());
                    fieldVisibilityChanges.put(oldFieldName, newFieldName);

                    PropertyDefinition propertyDefinition = getPropertyDefinition(graph, p.getName());
                    if (GeoShapeValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
                        fieldVisibilityChanges.put(oldFieldName + GEO_PROPERTY_NAME_SUFFIX, newFieldName + GEO_PROPERTY_NAME_SUFFIX);
                        if (GeoPointValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
                            fieldVisibilityChanges.put(oldFieldName + GEO_POINT_PROPERTY_NAME_SUFFIX, newFieldName + GEO_POINT_PROPERTY_NAME_SUFFIX);
                        }
                    }
                });

        if (mutation.getNewElementVisibility() != null) {
            String oldFieldName = addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, mutation.getOldElementVisibility());
            String newFieldName = addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, mutation.getNewElementVisibility());
            fieldVisibilityChanges.put(oldFieldName, newFieldName);
        }
        return fieldVisibilityChanges;
    }

    private void addExistingValuesToFieldMap(Graph graph, Element element, String propertyName, Visibility propertyVisibility, Map<String, Object> fieldsToSet) {
        Iterable<Property> properties = stream(element.getProperties(propertyName))
                .filter(p -> p.getVisibility().equals(propertyVisibility))
                .collect(Collectors.toList());
        Map<String, Object> remainingProperties = getPropertiesAsFields(graph, properties);
        for (Map.Entry<String, Object> remainingPropertyEntry : remainingProperties.entrySet()) {
            String remainingField = remainingPropertyEntry.getKey();
            Object remainingValue = remainingPropertyEntry.getValue();
            if (remainingValue instanceof List) {
                for (Object v : ((List) remainingValue)) {
                    addPropertyValueToPropertiesMap(fieldsToSet, remainingField, v);
                }
            } else {
                addPropertyValueToPropertiesMap(fieldsToSet, remainingField, remainingValue);
            }
        }

        if (getConfig().isAutoFlush()) {
            flush(graph);
        }
    }

    @Override
    public void addElementExtendedData(
            Graph graph,
            ElementLocation elementLocation,
            Iterable<ExtendedDataMutation> extendedData,
            Authorizations authorizations
    ) {
        Map<String, Map<String, ExtendedDataMutationUtils.Mutations>> byTableThenRowId = ExtendedDataMutationUtils.getByTableThenRowId(
                extendedData,
                null
        );

        for (Map.Entry<String, Map<String, ExtendedDataMutationUtils.Mutations>> byTableThenRowIdEntry : byTableThenRowId.entrySet()) {
            String tableName = byTableThenRowIdEntry.getKey();
            Map<String, ExtendedDataMutationUtils.Mutations> byRow = byTableThenRowIdEntry.getValue();
            for (Map.Entry<String, ExtendedDataMutationUtils.Mutations> byRowEntry : byRow.entrySet()) {
                String rowId = byRowEntry.getKey();
                ExtendedDataMutationUtils.Mutations mutations = byRowEntry.getValue();
                addElementExtendedData(
                        graph,
                        new ExtendedDataRowId(elementLocation.getElementType(), elementLocation.getId(), tableName, rowId),
                        elementLocation,
                        mutations.getExtendedData()
                );
            }
        }
    }

    @Override
    public void deleteExtendedData(Graph graph, ExtendedDataRowId rowId, Authorizations authorizations) {
        String indexName = getExtendedDataIndexName(rowId);
        String docId = getIdStrategy().createExtendedDataDocId(rowId);
        bulkUpdateService.addDelete(
                indexName,
                getIdStrategy().getType(),
                docId,
                ElementId.create(rowId.getElementType(), rowId.getElementId())
        );
    }

    @Override
    public void deleteExtendedData(
            Graph graph,
            ElementLocation elementLocation,
            String tableName,
            String row,
            String columnName,
            String key,
            Visibility visibility,
            Authorizations authorizations
    ) {
        String extendedDataDocId = getIdStrategy().createExtendedDataDocId(elementLocation, tableName, row);
        String fieldName = addVisibilityToPropertyName(graph, columnName, visibility);
        String indexName = getExtendedDataIndexName(elementLocation, tableName, row);
        removeFieldsFromDocument(
                graph,
                indexName,
                elementLocation,
                extendedDataDocId,
                Lists.newArrayList(fieldName, fieldName + "_e")
        );
    }

    private void addElementExtendedData(
            Graph graph,
            ExtendedDataRowId extendedDataRowId,
            ElementLocation sourceElementLocation,
            Iterable<ExtendedDataMutation> extendedData
    ) {
        if (MUTATION_LOGGER.isTraceEnabled()) {
            MUTATION_LOGGER.trace("addElementExtendedData: %s", extendedDataRowId);
        }

        addExtendedDataUpdateToBulk(
                graph,
                extendedDataRowId,
                sourceElementLocation,
                extendedData
        );

        if (getConfig().isAutoFlush()) {
            flush(graph);
        }
    }

    public <T extends Element> void alterExtendedDataElementTypeVisibility(
            Graph graph,
            ElementMutation<T> elementMutation,
            Iterable<ExtendedDataRow> rows,
            Visibility oldVisibility,
            Visibility newVisibility
    ) {
        for (ExtendedDataRow row : rows) {
            String tableName = ((TextValue) row.getPropertyValue(ExtendedDataRow.TABLE_NAME)).stringValue();
            String rowId = ((TextValue) row.getPropertyValue(ExtendedDataRow.ROW_ID)).stringValue();
            String extendedDataDocId = getIdStrategy().createExtendedDataDocId(elementMutation, tableName, rowId);

            List<ExtendedDataMutation> columns = stream(row.getProperties())
                    .map(property -> new ExtendedDataMutation(
                            tableName,
                            rowId,
                            property.getName(),
                            property.getKey(),
                            property.getValue(),
                            property.getTimestamp(),
                            property.getVisibility()
                    )).collect(Collectors.toList());

            IndexInfo indexInfo = addExtendedDataColumnsToIndex(graph, elementMutation, tableName, rowId, columns);

            String oldElementTypeVisibilityPropertyName = addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, oldVisibility);
            String newElementTypeVisibilityPropertyName = addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, newVisibility);
            Map<String, String> fieldsToRename = Collections.singletonMap(oldElementTypeVisibilityPropertyName, newElementTypeVisibilityPropertyName);

            bulkUpdateService.addExtendedDataUpdate(
                    indexInfo.getIndexName(),
                    getIdStrategy().getType(),
                    extendedDataDocId,
                    row.getId(),
                    elementMutation,
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    fieldsToRename,
                    true
            );
        }
    }

    @Override
    public void addExtendedData(
            Graph graph,
            ElementLocation sourceElementLocation,
            Iterable<ExtendedDataRow> extendedDatas,
            Authorizations authorizations
    ) {
        Map<ElementType, Map<String, List<ExtendedDataRow>>> rowsByElementTypeAndId = mapExtendedDatasByElementTypeByElementId(extendedDatas);
        rowsByElementTypeAndId.forEach((elementType, elements) -> {
            elements.forEach((elementId, rows) -> {
                rows.forEach(row -> {
                    String tableName = ((TextValue) row.getPropertyValue(ExtendedDataRow.TABLE_NAME)).stringValue();
                    String rowId = ((TextValue) row.getPropertyValue(ExtendedDataRow.ROW_ID)).stringValue();
                    List<ExtendedDataMutation> columns = stream(row.getProperties())
                            .map(property -> new ExtendedDataMutation(
                                    tableName,
                                    rowId,
                                    property.getName(),
                                    property.getKey(),
                                    property.getValue(),
                                    property.getTimestamp(),
                                    property.getVisibility()
                            )).collect(Collectors.toList());
                    addExtendedDataUpdateToBulk(
                            graph,
                            new ExtendedDataRowId(
                                    sourceElementLocation.getElementType(),
                                    sourceElementLocation.getId(),
                                    tableName,
                                    rowId
                            ),
                            sourceElementLocation,
                            columns
                    );
                });
            });
        });
    }

    private void addExtendedDataUpdateToBulk(
            Graph graph,
            ExtendedDataRowId extendedDataRowId,
            ElementLocation sourceElementLocation,
            Iterable<ExtendedDataMutation> extendedData
    ) {
        IndexInfo indexInfo = addExtendedDataColumnsToIndex(graph, sourceElementLocation, extendedDataRowId.getTableName(), extendedDataRowId.getRowId(), extendedData);
        String extendedDataDocId = getIdStrategy().createExtendedDataDocId(sourceElementLocation, extendedDataRowId.getTableName(), extendedDataRowId.getRowId());

        Map<String, Object> fieldsToSet = getExtendedDataColumnsAsFields(graph, extendedData).entrySet().stream()
                .collect(Collectors.toMap(e -> replaceFieldnameDots(e.getKey()), Map.Entry::getValue));

        Map<String, String> source = buildSourceForExtendedDataUpsert(graph, sourceElementLocation, extendedDataRowId.getTableName(), extendedDataRowId.getRowId());
        if (MUTATION_LOGGER.isTraceEnabled()) {
            String fieldsDebug = Joiner.on(", ").withKeyValueSeparator(": ").join(fieldsToSet);
            MUTATION_LOGGER.trace(
                    "addElementExtendedData json: %s: %s {%s}",
                    extendedDataRowId,
                    Joiner.on(",").withKeyValueSeparator("=").join(source),
                    fieldsDebug
            );
        }

        bulkUpdateService.addExtendedDataUpdate(
                indexInfo.getIndexName(),
                getIdStrategy().getType(),
                extendedDataDocId,
                extendedDataRowId,
                sourceElementLocation,
                source,
                fieldsToSet,
                Collections.emptyList(),
                Collections.emptyMap(),
                false
        );
    }

    private Map<String, String> buildSourceForExtendedDataUpsert(
            Graph graph,
            ElementLocation elementLocation,
            String tableName,
            String rowId
    ) {
        Map<String, String> source = new HashMap<>();

        String elementTypeString = ElasticsearchDocumentType.getExtendedDataDocumentTypeFromElementType(
                elementLocation.getElementType()
        ).getKey();

        source.put(ELEMENT_ID_FIELD_NAME, elementLocation.getId());
        source.put(ELEMENT_TYPE_FIELD_NAME, elementTypeString);

        String elementTypeVisibilityPropertyName = addElementTypeVisibilityPropertyToExtendedDataIndex(
                graph,
                elementLocation,
                tableName,
                rowId
        );

        source.put(elementTypeVisibilityPropertyName, elementTypeString);
        source.put(EXTENDED_DATA_TABLE_NAME_FIELD_NAME, tableName);
        source.put(EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME, rowId);

        if (elementLocation.getElementType() == ElementType.EDGE) {
            if (!(elementLocation instanceof EdgeElementLocation)) {
                throw new GeException(String.format(
                        "element location (%s) has type edge but does not implement %s",
                        elementLocation.getClass().getName(),
                        EdgeElementLocation.class.getName()
                ));
            }
            EdgeElementLocation edgeElementLocation = (EdgeElementLocation) elementLocation;
            source.put(IN_VERTEX_ID_FIELD_NAME, edgeElementLocation.getVertexId(Direction.IN));
            source.put(OUT_VERTEX_ID_FIELD_NAME, edgeElementLocation.getVertexId(Direction.OUT));
            source.put(EDGE_LABEL_FIELD_NAME, edgeElementLocation.getLabel());
        } else if (elementLocation.getElementType() == ElementType.VERTEX) {
            if (!(elementLocation instanceof VertexElementLocation)) {
                throw new GeException(String.format(
                        "element location (%s) has type vertex but does not implement %s",
                        elementLocation.getClass().getName(),
                        VertexElementLocation.class.getName()
                ));
            }
            VertexElementLocation vertexElementLocation = (VertexElementLocation) elementLocation;
            source.put(CONCEPT_TYPE_FIELD_NAME, vertexElementLocation.getConceptType());
        }

        return source;
    }

    private Map<String, Object> getExtendedDataColumnsAsFields(Graph graph, Iterable<ExtendedDataMutation> columns) {
        Map<String, Object> fieldsMap = new HashMap<>();
        List<ExtendedDataMutation> streamingColumns = new ArrayList<>();
        for (ExtendedDataMutation column : columns) {
            if (column.getValue() != null && shouldIgnoreType(column.getValue().getClass())) {
                continue;
            }

            if (column.getValue() instanceof StreamingPropertyValue) {
                StreamingPropertyValue spv = (StreamingPropertyValue) column.getValue();
                if (isStreamingPropertyValueIndexable(graph, column.getColumnName(), spv)) {
                    streamingColumns.add(column);
                }
            } else {
                addExtendedDataColumnToFieldMap(graph, column, column.getValue(), fieldsMap);
            }
        }
        addStreamingExtendedDataColumnsValuesToMap(graph, streamingColumns, fieldsMap);
        return fieldsMap;
    }

    private void addStreamingExtendedDataColumnsValuesToMap(Graph graph, List<ExtendedDataMutation> columns, Map<String, Object> fieldsMap) {
        List<StreamingPropertyValue> streamingPropertyValues = columns.stream()
                .map((column) -> {
                    if (!(column.getValue() instanceof StreamingPropertyValue)) {
                        throw new GeException("column with a value that is not a StreamingPropertyValue passed to addStreamingPropertyValuesToFieldMap");
                    }
                    return (StreamingPropertyValue) column.getValue();
                })
                .collect(Collectors.toList());

        List<InputStream> inputStreams = graph.getStreamingPropertyValueInputStreams(streamingPropertyValues);
        for (int i = 0; i < columns.size(); i++) {
            try {
                String propertyValue = IOUtils.toString(inputStreams.get(i));
                addExtendedDataColumnToFieldMap(graph, columns.get(i), new StreamingPropertyString(propertyValue), fieldsMap);
            } catch (IOException ex) {
                throw new GeException("could not convert streaming property to string", ex);
            }
        }
    }

    private void addExtendedDataColumnToFieldMap(Graph graph, ExtendedDataMutation column, Object value, Map<String, Object> fieldsMap) {
        String propertyName = addVisibilityToExtendedDataColumnName(graph, column);
        addValuesToFieldMap(graph, fieldsMap, propertyName, value);
    }

    private Map<ElementType, Map<String, List<ExtendedDataRow>>> mapExtendedDatasByElementTypeByElementId(Iterable<ExtendedDataRow> extendedData) {
        Map<ElementType, Map<String, List<ExtendedDataRow>>> rowsByElementTypeByElementId = new HashMap<>();
        extendedData.forEach(row -> {
            ExtendedDataRowId rowId = row.getId();
            Map<String, List<ExtendedDataRow>> elementTypeData = rowsByElementTypeByElementId.computeIfAbsent(rowId.getElementType(), key -> new HashMap<>());
            List<ExtendedDataRow> elementExtendedData = elementTypeData.computeIfAbsent(rowId.getElementId(), key -> new ArrayList<>());
            elementExtendedData.add(row);
        });
        return rowsByElementTypeByElementId;
    }

    @Override
    public <T extends Element> void alterElementVisibility(
            Graph graph,
            ExistingElementMutation<T> elementMutation,
            Visibility oldVisibility,
            Visibility newVisibility,
            Authorizations authorizations
    ) {
        // Remove old element field name
        String oldFieldName = addVisibilityToPropertyName(graph, ELEMENT_TYPE_FIELD_NAME, oldVisibility);
        removeFieldsFromDocument(graph, elementMutation, oldFieldName);

        addElement(graph, elementMutation.getElement(), authorizations);
    }

    private Map<String, String> buildSourceFromElement(Graph graph, Element element) {
        Map<String, String> source = new HashMap<>();

        String elementTypeVisibilityPropertyName = addElementTypeVisibilityPropertyToIndex(graph, element);

        source.put(ELEMENT_ID_FIELD_NAME, element.getId());
        source.put(ELEMENT_TYPE_FIELD_NAME, getElementTypeValueFromElement(element));
        if (element instanceof Vertex) {
            Vertex vertex = (Vertex) element;
            source.put(elementTypeVisibilityPropertyName, ElasticsearchDocumentType.VERTEX.getKey());
            source.put(CONCEPT_TYPE_FIELD_NAME, vertex.getConceptType());
        } else if (element instanceof Edge) {
            Edge edge = (Edge) element;
            source.put(elementTypeVisibilityPropertyName, ElasticsearchDocumentType.EDGE.getKey());
            source.put(IN_VERTEX_ID_FIELD_NAME, edge.getVertexId(Direction.IN));
            source.put(OUT_VERTEX_ID_FIELD_NAME, edge.getVertexId(Direction.OUT));
            source.put(EDGE_LABEL_FIELD_NAME, edge.getLabel());
        } else {
            throw new GeException("Unexpected element type " + element.getClass().getName());
        }

        return source;
    }

    @Override
    public void markElementHidden(Graph graph, Element element, Visibility visibility, Authorizations authorizations) {
        String hiddenVisibilityPropertyName = addVisibilityToPropertyName(graph, HIDDEN_VERTEX_FIELD_NAME, visibility);
        String indexName = getIndexName(element);

        if (!isPropertyInIndex(graph, HIDDEN_VERTEX_FIELD_NAME, visibility)) {
            IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
            addPropertyToIndex(graph, indexInfo, hiddenVisibilityPropertyName, visibility, BooleanValue.class, false, false, false);
        }

        Map<String, Object> fieldsToSet = new HashMap<>();
        fieldsToSet.put(hiddenVisibilityPropertyName, true);

        bulkUpdateService.addElementUpdate(
                indexName,
                getIdStrategy().getType(),
                getIdStrategy().createElementDocId(element),
                element,
                Collections.emptyMap(),
                fieldsToSet,
                Collections.emptyList(),
                Collections.emptyMap(),
                true
        );
    }

    @Override
    public void markElementVisible(
            Graph graph,
            ElementLocation elementLocation,
            Visibility visibility,
            Authorizations authorizations
    ) {
        String hiddenVisibilityPropertyName = addVisibilityToPropertyName(graph, HIDDEN_VERTEX_FIELD_NAME, visibility);
        if (isPropertyInIndex(graph, HIDDEN_VERTEX_FIELD_NAME, visibility)) {
            removeFieldsFromDocument(graph, elementLocation, hiddenVisibilityPropertyName);
        }
    }

    @Override
    public void markPropertyHidden(
            Graph graph,
            ElementLocation elementLocation,
            Property property,
            Visibility visibility,
            Authorizations authorizations
    ) {
        String hiddenVisibilityPropertyName = addVisibilityToPropertyName(graph, HIDDEN_PROPERTY_FIELD_NAME, visibility);
        String indexName = getIndexName(elementLocation);

        if (!isPropertyInIndex(graph, HIDDEN_PROPERTY_FIELD_NAME, visibility)) {
            IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
            addPropertyToIndex(graph, indexInfo, hiddenVisibilityPropertyName, visibility, BooleanValue.class, false, false, false);
        }

        Map<String, Object> fieldsToSet = new HashMap<>();
        fieldsToSet.put(hiddenVisibilityPropertyName, true);
        bulkUpdateService.addElementUpdate(
                indexName,
                getIdStrategy().getType(),
                getIdStrategy().createElementDocId(elementLocation),
                elementLocation,
                Collections.emptyMap(),
                fieldsToSet,
                Collections.emptyList(),
                Collections.emptyMap(),
                true
        );
    }

    @Override
    public void markPropertyVisible(
            Graph graph,
            ElementLocation elementLocation,
            Property property,
            Visibility visibility,
            Authorizations authorizations
    ) {
        String hiddenVisibilityPropertyName = addVisibilityToPropertyName(graph, HIDDEN_PROPERTY_FIELD_NAME, visibility);
        if (isPropertyInIndex(graph, HIDDEN_PROPERTY_FIELD_NAME, visibility)) {
            removeFieldsFromDocument(graph, elementLocation, hiddenVisibilityPropertyName);
        }
    }

    private String getElementTypeValueFromElement(Element element) {
        if (element instanceof Vertex) {
            return ElasticsearchDocumentType.VERTEX.getKey();
        }
        if (element instanceof Edge) {
            return ElasticsearchDocumentType.EDGE.getKey();
        }
        throw new GeException("Unhandled element type: " + element.getClass().getName());
    }

    protected void addTypeToMapping(XContentBuilder mapping, String propertyName, Class<? extends Value> dataType, boolean analyzed, boolean exact, boolean sortable) throws IOException {
        if (TextValue.class.isAssignableFrom(dataType) || TextArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'string' type for %s", propertyName);
            if (analyzed || exact || sortable) {
                mapping.field("type", "text");
                if (!analyzed) {
                    mapping.field("index", "false");
                } else {
                    mapping.field("fielddata", true);
                }

                if (exact || sortable) {
                    mapping.startObject("fields");
                    mapping.startObject(EXACT_MATCH_FIELD_NAME)
                            .field("type", "keyword")
                            .field("ignore_above", EXACT_MATCH_IGNORE_ABOVE_LIMIT)
                            .field("normalizer", LOWERCASER_NORMALIZER_NAME)
                            .endObject();
                    mapping.endObject();
                }
            } else {
                mapping.field("type", "keyword");
                mapping.field("ignore_above", EXACT_MATCH_IGNORE_ABOVE_LIMIT);
                mapping.field("normalizer", LOWERCASER_NORMALIZER_NAME);
            }
        } else if (FloatValue.class.isAssignableFrom(dataType) || FloatArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'float' type for %s", propertyName);
            mapping.field("type", "float");
        } else if (DoubleValue.class.isAssignableFrom(dataType) || DoubleArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'double' type for %s", propertyName);
            mapping.field("type", "double");
        } else if (ByteValue.class.isAssignableFrom(dataType) || ByteArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'byte' type for %s", propertyName);
            mapping.field("type", "byte");
        } else if (ShortValue.class.isAssignableFrom(dataType) || ShortArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'short' type for %s", propertyName);
            mapping.field("type", "short");
        } else if (IntValue.class.isAssignableFrom(dataType) || IntArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'integer' type for %s", propertyName);
            mapping.field("type", "integer");
        } else if (LongValue.class.isAssignableFrom(dataType) || LongArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'long' type for %s", propertyName);
            mapping.field("type", "long");
        } else if (DateTimeValue.class.isAssignableFrom(dataType) || DateTimeArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'date_nanos' type for %s", propertyName);
            mapping.field("type", "date_nanos");
            mapping.field("format", "epoch_millis");
        } else if (DateValue.class.isAssignableFrom(dataType) || DateArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'date' type for %s", propertyName);
            mapping.field("type", "date");
            mapping.field("format", "epoch_millis");
        } else if (TimeValue.class.isAssignableFrom(dataType) || TimeArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'date' type for %s", propertyName);
            mapping.field("type", "date_nanos");
            mapping.field("format", "basic_time");
        } else if (LocalDateTimeValue.class.isAssignableFrom(dataType) || LocalDateTimeArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'date' type for %s", propertyName);
            mapping.field("type", "date_nanos");
        } else if (LocalTimeValue.class.isAssignableFrom(dataType) || LocalTimeArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'date' type for %s", propertyName);
            mapping.field("type", "date_nanos");
            mapping.field("format", "basic_time");
        } else if (DurationValue.class.isAssignableFrom(dataType) || DurationArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'date' type for %s", propertyName);
            mapping.field("type", "keyword");
        } else if (BooleanValue.class.isAssignableFrom(dataType) || BooleanArray.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'boolean' type for %s", propertyName);
            mapping.field("type", "boolean");
        } else if (GeoPointValue.class.isAssignableFrom(dataType) && exact) {
            // ES5 doesn't support geo hash aggregations for shapes, so if this is a point marked for EXACT_MATCH
            // define it as a geo_point instead of a geo_shape. Points end up with 3 fields in the index for this
            // reason. This one for aggregating as well as the "_g" and description fields that all geoshapes get.
            LOGGER.debug("Registering 'geo_point' type for %s", propertyName);
            mapping.field("type", "geo_point");
        } else if (GeoShapeValue.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering 'geo_shape' type for %s", propertyName);
            mapping.field("type", "geo_shape");
            if (GeoPointValue.class.isAssignableFrom(dataType)) {
                mapping.field("points_only", "true");
            }
            mapping.field("tree", "quadtree");
            mapping.field("precision", geoShapePrecision);
            mapping.field("distance_error_pct", geoShapeErrorPct);
        } else {
            throw new GeException("Unexpected value type for property \"" + propertyName + "\": " + dataType.getName());
        }
    }

    protected Object convertValueForIndexing(String propertyName, Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof Value) {
            Value value = (Value) obj;

            if (value instanceof TextValue)
                return ((TextValue) value).stringValue();
            else if (value instanceof TextArray) {
                if (value instanceof StringArray) {
                    String[] arr = (String[]) value.asObjectCopy();
                    return Arrays.stream(arr).parallel().toArray(String[]::new);
                } else if (value instanceof CharArray) {
                    return ArrayUtils.toObject((char[]) value.asObjectCopy());
                }
            } else if (value instanceof NumberValue)
                return ((NumberValue) value).asObjectCopy();
            else if (value instanceof NumberArray) {
                if (value instanceof ByteArray)
                    return ArrayUtils.toObject((byte[]) value.asObjectCopy());
                else if (value instanceof ShortArray)
                    return ArrayUtils.toObject((short[]) value.asObjectCopy());
                else if (value instanceof IntArray)
                    return ArrayUtils.toObject((int[]) value.asObjectCopy());
                else if (value instanceof LongArray)
                    return ArrayUtils.toObject((long[]) value.asObjectCopy());
                else if (value instanceof FloatArray)
                    return ArrayUtils.toObject((float[]) value.asObjectCopy());
                else if (value instanceof DoubleArray)
                    return ArrayUtils.toObject((double[]) value.asObjectCopy());
                else
                    throw new IllegalArgumentException("Unknown numeric array type: " + value.getClass());
            } else if (value instanceof BooleanValue)
                return ((BooleanValue) value).booleanValue();
            else if (value instanceof BooleanArray) {
                return ArrayUtils.toObject((boolean[]) value.asObjectCopy());
            } else if (value instanceof DateValue) {
                return ((LocalDate) value.asObjectCopy()).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            } else if (value instanceof DateArray) {
                LocalDate[] arr = (LocalDate[]) value.asObjectCopy();
                return Arrays.stream(arr).parallel()
                        .mapToLong(d -> d.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli())
                        .toArray();
            } else if (value instanceof DateTimeValue) {
                ZonedDateTime dt = (ZonedDateTime) value.asObjectCopy();
                return dt.withZoneSameLocal(ZoneOffset.UTC).toInstant().toEpochMilli();
            } else if (value instanceof DateTimeArray) {
                ZonedDateTime[] arr = (ZonedDateTime[]) value.asObjectCopy();
                return Arrays.stream(arr).parallel()
                        .mapToLong(d -> d.withZoneSameLocal(ZoneOffset.UTC).toInstant().toEpochMilli())
                        .toArray();
            } else if (value instanceof LocalDateTimeValue) {
                LocalDateTime dt = (LocalDateTime) value.asObjectCopy();
                return dt.toInstant(ZoneOffset.UTC).toEpochMilli();
            } else if (value instanceof LocalDateTimeArray) {
                LocalDateTime[] arr = (LocalDateTime[]) value.asObjectCopy();
                return Arrays.stream(arr).parallel()
                        .mapToLong(d -> d.toInstant(ZoneOffset.UTC).toEpochMilli())
                        .toArray();
            } else if (value instanceof TimeValue) {
                OffsetTime t = (OffsetTime) value.asObjectCopy();
                return t.format(DateTimeFormatter.ofPattern("HHmmss.SSSZ"));
            } else if (value instanceof TimeArray) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HHmmss.SSSZ");
                OffsetTime[] arr = (OffsetTime[]) value.asObjectCopy();
                return Arrays.stream(arr).parallel()
                        .map(t -> t.format(formatter))
                        .toArray(String[]::new);
            } else if (value instanceof LocalTimeValue) {
                LocalTime t = (LocalTime) value.asObjectCopy();
                return t.format(DateTimeFormatter.ofPattern("HHmmss.SSS")) + "Z";
            } else if (value instanceof LocalTimeArray) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HHmmss.SSS");
                LocalTime[] arr = (LocalTime[]) value.asObjectCopy();
                return Arrays.stream(arr).parallel()
                        .map(t -> t.format(formatter) + "Z")
                        .toArray(String[]::new);
            } else if (value instanceof DurationValue) {
                return ((DurationValue) value).prettyPrint();
            } else if (value instanceof DurationArray) {
                return Arrays.stream(((DurationArray) value).asObjectCopy())
                        .parallel()
                        .map(DurationValue::prettyPrint)
                        .toArray(String[]::new);
            } else
                throw new IllegalArgumentException("Don't know how to map Value " + value.getClass().getName());
        }

        return obj;
    }

    private String addElementTypeVisibilityPropertyToExtendedDataIndex(
            Graph graph,
            ElementLocation elementLocation,
            String tableName,
            String rowId
    ) {
        String elementTypeVisibilityPropertyName = addVisibilityToPropertyName(
                graph,
                ELEMENT_TYPE_FIELD_NAME,
                elementLocation.getVisibility()
        );

        String indexName = getExtendedDataIndexName(elementLocation, tableName, rowId);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        addPropertyToIndex(
                graph,
                indexInfo,
                elementTypeVisibilityPropertyName,
                elementLocation.getVisibility(),
                TextValue.class,
                false,
                false,
                false
        );
        return elementTypeVisibilityPropertyName;
    }

    private String addElementTypeVisibilityPropertyToIndex(Graph graph, Element element) {
        String elementTypeVisibilityPropertyName = addVisibilityToPropertyName(
                graph,
                ELEMENT_TYPE_FIELD_NAME,
                element.getVisibility()
        );
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);

        addPropertyToIndex(
                graph,
                indexInfo,
                elementTypeVisibilityPropertyName,
                element.getVisibility(),
                TextValue.class,
                false,
                false,
                false
        );
        return elementTypeVisibilityPropertyName;
    }

    private Map<String, Object> getPropertiesAsFields(Graph graph, Iterable<Property> properties) {
        Map<String, Object> fieldsMap = new HashMap<>();
        List<Property> streamingProperties = new ArrayList<>();
        for (Property property : properties) {
            if (property.getValue() != null && shouldIgnoreType(property.getValue().getClass())) {
                continue;
            }

            if (property.getValue() instanceof StreamingPropertyValue) {
                StreamingPropertyValue spv = (StreamingPropertyValue) property.getValue();
                if (isStreamingPropertyValueIndexable(graph, property.getName(), spv)) {
                    streamingProperties.add(property);
                }
            } else {
                addPropertyToFieldMap(graph, property, property.getValue(), fieldsMap);
            }
        }
        addStreamingPropertyValuesToFieldMap(graph, streamingProperties, fieldsMap);
        return fieldsMap;
    }

    private void addPropertyToFieldMap(Graph graph, Property property, Object propertyValue, Map<String, Object> propertiesMap) {
        String propertyName = addVisibilityToPropertyName(graph, property);
        addValuesToFieldMap(graph, propertiesMap, propertyName, propertyValue);
    }

    private void addValuesToFieldMap(Graph graph, Map<String, Object> propertiesMap, String propertyName, Object propertyValue) {
        PropertyDefinition propertyDefinition = getPropertyDefinition(graph, propertyName);
        if (propertyValue instanceof GeoShapeValue) {
            convertGeoShape(propertiesMap, propertyName, (GeoShapeValue) propertyValue);
            if (propertyValue instanceof GeoPointValue) {
                GeoPointValue geoPointValue = (GeoPointValue) propertyValue;
                Map<String, Double> coordinates = new HashMap<>();
                coordinates.put("lat", geoPointValue.getLatitude().doubleValue());
                coordinates.put("lon", geoPointValue.getLongitude().doubleValue());
                addPropertyValueToPropertiesMap(propertiesMap, propertyName + GEO_POINT_PROPERTY_NAME_SUFFIX, coordinates);
            }
            return;
        } else if (propertyValue instanceof StreamingPropertyString) {
            propertyValue = ((StreamingPropertyString) propertyValue).getPropertyValue();
        } else if (propertyValue instanceof TextValue) {
            if (propertyDefinition == null ||
                    propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT) ||
                    propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH) ||
                    propertyDefinition.isSortable()) {
                addPropertyValueToPropertiesMap(propertiesMap, propertyName, propertyValue);
            }
            return;
        }

        addPropertyValueToPropertiesMap(propertiesMap, propertyName, propertyValue);
    }

    private boolean isStreamingPropertyValueIndexable(Graph graph, String propertyName, StreamingPropertyValue streamingPropertyValue) {
        if (!streamingPropertyValue.isSearchIndex()) {
            return false;
        }

        PropertyDefinition propertyDefinition = getPropertyDefinition(graph, propertyName);
        if (propertyDefinition != null && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
            return false;
        }

        Class<? extends Value> valueType = streamingPropertyValue.getValueType();
        if (TextValue.class.isAssignableFrom(valueType)) {
            return true;
        } else {
            throw new GeException("Unhandled StreamingPropertyValue type: " + valueType.getName());
        }
    }

    private void addStreamingPropertyValuesToFieldMap(Graph graph, List<Property> properties, Map<String, Object> propertiesMap) {
        List<StreamingPropertyValue> streamingPropertyValues = properties.stream()
                .map((property) -> {
                    if (!(property.getValue() instanceof StreamingPropertyValue)) {
                        throw new GeException("property with a value that is not a StreamingPropertyValue passed to addStreamingPropertyValuesToFieldMap");
                    }
                    return (StreamingPropertyValue) property.getValue();
                })
                .collect(Collectors.toList());

        if (streamingPropertyValues.size() > 0 && graph instanceof GraphWithSearchIndex) {
            ((GraphWithSearchIndex) graph).flushGraph();
        }

        List<InputStream> inputStreams = graph.getStreamingPropertyValueInputStreams(streamingPropertyValues);
        for (int i = 0; i < properties.size(); i++) {
            try {
                String propertyValue = IOUtils.toString(inputStreams.get(i));
                addPropertyToFieldMap(graph, properties.get(i), new StreamingPropertyString(propertyValue), propertiesMap);
            } catch (IOException ex) {
                throw new GeException("could not convert streaming property to string", ex);
            }
        }
    }

    public void handleBulkFailure(BulkItem<?> bulkItem, BulkItemResponse bulkItemResponse, AtomicBoolean retry) throws Exception {
        if (exceptionHandler == null) {
            LOGGER.error("bulk failure: %s: %s", bulkItem, bulkItemResponse.getFailureMessage());
            return;
        }
        exceptionHandler.handleBulkFailure(graph, this, bulkItem, bulkItemResponse, retry);
    }

    public boolean supportsExactMatchSearch(PropertyDefinition propertyDefinition) {
        return propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH) || propertyDefinition.isSortable();
    }

    public GeMetricRegistry getMetricsRegistry() {
        return graph.getMetricsRegistry();
    }

    private static class StreamingPropertyString {
        private final String propertyValue;

        public StreamingPropertyString(String propertyValue) {
            this.propertyValue = propertyValue;
        }

        public String getPropertyValue() {
            return propertyValue;
        }
    }

    protected String addVisibilityToPropertyName(Graph graph, Property property) {
        String propertyName = property.getName();
        Visibility propertyVisibility = property.getVisibility();
        return addVisibilityToPropertyName(graph, propertyName, propertyVisibility);
    }

    protected String addVisibilityToExtendedDataColumnName(Graph graph, ExtendedDataMutation extendedDataMutation) {
        String columnName = extendedDataMutation.getColumnName();
        Visibility propertyVisibility = extendedDataMutation.getVisibility();
        return addVisibilityToPropertyName(graph, columnName, propertyVisibility);
    }

    String addVisibilityToPropertyName(Graph graph, String propertyName, Visibility propertyVisibility) {
        String visibilityHash = getVisibilityHash(graph, propertyName, propertyVisibility);
        return propertyName + "_" + visibilityHash;
    }

    protected String removeVisibilityFromPropertyName(String string) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(string);
        if (m.matches()) {
            string = m.group(1);
        }
        return string;
    }

    private String removeVisibilityFromPropertyNameWithTypeSuffix(String string) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(string);
        if (m.matches()) {
            if (m.groupCount() >= 4 && m.group(4) != null) {
                string = m.group(1) + m.group(4);
            } else {
                string = m.group(1);
            }
        }
        return string;
    }

    public String getPropertyVisibilityHashFromPropertyName(String propertyName) {
        Matcher m = PROPERTY_NAME_PATTERN.matcher(propertyName);
        if (m.matches()) {
            return m.group(3);
        }
        throw new GeException("Could not match property name: " + propertyName);
    }

    public String getAggregationName(String name) {
        Matcher m = AGGREGATION_NAME_PATTERN.matcher(name);
        if (m.matches()) {
            return m.group(1);
        }
        throw new GeException("Could not get aggregation name from: " + name);
    }

    public String replaceFieldnameDots(String fieldName) {
        return fieldName.replace(".", FIELDNAME_DOT_REPLACEMENT);
    }

    public String[] getAllMatchingPropertyNames(Graph graph, String propertyName, Authorizations authorizations) {
        if (Element.ID_PROPERTY_NAME.equals(propertyName)
                || Edge.LABEL_PROPERTY_NAME.equals(propertyName)
                || Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)
                || Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(propertyName)
                || Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)
                || CONCEPT_TYPE_FIELD_NAME.equals(propertyName)
                || ExtendedDataRow.ELEMENT_ID.equals(propertyName)
                || ExtendedDataRow.ELEMENT_TYPE.equals(propertyName)
                || ExtendedDataRow.ROW_ID.equals(propertyName)
                || ExtendedDataRow.TABLE_NAME.equals(propertyName)) {
            return new String[]{propertyName};
        }
        Collection<String> hashes = this.propertyNameVisibilitiesStore.getHashes(graph, propertyName, authorizations);
        return Arrays.stream(addHashesToPropertyName(propertyName, hashes))
                .filter(matchedPropertyName -> isPropertyInIndex(graph, matchedPropertyName))
                .toArray(String[]::new);
    }

    public String[] addHashesToPropertyName(String propertyName, Collection<String> hashes) {
        if (hashes.size() == 0) {
            return new String[0];
        }
        String[] results = new String[hashes.size()];
        int i = 0;
        for (String hash : hashes) {
            results[i++] = propertyName + "_" + hash;
        }
        return results;
    }

    public Collection<String> getQueryableExtendedDataVisibilities(Graph graph, Authorizations authorizations) {
        return propertyNameVisibilitiesStore.getHashes(graph, authorizations);
    }

    public Collection<String> getQueryableElementTypeVisibilityPropertyNames(Graph graph, Authorizations authorizations) {
        Set<String> propertyNames = new HashSet<>();
        for (String hash : propertyNameVisibilitiesStore.getHashes(graph, ELEMENT_TYPE_FIELD_NAME, authorizations)) {
            propertyNames.add(ELEMENT_TYPE_FIELD_NAME + "_" + hash);
        }
        if (propertyNames.size() == 0) {
            throw new GeNoMatchingPropertiesException("No queryable " + ELEMENT_TYPE_FIELD_NAME + " for authorizations " + authorizations);
        }
        return propertyNames;
    }

    public Collection<String> getQueryablePropertyNames(Graph graph, Authorizations authorizations) {
        Set<String> propertyNames = new HashSet<>();
        for (PropertyDefinition propertyDefinition : graph.getPropertyDefinitions()) {
            if (propertyDefinition.getTextIndexHints() == null || propertyDefinition.getTextIndexHints().size() == 0)
                continue;

            List<String> queryableTypeSuffixes = getQueryableTypeSuffixes(propertyDefinition);
            if (queryableTypeSuffixes.size() == 0) {
                continue;
            }
            String propertyNameNoVisibility = removeVisibilityFromPropertyName(propertyDefinition.getPropertyName()); // could be stored deflated
            if (isReservedFieldName(propertyNameNoVisibility)) {
                continue;
            }
            for (String hash : propertyNameVisibilitiesStore.getHashes(graph, propertyNameNoVisibility, authorizations)) {
                for (String typeSuffix : queryableTypeSuffixes) {
                    propertyNames.add(propertyNameNoVisibility + "_" + hash + typeSuffix);
                }
            }
        }
        return propertyNames;
    }

    private static List<String> getQueryableTypeSuffixes(PropertyDefinition propertyDefinition) {
        List<String> typeSuffixes = new ArrayList<>();
        if (TextValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
            if (propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                typeSuffixes.add(EXACT_MATCH_PROPERTY_NAME_SUFFIX);
            }
            if (propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                typeSuffixes.add("");
            }
        } else if (GeoShapeValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
            typeSuffixes.add("");
        }
        return typeSuffixes;
    }

    protected static boolean isReservedFieldName(String fieldName) {
        return fieldName.startsWith("__");
    }

    private String getVisibilityHash(Graph graph, String propertyName, Visibility visibility) {
        return this.propertyNameVisibilitiesStore.getHash(graph, propertyName, visibility);
    }

    @Override
    public void deleteElement(Graph graph, ElementId elementId, Authorizations authorizations) {
        String indexName = getIndexName(elementId);
        String docId = getIdStrategy().createElementDocId(elementId);
        if (MUTATION_LOGGER.isTraceEnabled()) {
            LOGGER.trace("deleting document %s (docId: %s)", elementId.getId(), docId);
        }
        bulkUpdateService.addDelete(indexName, getIdStrategy().getType(), docId, elementId);
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return SearchIndexSecurityGranularity.PROPERTY;
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new ElasticsearchSearchGraphQuery(
                getClient(),
                graph,
                queryString,
                new ElasticsearchSearchQueryBase.Options()
                        .setIndexSelectionStrategy(getIndexSelectionStrategy())
                        .setPageSize(getConfig().getQueryPageSize())
                        .setPagingLimit(getConfig().getPagingLimit())
                        .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                        .setTermAggregationShardSize(getConfig().getTermAggregationShardSize())
                        .setMaxQueryStringTerms(getConfig().getMaxQueryStringTerms()),
                authorizations
        );
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new ElasticsearchSearchVertexQuery(
                getClient(),
                graph,
                vertex,
                queryString,
                new ElasticsearchSearchVertexQuery.Options()
                        .setIndexSelectionStrategy(getIndexSelectionStrategy())
                        .setPageSize(getConfig().getQueryPageSize())
                        .setPagingLimit(getConfig().getPagingLimit())
                        .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                        .setTermAggregationShardSize(getConfig().getTermAggregationShardSize())
                        .setMaxQueryStringTerms(getConfig().getMaxQueryStringTerms()),
                authorizations
        );
    }

    @Override
    public Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, Authorizations authorizations) {
        return new ElasticsearchSearchExtendedDataQuery(
                getClient(),
                graph,
                element.getId(),
                tableName,
                queryString,
                new ElasticsearchSearchExtendedDataQuery.Options()
                        .setIndexSelectionStrategy(getIndexSelectionStrategy())
                        .setPageSize(getConfig().getQueryPageSize())
                        .setPagingLimit(getConfig().getPagingLimit())
                        .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                        .setTermAggregationShardSize(getConfig().getTermAggregationShardSize())
                        .setMaxQueryStringTerms(getConfig().getMaxQueryStringTerms()),
                authorizations
        );
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(Graph graph, String[] similarToFields, String similarToText, Authorizations authorizations) {
        return new ElasticsearchSearchGraphQuery(
                getClient(),
                graph,
                similarToFields,
                similarToText,
                new ElasticsearchSearchQueryBase.Options()
                        .setIndexSelectionStrategy(getIndexSelectionStrategy())
                        .setPageSize(getConfig().getQueryPageSize())
                        .setPagingLimit(getConfig().getPagingLimit())
                        .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                        .setTermAggregationShardSize(getConfig().getTermAggregationShardSize())
                        .setMaxQueryStringTerms(getConfig().getMaxQueryStringTerms()),
                authorizations
        );
    }

    @Override
    public boolean isFieldLevelSecuritySupported() {
        return true;
    }

    protected void addPropertyDefinitionToIndex(
            Graph graph,
            IndexInfo indexInfo,
            String propertyName,
            Visibility propertyVisibility,
            PropertyDefinition propertyDefinition
    ) {
        String propertyNameWithVisibility = addVisibilityToPropertyName(graph, propertyName, propertyVisibility);

        if (GeoShapeValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility + GEO_PROPERTY_NAME_SUFFIX, propertyVisibility, propertyDefinition.getDataType(), true, false, false);
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, TextValue.class, true, true, false);
            if (propertyDefinition.getDataType().isAssignableFrom(GeoPointValue.class)) {
                addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility + GEO_POINT_PROPERTY_NAME_SUFFIX, propertyVisibility, propertyDefinition.getDataType(), true, true, false);
            }
            return;
        }

        if (TextValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
            boolean exact = propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH);
            boolean analyzed = propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT);
            boolean sortable = propertyDefinition.isSortable();
            if (analyzed || exact || sortable) {
                addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, TextValue.class, analyzed, exact, sortable);
            }
            return;
        }

        addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, propertyDefinition.getDataType(), true, false, false);
    }

    protected PropertyDefinition getPropertyDefinition(Graph graph, String propertyName) {
        propertyName = removeVisibilityFromPropertyNameWithTypeSuffix(propertyName);
        return graph.getPropertyDefinition(propertyName);
    }

    public void addPropertyToIndex(
            Graph graph,
            IndexInfo indexInfo,
            String propertyName,
            Value propertyValue,
            Visibility propertyVisibility
    ) {
        PropertyDefinition propertyDefinition = getPropertyDefinition(graph, propertyName);

        if (propertyDefinition != null) {
            addPropertyDefinitionToIndex(graph, indexInfo, propertyName, propertyVisibility, propertyDefinition);
        } else {
            addPropertyToIndexInner(graph, indexInfo, propertyName, propertyValue, propertyVisibility);
        }

        propertyDefinition = getPropertyDefinition(graph, propertyName + EXACT_MATCH_PROPERTY_NAME_SUFFIX);
        if (propertyDefinition != null) {
            addPropertyDefinitionToIndex(graph, indexInfo, propertyName, propertyVisibility, propertyDefinition);
        }

        if (propertyValue instanceof GeoShapeValue) {
            propertyDefinition = getPropertyDefinition(graph, propertyName + GEO_PROPERTY_NAME_SUFFIX);
            if (propertyDefinition != null) {
                addPropertyDefinitionToIndex(graph, indexInfo, propertyName, propertyVisibility, propertyDefinition);
            }
        }
    }

    private void addPropertyToIndexInner(
            Graph graph,
            IndexInfo indexInfo,
            String propertyName,
            Value propertyValue,
            Visibility propertyVisibility
    ) {
        String propertyNameWithVisibility = addVisibilityToPropertyName(graph, propertyName, propertyVisibility);

        if (indexInfo.isPropertyDefined(propertyNameWithVisibility, propertyVisibility)) {
            return;
        }

        Class<? extends Value> dataType;
        if (propertyValue instanceof StreamingPropertyValue) {
            StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValue;
            if (!streamingPropertyValue.isSearchIndex()) {
                return;
            }
            dataType = streamingPropertyValue.getValueType();
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, dataType, true, false, false);
        } else if (propertyValue instanceof TextValue) {
            dataType = TextValue.class;
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, dataType, true, true, false);
        } else if (propertyValue instanceof GeoShapeValue) {
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility + GEO_PROPERTY_NAME_SUFFIX, propertyVisibility, propertyValue.getClass(), true, false, false);
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, TextValue.class, true, true, false);
            if (propertyValue instanceof GeoPointValue) {
                addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility + GEO_POINT_PROPERTY_NAME_SUFFIX, propertyVisibility, propertyValue.getClass(), true, true, false);
            }
        } else {
            checkNotNull(propertyValue, "property value cannot be null for property: " + propertyNameWithVisibility);
            dataType = propertyValue.getClass();
            addPropertyToIndex(graph, indexInfo, propertyNameWithVisibility, propertyVisibility, dataType, true, false, false);
        }
    }

    protected void addPropertyToIndex(
            Graph graph,
            IndexInfo indexInfo,
            String propertyName,
            Visibility propertyVisibility,
            Class<? extends Value> dataType,
            boolean analyzed,
            boolean exact,
            boolean sortable
    ) {
        if (indexInfo.isPropertyDefined(propertyName, propertyVisibility)) {
            return;
        }

        if (shouldIgnoreType(dataType)) {
            return;
        }

        this.indexInfosLock.writeLock().lock();
        try {
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(getIdStrategy().getType())
                    .startObject("properties")
                    .startObject(replaceFieldnameDots(propertyName));

            addTypeToMapping(mapping, propertyName, dataType, analyzed, exact, sortable);

            mapping
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("addPropertyToIndex: %s: %s", dataType.getName(), Strings.toString(mapping));
            }

            getClient()
                    .admin()
                    .indices()
                    .preparePutMapping(indexInfo.getIndexName())
                    .setType(getIdStrategy().getType())
                    .setSource(mapping)
                    .execute()
                    .actionGet();

            addPropertyNameVisibility(graph, indexInfo, propertyName, propertyVisibility);
        } catch (IOException ex) {
            throw new GeException(
                    String.format(
                            "Could not add property to index (index: %s, propertyName: %s)",
                            indexInfo.getIndexName(),
                            propertyName
                    ),
                    ex
            );
        } finally {
            this.indexInfosLock.writeLock().unlock();
        }
    }

    protected void addPropertyNameVisibility(Graph graph, IndexInfo indexInfo, String propertyName, Visibility propertyVisibility) {
        String propertyNameNoVisibility = removeVisibilityFromPropertyName(propertyName);
        if (propertyVisibility != null) {
            this.propertyNameVisibilitiesStore.getHash(graph, propertyNameNoVisibility, propertyVisibility);
        }
        indexInfo.addPropertyNameVisibility(propertyNameNoVisibility, propertyVisibility);
        indexInfo.addPropertyNameVisibility(propertyName, propertyVisibility);
    }

    @Override
    public Map<Object, Long> getVertexPropertyCountByValue(Graph graph, String propertyName, Authorizations authorizations) {
        indexRefreshTracker.refresh(client);

        TermQueryBuilder elementTypeFilterBuilder = new TermQueryBuilder(ELEMENT_TYPE_FIELD_NAME, ElasticsearchDocumentType.VERTEX.getKey());

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchAllQuery())
                .filter(elementTypeFilterBuilder);

        SearchRequestBuilder q = getClient().prepareSearch(getIndexNamesAsArray(graph))
                .setQuery(queryBuilder)
                .setSize(0);

        for (String p : getAllMatchingPropertyNames(graph, propertyName, authorizations)) {
            String countAggName = "count-" + p;
            PropertyDefinition propertyDefinition = getPropertyDefinition(graph, p);
            p = replaceFieldnameDots(p);
            if (propertyDefinition != null && propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                p = p + EXACT_MATCH_PROPERTY_NAME_SUFFIX;
            }

            TermsAggregationBuilder countAgg = AggregationBuilders
                    .terms(countAggName)
                    .field(p)
                    .size(500000);
            q = q.addAggregation(countAgg);
        }

        if (ElasticsearchSearchQueryBase.QUERY_LOGGER.isTraceEnabled()) {
            ElasticsearchSearchQueryBase.QUERY_LOGGER.trace("query: %s", q);
        }
        SearchResponse response = checkForFailures(getClient().search(q.request()).actionGet());
        Map<Object, Long> results = new HashMap<>();
        for (Aggregation agg : response.getAggregations().asList()) {
            Terms propertyCountResults = (Terms) agg;
            for (Terms.Bucket propertyCountResult : propertyCountResults.getBuckets()) {
                String mapKey = ((String) propertyCountResult.getKey()).toLowerCase();
                Long previousValue = results.get(mapKey);
                if (previousValue == null) {
                    previousValue = 0L;
                }
                results.put(mapKey, previousValue + propertyCountResult.getDocCount());
            }
        }
        return results;
    }

    public IndexInfo ensureIndexCreatedAndInitialized(String indexName) {
        Map<String, IndexInfo> indexInfos = getIndexInfos();
        IndexInfo indexInfo = indexInfos.get(indexName);
        if (indexInfo != null && indexInfo.isElementTypeDefined()) {
            return indexInfo;
        }
        return initializeIndex(indexInfo, indexName);
    }

    private IndexInfo initializeIndex(String indexName) {
        return initializeIndex(null, indexName);
    }

    private IndexInfo initializeIndex(IndexInfo indexInfo, String indexName) {
        indexInfosLock.writeLock().lock();
        try {
            if (indexInfo == null) {
                if (!client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {
                    try {
                        createIndex(indexName);
                    } catch (Exception e) {
                        throw new GeException("Could not create index: " + indexName, e);
                    }
                }

                indexInfo = createIndexInfo(indexName);

                if (indexInfos == null) {
                    loadIndexInfos();
                }
                indexInfos.put(indexName, indexInfo);
            }

            ensureMappingsCreated(indexInfo);

            return indexInfo;
        } finally {
            indexInfosLock.writeLock().unlock();
        }
    }

    protected IndexInfo createIndexInfo(String indexName) {
        return new IndexInfo(indexName);
    }

    protected void ensureMappingsCreated(IndexInfo indexInfo) {
        if (!indexInfo.isElementTypeDefined()) {
            try {
                XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("_source").field("enabled", true).endObject()
                        .startObject("properties");
                createIndexAddFieldsToElementType(mappingBuilder);
                XContentBuilder mapping = mappingBuilder.endObject()
                        .endObject();

                client.admin().indices().preparePutMapping(indexInfo.getIndexName())
                        .setType(getIdStrategy().getType())
                        .setSource(mapping)
                        .execute()
                        .actionGet();

                indexInfo.setElementTypeDefined(true);
            } catch (Throwable e) {
                throw new GeException("Could not add mappings to index: " + indexInfo.getIndexName(), e);
            }
        }
    }

    protected void createIndexAddFieldsToElementType(XContentBuilder builder) throws IOException {
        builder
                .startObject(ELEMENT_ID_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(ELEMENT_TYPE_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(EXTENDED_DATA_TABLE_NAME_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(VISIBILITY_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(IN_VERTEX_ID_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(OUT_VERTEX_ID_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(EDGE_LABEL_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
                .startObject(CONCEPT_TYPE_FIELD_NAME).field("type", "keyword").field("store", "true").endObject()
        ;
    }

    @Override
    public void deleteProperty(Graph graph, Element element, PropertyDescriptor property, Authorizations authorizations) {
        deleteProperties(graph, element, Collections.singletonList(property), authorizations);
    }

    @Override
    public void deleteProperties(Graph graph, Element element, Collection<PropertyDescriptor> propertyList, Authorizations authorizations) {
        List<String> fieldsToRemove = new ArrayList<>();
        Map<String, Object> fieldsToSet = new HashMap<>();
        propertyList.forEach(p -> {
            fieldsToRemove.addAll(getFieldsToRemove(graph, p.getName(), p.getVisibility()));
            addExistingValuesToFieldMap(graph, element, p.getName(), p.getVisibility(), fieldsToSet);
        });

        String documentId = getIdStrategy().createElementDocId(element);
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        addUpdateToBulk(
                indexInfo.getIndexName(),
                documentId,
                element,
                fieldsToSet,
                fieldsToRemove,
                null
        );
        if (getConfig().isAutoFlush()) {
            flush(graph);
        }
    }

    @Override
    public void addElements(Graph graph, Iterable<? extends Element> elements, Authorizations authorizations) {
        for (Element element : elements) {
            addElement(graph, element, authorizations);
        }
    }

    @Override
    public MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, Authorizations authorizations) {
        return new ElasticsearchSearchMultiVertexQuery(
                getClient(),
                graph,
                vertexIds,
                queryString,
                new ElasticsearchSearchQueryBase.Options()
                        .setIndexSelectionStrategy(getIndexSelectionStrategy())
                        .setPageSize(getConfig().getQueryPageSize())
                        .setPagingLimit(getConfig().getPagingLimit())
                        .setScrollKeepAlive(getConfig().getScrollKeepAlive())
                        .setTermAggregationShardSize(getConfig().getTermAggregationShardSize())
                        .setMaxQueryStringTerms(getConfig().getMaxQueryStringTerms()),
                authorizations
        );
    }

    @Override
    public boolean isQuerySimilarToTextSupported() {
        return true;
    }

    @Override
    public void flush(Graph graph) {
        Runnable r = () -> {
            bulkUpdateService.flush();

            if (shouldRefreshIndexOnFlush()) {
                indexRefreshTracker.refresh(client);
            }
        };

        if (bulkIngestEnabled) {
            executorService.submit(r);
        } else {
            r.run();
        }
    }

    private void removeFieldsFromDocument(Graph graph, ElementLocation elementLocation, String field) {
        removeFieldsFromDocument(graph, elementLocation, Lists.newArrayList(field));
    }

    private void removeFieldsFromDocument(Graph graph, ElementLocation elementLocation, Collection<String> fields) {
        String indexName = getIndexName(elementLocation);
        String documentId = getIdStrategy().createElementDocId(elementLocation);
        removeFieldsFromDocument(graph, indexName, elementLocation, documentId, fields);
    }

    private void removeFieldsFromDocument(
            Graph graph,
            String indexName,
            ElementLocation elementLocation,
            String documentId,
            Collection<String> fields
    ) {
        if (fields == null || fields.isEmpty()) {
            return;
        }

        List<String> fieldNames = fields.stream().map(this::replaceFieldnameDots).collect(Collectors.toList());
        if (fieldNames.isEmpty()) {
            return;
        }

        bulkUpdateService.addElementUpdate(
                indexName,
                getIdStrategy().getType(),
                documentId,
                elementLocation,
                Collections.emptyMap(),
                Collections.emptyMap(),
                fieldNames,
                Collections.emptyMap(),
                true
        );

        if (getConfig().isAutoFlush()) {
            flush(graph);
        }
    }

    private void addUpdateToBulk(
            String indexName,
            String documentId,
            ElementLocation elementLocation,
            Map<String, Object> fieldsToSet,
            Collection<String> fieldsToRemove,
            Map<String, String> fieldsToRename
    ) {
        if ((fieldsToSet == null || fieldsToSet.isEmpty()) &&
                (fieldsToRemove == null || fieldsToRemove.isEmpty()) &&
                (fieldsToRename == null || fieldsToRename.isEmpty())) {
            return;
        }

        fieldsToSet = fieldsToSet == null ? Collections.emptyMap() : fieldsToSet.entrySet().stream()
                .collect(Collectors.toMap(e -> replaceFieldnameDots(e.getKey()), Map.Entry::getValue));
        fieldsToRemove = fieldsToRemove == null ? Collections.emptyList() : fieldsToRemove.stream().map(this::replaceFieldnameDots).collect(Collectors.toList());
        fieldsToRename = fieldsToRename == null ? Collections.emptyMap() : fieldsToRename.entrySet().stream()
                .collect(Collectors.toMap(e -> replaceFieldnameDots(e.getKey()), e -> replaceFieldnameDots(e.getValue())));

        bulkUpdateService.addElementUpdate(
                indexName,
                getIdStrategy().getType(),
                documentId,
                elementLocation,
                Collections.emptyMap(),
                fieldsToSet,
                fieldsToRemove,
                fieldsToRename,
                true
        );
    }


    protected String[] getIndexNamesAsArray(Graph graph) {
        Map<String, IndexInfo> indexInfos = getIndexInfos();
        if (indexInfos.size() == indexInfosLastSize) {
            return indexNamesAsArray;
        }
        synchronized (this) {
            Set<String> keys = indexInfos.keySet();
            indexNamesAsArray = keys.toArray(new String[0]);
            indexInfosLastSize = indexInfos.size();
            return indexNamesAsArray;
        }
    }

    @Override
    public void shutdown() {
        bulkUpdateService.shutdown();

        try {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
        shutdownElasticsearchClient();

        if (propertyNameVisibilitiesStore instanceof Closeable) {
            try {
                ((Closeable) propertyNameVisibilitiesStore).close();
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }
    }

    /**
     * Visible to allow special handling of the client, particularly in unit tests.
     */
    protected void shutdownElasticsearchClient() {
        if (client != null)
            client.close();

        if (sidecar != null)
            sidecar.stop();
    }

    @SuppressWarnings("unused")
    protected String[] getIndexNames(PropertyDefinition propertyDefinition) {
        return indexSelectionStrategy.getIndexNames(this, propertyDefinition);
    }

    protected String getIndexName(ElementId elementId) {
        return indexSelectionStrategy.getIndexName(this, elementId);
    }

    protected String getExtendedDataIndexName(
            ElementLocation elementLocation,
            String tableName,
            String rowId
    ) {
        return indexSelectionStrategy.getExtendedDataIndexName(this, elementLocation, tableName, rowId);
    }

    protected String getExtendedDataIndexName(ExtendedDataRowId rowId) {
        return indexSelectionStrategy.getExtendedDataIndexName(this, rowId);
    }

    protected String[] getIndicesToQuery() {
        return indexSelectionStrategy.getIndicesToQuery(this);
    }

    @Override
    public boolean isFieldBoostSupported() {
        return false;
    }

    private IndexInfo addExtendedDataColumnsToIndex(
            Graph graph,
            ElementLocation elementLocation,
            String tableName,
            String rowId,
            Iterable<ExtendedDataMutation> columns
    ) {
        String indexName = getExtendedDataIndexName(elementLocation, tableName, rowId);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        for (ExtendedDataMutation column : columns) {
            addPropertyToIndex(graph, indexInfo, column.getColumnName(), column.getValue(), column.getVisibility());
        }
        return indexInfo;
    }

    public IndexInfo addPropertiesToIndex(Graph graph, Element element, Iterable<Property> properties) {
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName);
        for (Property property : properties) {
            addPropertyToIndex(graph, indexInfo, property.getName(), property.getValue(), property.getVisibility());
        }
        return indexInfo;
    }

    protected boolean shouldIgnoreType(Class<? extends Value> dataType) {
        return dataType == ByteArray.class || dataType == NoValue.class;
    }

    @Override
    public synchronized void truncate(Graph graph) {
        LOGGER.warn("Truncate of Elasticsearch is not possible, dropping the indices and recreating instead.");
        drop(graph);
    }

    @Override
    public void drop(Graph graph) {
        this.indexInfosLock.writeLock().lock();
        try {
            if (this.indexInfos == null) {
                loadIndexInfos();
            }
            Set<String> indexInfosSet = new HashSet<>(this.indexInfos.keySet());
            for (String indexName : indexInfosSet) {
                try {
                    DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indexName);
                    getClient().admin().indices().delete(deleteRequest).actionGet();
                } catch (Exception ex) {
                    throw new GeException("Could not delete index " + indexName, ex);
                }
                this.indexInfos.remove(indexName);
                initializeIndex(indexName);
            }
        } finally {
            this.indexInfosLock.writeLock().unlock();
        }
    }

    @SuppressWarnings("unchecked")
    protected void addPropertyValueToPropertiesMap(Map<String, Object> propertiesMap, String propertyName, Object propertyValue) {
        Object existingValue = propertiesMap.get(propertyName);
        Object valueForIndex = convertValueForIndexing(propertyName, propertyValue);
        if (existingValue == null) {
            propertiesMap.put(propertyName, valueForIndex);
            return;
        }

        if (existingValue instanceof List) {
            try {
                ((List) existingValue).add(valueForIndex);
            } catch (Exception ex) {
                LOGGER.error("could not add to existing list, this could cause performance issues", ex);
                ArrayList newList = new ArrayList<>((List) existingValue);
                newList.add(valueForIndex);
                propertiesMap.put(propertyName, newList);
            }
            return;
        }

        List list = new ArrayList();
        list.add(existingValue);
        list.add(valueForIndex);
        propertiesMap.put(propertyName, list);
    }

    protected void convertGeoShape(Map<String, Object> propertiesMap, String propertyNameWithVisibility, GeoShapeValue geoShapeValue) {
        GeoShape geoShape = geoShapeValue.asObjectCopy();
        try {
            geoShape.validate();
        } catch (GeInvalidShapeException ve) {
            LOGGER.warn("Attempting to repair invalid GeoShape", ve);
            geoShape = GeoUtils.repair(geoShape);
        }

        Map<String, Object> propertyValueMap;
        if (geoShape instanceof GeoPoint) {
            propertyValueMap = convertGeoPoint((GeoPoint) geoShape);
        } else if (geoShape instanceof GeoCircle) {
            propertyValueMap = convertGeoCircle((GeoCircle) geoShape);
        } else if (geoShape instanceof GeoLine) {
            propertyValueMap = convertGeoLine((GeoLine) geoShape);
        } else if (geoShape instanceof GeoPolygon) {
            propertyValueMap = convertGeoPolygon((GeoPolygon) geoShape);
        } else if (geoShape instanceof GeoCollection) {
            propertyValueMap = convertGeoCollection((GeoCollection) geoShape);
        } else if (geoShape instanceof GeoRect) {
            propertyValueMap = convertGeoRect((GeoRect) geoShape);
        } else {
            throw new GeException("Unexpected GeoShape value of type: " + geoShape.getClass().getName());
        }

        addPropertyValueToPropertiesMap(propertiesMap, propertyNameWithVisibility + GEO_PROPERTY_NAME_SUFFIX, propertyValueMap);
    }

    protected Map<String, Object> convertGeoPoint(GeoPoint geoPoint) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(ShapeParser.FIELD_TYPE.getPreferredName(), "point");
        propertyValueMap.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude()));
        return propertyValueMap;
    }

    protected Map<String, Object> convertGeoCircle(GeoCircle geoCircle) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(ShapeParser.FIELD_TYPE.getPreferredName(), "circle");
        List<Double> coordinates = new ArrayList<>();
        coordinates.add(geoCircle.getLongitude());
        coordinates.add(geoCircle.getLatitude());
        propertyValueMap.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), coordinates);
        propertyValueMap.put(CircleBuilder.FIELD_RADIUS.getPreferredName(), geoCircle.getRadius() + "km");
        return propertyValueMap;
    }

    protected Map<String, Object> convertGeoRect(GeoRect geoRect) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(ShapeParser.FIELD_TYPE.getPreferredName(), "envelope");
        List<List<Double>> coordinates = new ArrayList<>();
        coordinates.add(Arrays.asList(geoRect.getNorthWest().getLongitude(), geoRect.getNorthWest().getLatitude()));
        coordinates.add(Arrays.asList(geoRect.getSouthEast().getLongitude(), geoRect.getSouthEast().getLatitude()));
        propertyValueMap.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), coordinates);
        return propertyValueMap;
    }

    protected Map<String, Object> convertGeoLine(GeoLine geoLine) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(ShapeParser.FIELD_TYPE.getPreferredName(), "linestring");
        List<List<Double>> coordinates = new ArrayList<>();
        geoLine.getGeoPoints().forEach(geoPoint -> coordinates.add(Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude())));
        propertyValueMap.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), coordinates);
        return propertyValueMap;
    }

    protected Map<String, Object> convertGeoPolygon(GeoPolygon geoPolygon) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(ShapeParser.FIELD_TYPE.getPreferredName(), "polygon");
        List<List<List<Double>>> coordinates = new ArrayList<>();
        coordinates.add(geoPolygon.getOuterBoundary().stream()
                .map(geoPoint -> Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude()))
                .collect(Collectors.toList()));
        geoPolygon.getHoles().forEach(holeBoundary ->
                coordinates.add(holeBoundary.stream()
                        .map(geoPoint -> Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude()))
                        .collect(Collectors.toList())));
        propertyValueMap.put(ShapeParser.FIELD_COORDINATES.getPreferredName(), coordinates);
        return propertyValueMap;
    }

    protected Map<String, Object> convertGeoCollection(GeoCollection geoCollection) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(ShapeParser.FIELD_TYPE.getPreferredName(), "geometrycollection");

        List<Map<String, Object>> geometries = new ArrayList<>();
        geoCollection.getGeoShapes().forEach(geoShape -> {
            if (geoShape instanceof GeoPoint) {
                geometries.add(convertGeoPoint((GeoPoint) geoShape));
            } else if (geoShape instanceof GeoCircle) {
                geometries.add(convertGeoCircle((GeoCircle) geoShape));
            } else if (geoShape instanceof GeoLine) {
                geometries.add(convertGeoLine((GeoLine) geoShape));
            } else if (geoShape instanceof GeoPolygon) {
                geometries.add(convertGeoPolygon((GeoPolygon) geoShape));
            } else {
                throw new GeException("Unsupported GeoShape value in GeoCollection of type: " + geoShape.getClass().getName());
            }
        });
        propertyValueMap.put(ShapeParser.FIELD_GEOMETRIES.getPreferredName(), geometries);

        return propertyValueMap;
    }

    public IndexSelectionStrategy getIndexSelectionStrategy() {
        return indexSelectionStrategy;
    }

    @SuppressWarnings("unused")
    protected void createIndex(String indexName) throws IOException {
        CreateIndexResponse createResponse = client.admin().indices().prepareCreate(indexName)
                .setSettings(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("index.indexing.slowlog.threshold.index.debug", "0s")
                        .field("index.search.slowlog.threshold.fetch.debug", "7s")
                        .field("index.search.slowlog.threshold.query.debug", "7s")
                        .field("number_of_shards", getConfig().getNumberOfShards())
                        .field("number_of_replicas", getConfig().getNumberOfReplicas())
                        .field("index.mapping.total_fields.limit", 100000)
                        .field("refresh_interval", getConfig().getIndexRefreshInterval())
                        .field("index.max_result_window", 50000)
                        .startObject("analysis")
                        .startObject("analyzer")
                        .startObject("default")
                        .field("type", "custom")
                        .field("tokenizer", "standard")
                        .array("filter", "lowercase", "my_ascii_folding")
                        .endObject()
                        .endObject()
                        .startObject("filter")
                        .startObject("my_ascii_folding")
                        .field("type", "asciifolding")
                        .field("preserve_original", true)
                        .endObject()
                        .endObject()
                        .startObject("normalizer")
                        .startObject(LOWERCASER_NORMALIZER_NAME)
                        .field("type", "custom")
                        .array("filter", "lowercase")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .execute().actionGet();

        ClusterHealthResponse health = client.admin().cluster().prepareHealth(indexName)
                .setWaitForGreenStatus()
                .execute().actionGet();
        LOGGER.debug("Index status: %s", health.toString());
        if (health.isTimedOut()) {
            LOGGER.warn("timed out waiting for yellow/green index status, for index: %s", indexName);
        }
    }

    public Client getClient() {
        return client;
    }

    public ElasticsearchSearchIndexConfiguration getConfig() {
        return config;
    }

    public IdStrategy getIdStrategy() {
        return idStrategy;
    }

    public QueryStringTransformer getQueryStringTransformer() {
        return this.config.getQueryStringTransformer();
    }

    public IndexRefreshTracker getIndexRefreshTracker() {
        return indexRefreshTracker;
    }

    public boolean isPropertyInIndex(Graph graph, String propertyName, Visibility visibility) {
        Map<String, IndexInfo> indexInfos = getIndexInfos();
        for (Map.Entry<String, IndexInfo> entry : indexInfos.entrySet()) {
            if (entry.getValue().isPropertyDefined(propertyName, visibility)) {
                return true;
            }
        }
        return false;
    }

    public boolean isPropertyInIndex(Graph graph, String propertyName) {
        Map<String, IndexInfo> indexInfos = getIndexInfos();
        for (Map.Entry<String, IndexInfo> entry : indexInfos.entrySet()) {
            if (entry.getValue().isPropertyDefined(propertyName)) {
                return true;
            }
        }
        return false;
    }

    public String[] getPropertyNames(Graph graph, String propertyName, Authorizations authorizations) {
        String[] allMatchingPropertyNames = getAllMatchingPropertyNames(graph, propertyName, authorizations);
        return Arrays.stream(allMatchingPropertyNames)
                .map(this::replaceFieldnameDots)
                .collect(Collectors.toList())
                .toArray(new String[allMatchingPropertyNames.length]);
    }

    public void enableBulkIngest(boolean enable) {
        this.bulkIngestEnabled = enable;
        String[] indexes = getIndexInfos().values().stream().map(IndexInfo::getIndexName).toArray(String[]::new);
        UpdateSettingsRequestBuilder req = client.admin().indices().prepareUpdateSettings(indexes);
        Settings.Builder settings = Settings.builder();
        if (enable) {
            settings.put("index.refresh_interval", "-1");
        } else {
            settings.put("index.refresh_interval", "1s");
        }
        req.setSettings(settings);
        req.get();

        if (!enable) {
            // do a force merge
            client.admin().indices().prepareForceMerge(indexes)
                    .setMaxNumSegments(5)
                    .get();
        }
    }

    @Override
    public int getNumShards() {
        ClusterHealthResponse healthResponse =
                getClient().admin().cluster().health(new ClusterHealthRequest()).actionGet();
        return healthResponse.getActivePrimaryShards();
    }

    boolean shouldRefreshIndexOnQuery() {
        return !shouldRefreshIndexOnFlush();
    }

    boolean shouldRefreshIndexOnFlush() {
        return refreshIndexOnFlush;
    }

    public boolean isBulkIngestEnabled() {
        return bulkIngestEnabled;
    }
}
