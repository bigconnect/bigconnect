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
package com.mware.ge.accumulo;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.*;
import com.mware.core.config.options.GraphOptions;
import com.mware.ge.*;
import com.mware.ge.accumulo.iterator.*;
import com.mware.ge.accumulo.iterator.model.ElementData;
import com.mware.ge.accumulo.iterator.model.IteratorFetchHints;
import com.mware.ge.accumulo.iterator.model.PropertyColumnQualifier;
import com.mware.ge.accumulo.iterator.model.PropertyMetadataColumnQualifier;
import com.mware.ge.accumulo.iterator.util.ByteArrayWrapper;
import com.mware.ge.accumulo.iterator.util.ByteSequenceUtils;
import com.mware.ge.accumulo.keys.KeyHelper;
import com.mware.ge.accumulo.tools.HDFSGraphBackup;
import com.mware.ge.accumulo.tools.HDFSGraphRestore;
import com.mware.ge.accumulo.util.DataInputStreamUtils;
import com.mware.ge.accumulo.util.GeTabletServerBatchReader;
import com.mware.ge.accumulo.util.RangeUtils;
import com.mware.ge.accumulo.util.SnappyUtils;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.security.ColumnVisibility;
import com.mware.ge.serializer.GeSerializer;
import com.mware.ge.store.*;
import com.mware.ge.store.mutations.StoreColumnUpdate;
import com.mware.ge.store.mutations.StoreMutation;
import com.mware.ge.store.util.KeyBase;
import com.mware.ge.store.util.MetadataEntry;
import com.mware.ge.store.util.StorableKeyHelper;
import com.mware.ge.tools.GraphBackup;
import com.mware.ge.tools.GraphRestore;
import com.mware.ge.util.*;
import com.mware.ge.values.storable.StreamingPropertyValueRef;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.WholeRowIterator;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.hadoop.io.Text;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.mware.ge.util.IterableUtils.toList;

public class AccumuloGraph extends AbstractStorableGraph<StorableVertex, StorableEdge> implements Traceable {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(AccumuloGraph.class);
    public static final AccumuloGraphLogger GRAPH_LOGGER = new AccumuloGraphLogger(QUERY_LOGGER);
    private static final String ROW_DELETING_ITERATOR_NAME = RowDeletingIterator.class.getSimpleName();
    private static final int ROW_DELETING_ITERATOR_PRIORITY = 7;
    public static final int SINGLE_VERSION = 1;
    public static final Integer ALL_VERSIONS = null;
    private static final int ACCUMULO_DEFAULT_VERSIONING_ITERATOR_PRIORITY = 20;
    private static final String ACCUMULO_DEFAULT_VERSIONING_ITERATOR_NAME = "vers";
    private static final String CLASSPATH_CONTEXT_NAME = "ge";

    private static final Object addIteratorLock = new Object();
    private static final ColumnVisibility EMPTY_COLUMN_VISIBILITY = new ColumnVisibility();
    private final Connector connector;
    private final MultiTableBatchWriter batchWriter;
    private final int numberOfQueryThreads;
    private final boolean compressIteratorTransfers;
    private boolean distributedTraceEnabled;
    private int largeValueErrorThreshold;
    private int largeValueWarningThreshold;

    protected AccumuloGraph(AccumuloGraphConfiguration config, Connector connector) {
        super(config);

        this.connector = connector;
        this.nameSubstitutionStrategy = AccumuloNameSubstitutionStrategy.create(config.createSubstitutionStrategy(this));

        setGraphMetadataStore(new AccumuloMetadataStore( this));

        this.numberOfQueryThreads = getConfiguration().getNumberOfQueryThreads();
        this.largeValueErrorThreshold = getConfiguration().getLargeValueErrorThreshold();
        this.largeValueWarningThreshold = getConfiguration().getLargeValueWarningThreshold();
        this.compressIteratorTransfers = getConfiguration().isCompressIteratorTransfers() && SnappyUtils.testSnappySupport();

        BatchWriterConfig writerConfig = getConfiguration().createBatchWriterConfig();
        this.batchWriter = connector.createMultiTableBatchWriter(writerConfig);
    }

    public static AccumuloGraph create(AccumuloGraphConfiguration config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }

        Connector connector = config.createConnector();
        if (config.isHistoryInSeparateTable()) {
            ensureTableExists(connector, getVerticesTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables(), config);
            ensureTableExists(connector, getEdgesTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables(), config);

            ensureTableExists(connector, getHistoryVerticesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables(), config);
            ensureTableExists(connector, getHistoryEdgesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables(), config);
            ensureRowDeletingIteratorIsAttached(connector, getHistoryVerticesTableName(config.getTableNamePrefix()));
            ensureRowDeletingIteratorIsAttached(connector, getHistoryEdgesTableName(config.getTableNamePrefix()));
        } else {
            ensureTableExists(connector, getVerticesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables(), config);
            ensureTableExists(connector, getEdgesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables(), config);
        }
        ensureTableExists(connector, getExtendedDataTableName(config.getTableNamePrefix()), config.getExtendedDataMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables(), config);
        ensureTableExists(connector, getDataTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables(), config);
        ensureTableExists(connector, getMetadataTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables(), config);
        ensureRowDeletingIteratorIsAttached(connector, getVerticesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getEdgesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getDataTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getExtendedDataTableName(config.getTableNamePrefix()));
        AccumuloGraph graph = new AccumuloGraph(config, connector);
        graph.setup();
        return graph;
    }

    protected static void ensureTableExists(Connector connector, String tableName, Integer maxVersions, String hdfsContextClasspath, boolean createTable, AccumuloGraphConfiguration config) {
        try {
            if (!connector.tableOperations().exists(tableName)) {
                if (!createTable) {
                    throw new GeException("Table '" + tableName + "' does not exist and 'graph." + GraphOptions.CREATE_TABLES.name() + "' is set to false");
                }
                NewTableConfiguration ntc = new NewTableConfiguration()
                        .setTimeType(TimeType.MILLIS)
                        .withoutDefaultIterators();
                connector.tableOperations().create(tableName, ntc);

                if (maxVersions != null) {
                    // The following parameters match the Accumulo defaults for the VersioningIterator
                    IteratorSetting versioningSettings = new IteratorSetting(
                            ACCUMULO_DEFAULT_VERSIONING_ITERATOR_PRIORITY,
                            ACCUMULO_DEFAULT_VERSIONING_ITERATOR_NAME,
                            VersioningIterator.class
                    );
                    VersioningIterator.setMaxVersions(versioningSettings, maxVersions);
                    EnumSet<IteratorUtil.IteratorScope> scope = EnumSet.allOf(IteratorUtil.IteratorScope.class);
                    connector.tableOperations().attachIterator(tableName, versioningSettings, scope);
                }

                if (tableName.equals(getVerticesTableName(config.getTableNamePrefix()))
                        || tableName.equals(getEdgesTableName(config.getTableNamePrefix()))
                        || tableName.equals(getMetadataTableName(config.getTableNamePrefix()))
                        || tableName.equals(getDataTableName(config.getTableNamePrefix()))
                        || tableName.equals(getHistoryVerticesTableName(config.getTableNamePrefix()))
                        || tableName.equals(getHistoryEdgesTableName(config.getTableNamePrefix()))
                        || tableName.equals(getExtendedDataTableName(config.getTableNamePrefix()))) {
                    // enable block cache for frequently accessed tables
                    connector.tableOperations().setProperty(tableName, "table.cache.block.enable", "true");
                    connector.tableOperations().setProperty(tableName, "table.bloom.enabled", "true");
                }
            }

            if (hdfsContextClasspath != null) {
                connector.instanceOperations().setProperty("general.vfs.context.classpath." + CLASSPATH_CONTEXT_NAME + "-" + tableName, hdfsContextClasspath);
                connector.tableOperations().setProperty(tableName, "table.classpath.context", CLASSPATH_CONTEXT_NAME + "-" + tableName);
            }
        } catch (Exception e) {
            throw new GeException("Unable to create table " + tableName, e);
        }
    }

    protected static void ensureRowDeletingIteratorIsAttached(Connector connector, String tableName) {
        try {
            synchronized (addIteratorLock) {
                IteratorSetting is = new IteratorSetting(ROW_DELETING_ITERATOR_PRIORITY, ROW_DELETING_ITERATOR_NAME, RowDeletingIterator.class);
                if (!connector.tableOperations().listIterators(tableName).containsKey(ROW_DELETING_ITERATOR_NAME)) {
                    try {
                        connector.tableOperations().attachIterator(tableName, is);
                    } catch (Exception ex) {
                        // If many processes are starting up at the same time (see YARN). It's possible that there will be a collision.
                        final int SLEEP_TIME = 5000;
                        LOGGER.warn("Failed to attach RowDeletingIterator. Retrying in %dms.", SLEEP_TIME);
                        Thread.sleep(SLEEP_TIME);
                        if (!connector.tableOperations().listIterators(tableName).containsKey(ROW_DELETING_ITERATOR_NAME)) {
                            connector.tableOperations().attachIterator(tableName, is);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new GeException("Could not attach RowDeletingIterator", e);
        }
    }

    // called from GraphFactory
    @SuppressWarnings("unchecked")
    public static AccumuloGraph create(Map config) {
        return create(new AccumuloGraphConfiguration(config));
    }

    @Override
    protected void addMutations(GeObjectType objectType, StoreMutation... mutations) {
        _addMutations(getWriterFromElementType(objectType), mutations);
        if (isHistoryInSeparateTable() && objectType != GeObjectType.EXTENDED_DATA && objectType != GeObjectType.STREAMING_DATA) {
            _addMutations(getHistoryWriterFromElementType(objectType), mutations);
        }
    }

    protected void _addMutations(BatchWriter writer, StoreMutation... mutations) {
        try {
            for (StoreMutation mutation : mutations) {
                writer.addMutation(toAccumuloMutation(mutation));
            }
            if (getConfiguration().isAutoFlush()) {
                flush();
            }
        } catch (MutationsRejectedException ex) {
            throw new GeException("Could not add mutation", ex);
        }
    }

    public static Mutation toAccumuloMutation(StoreMutation sm) {
        List<StoreColumnUpdate> updates = sm.getUpdates();
        Mutation m = new Mutation(sm.getRow());
        for (int i = 0; i < updates.size(); i++) {
            StoreColumnUpdate update = updates.get(i);
            if (update.isDeleted()) {
                if (update.hasTimestamp()) {
                    m.putDelete(
                            update.getColumnFamily(),
                            update.getColumnQualifier(),
                            new org.apache.accumulo.core.security.ColumnVisibility(update.getColumnVisibility()),
                            update.getTimestamp()
                    );
                } else {
                    m.putDelete(
                            update.getColumnFamily(),
                            update.getColumnQualifier(),
                            new org.apache.accumulo.core.security.ColumnVisibility(update.getColumnVisibility())
                    );
                }
            } else {
                if (update.hasTimestamp()) {
                    m.put(
                            update.getColumnFamily(),
                            update.getColumnQualifier(),
                            new org.apache.accumulo.core.security.ColumnVisibility(update.getColumnVisibility()),
                            update.getTimestamp(),
                            update.getValue()
                    );
                } else {
                    m.put(
                            update.getColumnFamily(),
                            update.getColumnQualifier(),
                            new org.apache.accumulo.core.security.ColumnVisibility(update.getColumnVisibility()),
                            update.getValue()
                    );
                }
            }
        }

        return m;
    }

    public BatchWriter getVerticesWriter() {
        return getWriterForTable(getVerticesTableName());
    }

    private BatchWriter getWriterForTable(String tableName) {
        try {
            return batchWriter.getBatchWriter(tableName);
        } catch (Exception e) {
            throw new GeException("Unable to get writer for table " + tableName, e);
        }
    }

    public BatchWriter getHistoryVerticesWriter() {
        return getWriterForTable(getHistoryVerticesTableName());
    }

    public BatchWriter getEdgesWriter() {
        return getWriterForTable(getEdgesTableName());
    }

    public BatchWriter getHistoryEdgesWriter() {
        return getWriterForTable(getHistoryEdgesTableName());
    }

    public BatchWriter getExtendedDataWriter() {
        return getWriterForTable(getExtendedDataTableName());
    }

    public BatchWriter getDataWriter() {
        return getWriterForTable(getDataTableName());
    }

    public BatchWriter getWriterFromElementType(GeObjectType objectType) {
        switch (objectType) {
            case VERTEX:
                return getVerticesWriter();
            case EDGE:
                return getEdgesWriter();
            case EXTENDED_DATA:
                return getExtendedDataWriter();
            case STREAMING_DATA:
                return getDataWriter();
            default:
                throw new GeException("Unexpected object type: " + objectType);
        }
    }

    public BatchWriter getHistoryWriterFromElementType(GeObjectType objectType) {
        switch (objectType) {
            case VERTEX:
                return getHistoryVerticesWriter();
            case EDGE:
                return getHistoryEdgesWriter();
            default:
                throw new GeException("Unexpected object type: " + objectType);
        }
    }

    protected BatchWriter getMetadataWriter() {
        return getWriterForTable(getMetadataTableName());
    }

    public void logLargeRow(Key key, Value value) {
        if (value.getSize() > largeValueErrorThreshold || value.getSize() > largeValueWarningThreshold) {
            String message = String.format(
                    "large row detected (key: %s, size: %,d\n%s",
                    key,
                    value.getSize(),
                    Arrays.stream(Thread.currentThread().getStackTrace())
                            .map(StackTraceElement::toString)
                            .collect(Collectors.joining("\n  "))
            );
            if (value.getSize() > largeValueErrorThreshold) {
                LOGGER.error("%s", message);
            } else if (value.getSize() > largeValueWarningThreshold) {
                LOGGER.warn("%s", message);
            }
        }
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(
            Element element,
            String key,
            String name,
            Visibility visibility,
            Long startTime,
            Long endTime,
            Authorizations authorizations
    ) {
        Span trace = Trace.start("getHistoricalPropertyValues");
        if (Trace.isTracing()) {
            if (key != null)
                trace.data("key", key);

            if (name != null)
                trace.data("name", name);

            if (visibility != null)
                trace.data("visibility", visibility.getVisibilityString());

            if (startTime != null) {
                trace.data("startTime", Long.toString(startTime));
            }
            if (endTime != null) {
                trace.data("endTime", Long.toString(endTime));
            }
        }
        try {
            ElementType elementType = ElementType.getTypeFromElement(element);

            FetchHints fetchHints = FetchHints.PROPERTIES_AND_METADATA;
            traceDataFetchHints(trace, fetchHints);
            org.apache.accumulo.core.data.Range range = RangeUtils.createRangeFromString(element.getId());
            final ScannerBase scanner = createElementScanner(
                    fetchHints,
                    elementType,
                    ALL_VERSIONS,
                    startTime,
                    endTime,
                    Lists.newArrayList(range),
                    false,
                    authorizations
            );

            try {
                Map<String, HistoricalPropertyValue> results = new HashMap<>();

                ArrayListMultimap<String, String> activeVisibilities = ArrayListMultimap.create();
                Map<String, Key> softDeleteObserved = Maps.newHashMap();
                Map<String, Long> lastPropertyEntryList = Maps.newHashMap();

                for (Map.Entry<Key, Value> column : scanner) {
                    String cq = column.getKey().getColumnQualifier().toString();
                    String columnVisibility = column.getKey().getColumnVisibility().toString();
                    if (column.getKey().getColumnFamily().equals(AccumuloElement.CF_PROPERTY)) {
                        if (visibility != null && !columnVisibility.equals(visibility.getVisibilityString())) {
                            continue;
                        }
                        PropertyColumnQualifier propertyColumnQualifier = KeyHelper.createPropertyColumnQualifier(cq, getNameSubstitutionStrategy());
                        if (name != null && !propertyColumnQualifier.getPropertyName().equals(name)) {
                            continue;
                        }
                        if (key != null && !propertyColumnQualifier.getPropertyKey().equals(key)) {
                            continue;
                        }
                        String resultsKey = propertyColumnQualifier.getDiscriminator(columnVisibility, column.getKey().getTimestamp());
                        long timestamp = column.getKey().getTimestamp();
                        Object value = geSerializer.bytesToObject(element, column.getValue().get());
                        Metadata metadata = Metadata.create();
                        Set<Visibility> hiddenVisibilities = null; // TODO should we preserve these over time
                        if (value instanceof StreamingPropertyValueRef) {
                            value = ((StreamingPropertyValueRef) value).toStreamingPropertyValue(this, timestamp);
                        }
                        String propertyKey = propertyColumnQualifier.getPropertyKey();
                        String propertyName = propertyColumnQualifier.getPropertyName();
                        Visibility propertyVisibility = accumuloVisibilityToVisibility(columnVisibility);

                        HistoricalPropertyValue hpv =
                                new HistoricalPropertyValue.HistoricalPropertyValueBuilder(propertyKey, propertyName, timestamp)
                                        .propertyVisibility(propertyVisibility)
                                        .value(value)
                                        .metadata(metadata)
                                        .hiddenVisibilities(hiddenVisibilities)
                                        .build();

                        String propIdent = propertyKey + ":" + propertyName;
                        activeVisibilities.put(propIdent, columnVisibility);

                        results.put(resultsKey, hpv);

                        // Need to keep track on the last property entry to get the original property metadata on a
                        // soft delete
                        String lastPropKey = propertyColumnQualifier.getDiscriminator(columnVisibility);
                        if (lastPropertyEntryList.containsKey(lastPropKey)) {
                            long lastPropTimestamp = lastPropertyEntryList.get(lastPropKey);
                            if (timestamp > lastPropTimestamp) {
                                lastPropertyEntryList.put(lastPropKey, timestamp);
                            }
                        } else {
                            lastPropertyEntryList.put(lastPropKey, timestamp);
                        }
                    } else if (column.getKey().getColumnFamily().equals(AccumuloElement.CF_PROPERTY_SOFT_DELETE)) {
                        PropertyColumnQualifier propertyColumnQualifier = KeyHelper.createPropertyColumnQualifier(cq, getNameSubstitutionStrategy());
                        String propertyKey = propertyColumnQualifier.getPropertyKey();
                        String propertyName = propertyColumnQualifier.getPropertyName();

                        String propIdent = propertyKey + ":" + propertyName;
                        activeVisibilities.remove(propIdent, columnVisibility);
                        softDeleteObserved.put(propIdent, column.getKey());
                    } else if (column.getKey().getColumnFamily().equals(AccumuloElement.CF_PROPERTY_METADATA)) {
                        PropertyMetadataColumnQualifier propertyMetadataColumnQualifier = KeyHelper.createPropertyMetadataColumnQualifier(cq, getNameSubstitutionStrategy());
                        String resultsKey = propertyMetadataColumnQualifier.getPropertyDiscriminator(column.getKey().getTimestamp());
                        HistoricalPropertyValue hpv = results.get(resultsKey);
                        if (hpv == null) {
                            continue;
                        }
                        com.mware.ge.values.storable.Value value = geSerializer.bytesToObject(element, column.getValue().get());
                        Visibility metadataVisibility = accumuloVisibilityToVisibility(columnVisibility);
                        hpv.getMetadata().add(propertyMetadataColumnQualifier.getMetadataKey(), value, metadataVisibility);
                    }
                }

                for (Key entry : softDeleteObserved.values()) {
                    String cq = entry.getColumnQualifier().toString();
                    PropertyColumnQualifier propertyColumnQualifier = KeyHelper.createPropertyColumnQualifier(cq, getNameSubstitutionStrategy());
                    String propertyKey = propertyColumnQualifier.getPropertyKey();
                    String propertyName = propertyColumnQualifier.getPropertyName();
                    String propIdent = propertyKey + ":" + propertyName;

                    List<String> active = activeVisibilities.get(propIdent);
                    if (active == null || active.isEmpty()) {
                        long timestamp = entry.getTimestamp() + 1;
                        String columnVisibility = entry.getColumnVisibility().toString();
                        Visibility propertyVisibility = accumuloVisibilityToVisibility(columnVisibility);

                        String lastPropertyEntryKey = propertyColumnQualifier.getDiscriminator(columnVisibility);
                        Long propertyTimestamp = lastPropertyEntryList.get(lastPropertyEntryKey);
                        if (propertyTimestamp == null) {
                            throw new GeException("Did not find last property entry timestamp: " + lastPropertyEntryKey);
                        }
                        String resultKey = propertyColumnQualifier.getDiscriminator(columnVisibility, propertyTimestamp);
                        HistoricalPropertyValue property = results.get(resultKey);
                        if (property == null) {
                            throw new GeException("Did not find a matching historical property value for the last property entry timestamp: " + resultKey);
                        }

                        HistoricalPropertyValue hpv =
                                new HistoricalPropertyValue.HistoricalPropertyValueBuilder(propertyKey, propertyName, timestamp)
                                        .propertyVisibility(propertyVisibility)
                                        .metadata(property.getMetadata())
                                        .value(property.getValue())
                                        .isDeleted(true)
                                        .build();

                        String resultsKey = propertyColumnQualifier.getDiscriminator(columnVisibility, timestamp);
                        results.put(resultsKey, hpv);
                    }
                }

                return new TreeSet<>(results.values());
            } finally {
                scanner.close();
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedDataForElements(
            Iterable<? extends ElementId> elementIdsArg,
            String tableName,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        try {
            return super.getExtendedDataForElements(elementIdsArg, tableName, fetchHints, authorizations);
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof AccumuloSecurityException) {
                throw new SecurityGeException("Could not get extended data " + Joiner.on(", ").join(toList(elementIdsArg)) + ":" + tableName + " with authorizations: " + authorizations, authorizations, ex.getCause());
            }
            throw ex;
        }
    }

    @Override
    public void flushGraph() {
        flushWriter(this.batchWriter);
    }

    @Override
    public void flush() {
        flushTimer.time(() -> {
            if (hasEventListeners()) {
                synchronized (this.graphEventQueue) {
                    flushWritersAndSuper();
                    flushGraphEventQueue();
                }
            } else {
                flushWritersAndSuper();
            }
        });
    }

    private void flushWritersAndSuper() {
        flushWriter(this.batchWriter);
        super.flush();
    }

    private void flushGraphEventQueue() {
        GraphEvent graphEvent;
        while ((graphEvent = this.graphEventQueue.poll()) != null) {
            fireGraphEvent(graphEvent);
        }
    }

    private static void flushWriter(MultiTableBatchWriter writer) {
        if (writer == null) {
            return;
        }

        try {
            if (!writer.isClosed()) {
                writer.flush();
            }
        } catch (MutationsRejectedException e) {
            throw new GeException("Unable to flush writer", e);
        }
    }

    @Override
    public AccumuloGraphConfiguration getConfiguration() {
        return (AccumuloGraphConfiguration) super.getConfiguration();
    }

    @Override
    public Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, Authorizations authorizations) throws GeException {
        try {
           return super.getVertex(vertexId, fetchHints, endTime, authorizations);
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof AccumuloSecurityException) {
                throw new SecurityGeException("Could not get vertex " + vertexId + " with authorizations: " + authorizations, authorizations, ex.getCause());
            }
            throw ex;
        }
    }

    @Override
    public Iterable<String> getVertexIds(Authorizations authorizations) {
        return new LookAheadIterable<Map.Entry<Key, Value>, String>() {
            public ScannerBase scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, String dest) {
                return dest != null;
            }

            @Override
            protected String convert(Map.Entry<Key, Value> next) {
                try {
                    ByteArrayInputStream bain = new ByteArrayInputStream(next.getValue().get());
                    try (DataInputStream in = DataInputStreamUtils.decodeHeader(bain, ElementData.TYPE_ID_VERTEX)) {
                        return DataInputStreamUtils.decodeString(in);
                    }
                } catch (IOException ex) {
                    throw new GeException("Could not read vertex", ex);
                }
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                try {
                    scanner = createVertexScanner(
                            FetchHints.NONE,
                            SINGLE_VERSION,
                            null,
                            null,
                            new org.apache.accumulo.core.data.Range(),
                            authorizations
                    );
                    return scanner.iterator();
                } catch (RuntimeException ex) {
                    if (ex.getCause() instanceof AccumuloSecurityException) {
                        throw new SecurityGeException("Could not get vertices with authorizations: " + authorizations, authorizations, ex.getCause());
                    }
                    throw ex;
                }
            }

            @Override
            public void close() {
                super.close();
                if (scanner != null) {
                    scanner.close();
                }
            }
        };
    }

    public ScannerBase createVertexScanner(
            FetchHints fetchHints,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            org.apache.accumulo.core.data.Range range,
            Authorizations authorizations
    ) throws GeException {
        return createElementScanner(fetchHints, ElementType.VERTEX, maxVersions, startTime, endTime, Lists.newArrayList(range), authorizations);
    }

    public ScannerBase createEdgeScanner(
            FetchHints fetchHints,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            org.apache.accumulo.core.data.Range range,
            Authorizations authorizations
    ) throws GeException {
        return createElementScanner(fetchHints, ElementType.EDGE, maxVersions, startTime, endTime, Lists.newArrayList(range), authorizations);
    }

    private ScannerBase createElementScanner(
            FetchHints fetchHints,
            ElementType elementType,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            Authorizations authorizations
    ) throws GeException {
        return createElementScanner(fetchHints, elementType, maxVersions, startTime, endTime, ranges, true, authorizations);
    }

    ScannerBase createElementScanner(
            FetchHints fetchHints,
            ElementType elementType,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            boolean useGeElementIterators,
            Authorizations authorizations
    ) throws GeException {
        try {
            String tableName;
            if (isHistoryInSeparateTable() && (startTime != null || endTime != null || maxVersions == null || maxVersions > 1)) {
                tableName = getHistoryTableNameFromElementType(elementType);
            } else {
                tableName = getTableNameFromElementType(elementType);
            }
            ScannerBase scanner;
            if (ranges == null || ranges.size() == 1) {
                org.apache.accumulo.core.data.Range range = ranges == null ? null : ranges.iterator().next();
                scanner = createScanner(tableName, range, authorizations);
            } else {
                scanner = createBatchScanner(tableName, ranges, authorizations);
            }

            if ((startTime != null || endTime != null) && getConfiguration().getMaxVersions() > 1) {
                IteratorSetting iteratorSetting = new IteratorSetting(
                        80,
                        TimestampFilter.class.getSimpleName(),
                        TimestampFilter.class
                );
                if (startTime != null) {
                    TimestampFilter.setStart(iteratorSetting, startTime, true);
                }
                if (endTime != null) {
                    TimestampFilter.setEnd(iteratorSetting, endTime, true);
                }
                scanner.addScanIterator(iteratorSetting);
            }

            if (maxVersions != null) {
                IteratorSetting versioningIteratorSettings = new IteratorSetting(
                        90,
                        VersioningIterator.class.getSimpleName(),
                        VersioningIterator.class
                );
                VersioningIterator.setMaxVersions(versioningIteratorSettings, maxVersions);
                scanner.addScanIterator(versioningIteratorSettings);
            }

            if (useGeElementIterators) {
                if (elementType == ElementType.VERTEX) {
                    IteratorSetting vertexIteratorSettings = new IteratorSetting(
                            1000,
                            VertexIterator.class.getSimpleName(),
                            VertexIterator.class
                    );
                    VertexIterator.setFetchHints(vertexIteratorSettings, toIteratorFetchHints(fetchHints));
                    VertexIterator.setCompressTransfer(vertexIteratorSettings, compressIteratorTransfers);
                    scanner.addScanIterator(vertexIteratorSettings);
                } else if (elementType == ElementType.EDGE) {
                    IteratorSetting edgeIteratorSettings = new IteratorSetting(
                            1000,
                            EdgeIterator.class.getSimpleName(),
                            EdgeIterator.class
                    );
                    EdgeIterator.setFetchHints(edgeIteratorSettings, toIteratorFetchHints(fetchHints));
                    EdgeIterator.setCompressTransfer(edgeIteratorSettings, compressIteratorTransfers);
                    scanner.addScanIterator(edgeIteratorSettings);
                } else {
                    throw new GeException("Unexpected element type: " + elementType);
                }
            }

            applyFetchHints(scanner, fetchHints, elementType);
            GRAPH_LOGGER.logStartIterator(tableName, scanner);
            return scanner;
        } catch (TableNotFoundException e) {
            throw new GeException(e);
        }
    }

    public IteratorFetchHints toIteratorFetchHints(FetchHints fetchHints) {
        return new IteratorFetchHints(
                fetchHints.isIncludeAllProperties(),
                deflateByteSequences(fetchHints.getPropertyNamesToInclude()),
                fetchHints.isIncludeAllPropertyMetadata(),
                deflateByteSequences(fetchHints.getMetadataKeysToInclude()),
                fetchHints.isIncludeHidden(),
                fetchHints.isIncludeAllEdgeRefs(),
                fetchHints.isIncludeOutEdgeRefs(),
                fetchHints.isIncludeInEdgeRefs(),
                fetchHints.isIncludeEdgeIds(),
                fetchHints.isIncludeEdgeVertexIds(),
                deflate(fetchHints.getEdgeLabelsOfEdgeRefsToInclude()),
                fetchHints.isIncludeEdgeLabelsAndCounts(),
                fetchHints.isIncludeExtendedDataTableNames()
        );
    }

    private ImmutableSet<ByteSequence> deflateByteSequences(ImmutableSet<String> strings) {
        if (strings == null) {
            return null;
        }
        return ImmutableSet.copyOf(
                strings.stream()
                        .map(s -> new ArrayByteSequence(getNameSubstitutionStrategy().deflate(s)))
                        .collect(Collectors.toSet())
        );
    }

    private ImmutableSet<String> deflate(ImmutableSet<String> strings) {
        if (strings == null) {
            return null;
        }
        return ImmutableSet.copyOf(
                strings.stream()
                        .map(s -> getNameSubstitutionStrategy().deflate(s))
                        .collect(Collectors.toSet())
        );
    }

    protected ScannerBase createVertexScanner(
            FetchHints fetchHints,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            Authorizations authorizations
    ) throws GeException {
        return createElementScanner(
                fetchHints,
                ElementType.VERTEX,
                maxVersions,
                startTime,
                endTime,
                ranges,
                authorizations
        );
    }

    protected ScannerBase createEdgeScanner(
            FetchHints fetchHints,
            Integer maxVersions,
            Long startTime,
            Long endTime,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            Authorizations authorizations
    ) throws GeException {
        return createElementScanner(
                fetchHints,
                ElementType.EDGE,
                maxVersions,
                startTime,
                endTime,
                ranges,
                authorizations
        );
    }

    public ScannerBase createBatchScanner(
            String tableName,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            Authorizations authorizations
    ) throws TableNotFoundException {
        org.apache.accumulo.core.security.Authorizations accumuloAuthorizations = toAccumuloAuthorizations(authorizations);
        return createBatchScanner(tableName, ranges, accumuloAuthorizations);
    }

    public ScannerBase createBatchScanner(
            String tableName,
            Collection<org.apache.accumulo.core.data.Range> ranges,
            org.apache.accumulo.core.security.Authorizations accumuloAuthorizations
    ) throws TableNotFoundException {
        GeTabletServerBatchReader scanner = new GeTabletServerBatchReader(
                connector,
                tableName,
                accumuloAuthorizations,
                numberOfQueryThreads
        );
        scanner.setRanges(ranges);
        return scanner;
    }

    public Scanner createScanner(
            String tableName,
            org.apache.accumulo.core.data.Range range,
            Authorizations authorizations
    ) throws TableNotFoundException {
        org.apache.accumulo.core.security.Authorizations accumuloAuthorizations = toAccumuloAuthorizations(authorizations);
        return createScanner(tableName, range, accumuloAuthorizations);
    }

    private Scanner createScanner(
            String tableName,
            org.apache.accumulo.core.data.Range range,
            org.apache.accumulo.core.security.Authorizations accumuloAuthorizations
    ) throws TableNotFoundException {
        Scanner scanner = connector.createScanner(tableName, accumuloAuthorizations);
        if (range != null) {
            scanner.setRange(range);
        }
        return scanner;
    }

    private void applyFetchHints(ScannerBase scanner, FetchHints fetchHints, ElementType elementType) {
        scanner.clearColumns();

        Iterable<Text> columnFamiliesToFetch = getColumnFamiliesToFetch(elementType, fetchHints);
        for (Text columnFamilyToFetch : columnFamiliesToFetch) {
            scanner.fetchColumnFamily(columnFamilyToFetch);
        }
    }

    public static Iterable<Text> getColumnFamiliesToFetch(ElementType elementType, FetchHints fetchHints) {
        List<Text> columnFamiliesToFetch = new ArrayList<>();

        columnFamiliesToFetch.add(AccumuloElement.CF_HIDDEN);
        columnFamiliesToFetch.add(AccumuloElement.CF_SOFT_DELETE);
        columnFamiliesToFetch.add(AccumuloElement.DELETE_ROW_COLUMN_FAMILY);

        if (elementType == ElementType.VERTEX) {
            columnFamiliesToFetch.add(new Text(StorableVertex.CF_SIGNAL));
        } else if (elementType == ElementType.EDGE) {
            columnFamiliesToFetch.add(new Text(StorableEdge.CF_SIGNAL));
            columnFamiliesToFetch.add(new Text(StorableEdge.CF_IN_VERTEX));
            columnFamiliesToFetch.add(new Text(StorableEdge.CF_OUT_VERTEX));
        } else {
            throw new GeException("Unhandled element type: " + elementType);
        }

        if (fetchHints.isIncludeAllEdgeRefs()
                || fetchHints.isIncludeInEdgeRefs()
                || fetchHints.isIncludeEdgeLabelsAndCounts()
                || fetchHints.hasEdgeLabelsOfEdgeRefsToInclude()) {
            columnFamiliesToFetch.add(new Text(StorableVertex.CF_IN_EDGE));
            columnFamiliesToFetch.add(new Text(StorableVertex.CF_IN_EDGE_HIDDEN));
            columnFamiliesToFetch.add(new Text(StorableVertex.CF_IN_EDGE_SOFT_DELETE));
        }
        if (fetchHints.isIncludeAllEdgeRefs()
                || fetchHints.isIncludeOutEdgeRefs()
                || fetchHints.isIncludeEdgeLabelsAndCounts()
                || fetchHints.hasEdgeLabelsOfEdgeRefsToInclude()) {
            columnFamiliesToFetch.add(new Text(StorableVertex.CF_OUT_EDGE));
            columnFamiliesToFetch.add(new Text(StorableVertex.CF_OUT_EDGE_HIDDEN));
            columnFamiliesToFetch.add(new Text(StorableVertex.CF_OUT_EDGE_SOFT_DELETE));
        }
        if (fetchHints.isIncludeProperties()) {
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_HIDDEN);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_SOFT_DELETE);
        }
        if (fetchHints.isIncludePropertyMetadata()) {
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_METADATA);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_HIDDEN);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_SOFT_DELETE);
        }
        if (fetchHints.isIncludeExtendedDataTableNames()) {
            columnFamiliesToFetch.add(AccumuloElement.CF_EXTENDED_DATA);
        }

        return columnFamiliesToFetch;
    }

    public static org.apache.accumulo.core.security.Authorizations toAccumuloAuthorizations(Authorizations authorizations) {
        if (authorizations == null) {
            throw new NullPointerException("authorizations is required");
        }
        return new org.apache.accumulo.core.security.Authorizations(authorizations.getAuthorizations());
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
       try {
           return super.getEdge(edgeId, fetchHints, endTime, authorizations);
       } catch (RuntimeException ex) {
           if (ex.getCause() instanceof AccumuloSecurityException) {
               throw new SecurityGeException("Could not get edge " + edgeId + " with authorizations: " + authorizations, authorizations, ex.getCause());
           }
           throw ex;
       }
    }

    public static ColumnVisibility visibilityToAccumuloVisibility(String visibilityString) {
        return new ColumnVisibility(visibilityString);
    }

    public static ColumnVisibility visibilityToAccumuloVisibility(ByteSequence visibilityBytes) {
        return new ColumnVisibility(ByteSequenceUtils.getBytes(visibilityBytes));
    }

    public static Visibility accumuloVisibilityToVisibility(ColumnVisibility columnVisibility) {
        if (columnVisibility.equals(EMPTY_COLUMN_VISIBILITY)) {
            return Visibility.EMPTY;
        }
        String columnVisibilityString = columnVisibility.toString();
        return accumuloVisibilityToVisibility(columnVisibilityString);
    }

    public static Visibility accumuloVisibilityToVisibility(Text columnVisibility) {
        return accumuloVisibilityToVisibility(columnVisibility.toString());
    }

    public static Visibility accumuloVisibilityToVisibility(String columnVisibilityString) {
        if (columnVisibilityString.startsWith("[") && columnVisibilityString.endsWith("]")) {
            if (columnVisibilityString.length() == 2) {
                return Visibility.EMPTY;
            }
            columnVisibilityString = columnVisibilityString.substring(1, columnVisibilityString.length() - 1);
        }
        if (columnVisibilityString.length() == 0) {
            return Visibility.EMPTY;
        }
        return new Visibility(columnVisibilityString);
    }

    public AccumuloGraphLogger getGraphLogger() {
        return GRAPH_LOGGER;
    }

    public Connector getConnector() {
        return connector;
    }

    public Iterable<IdRange> listVerticesTableSplits() {
        return listTableSplits(getVerticesTableName());
    }

    public Iterable<IdRange> listHistoryVerticesTableSplits() {
        return listTableSplits(getHistoryVerticesTableName());
    }

    public Iterable<IdRange> listEdgesTableSplits() {
        return listTableSplits(getEdgesTableName());
    }

    public Iterable<IdRange> listHistoryEdgesTableSplits() {
        return listTableSplits(getHistoryEdgesTableName());
    }

    public Iterable<IdRange> listDataTableSplits() {
        return listTableSplits(getDataTableName());
    }

    public Iterable<IdRange> listExtendedDataTableSplits() {
        return listTableSplits(getExtendedDataTableName());
    }

    private Iterable<IdRange> listTableSplits(String tableName) {
        try {
            return splitsIterableToRangeIterable(getConnector().tableOperations().listSplits(tableName));
        } catch (Exception ex) {
            throw new GeException("Could not get splits for: " + tableName, ex);
        }
    }

    private Iterable<IdRange> splitsIterableToRangeIterable(final Iterable<Text> splits) {
        String inclusiveStart = null;
        List<IdRange> ranges = new ArrayList<>();
        for (Text split : splits) {
            String exclusiveEnd = new Key(split).getRow().toString();
            ranges.add(new IdRange(inclusiveStart, exclusiveEnd));
            inclusiveStart = exclusiveEnd;
        }
        ranges.add(new IdRange(inclusiveStart, null));
        return ranges;
    }

    @Override
    public void truncate() {
        try {
            this.connector.tableOperations().deleteRows(getDataTableName(), null, null);
            this.connector.tableOperations().deleteRows(getEdgesTableName(), null, null);
            this.connector.tableOperations().deleteRows(getVerticesTableName(), null, null);
            this.connector.tableOperations().deleteRows(getExtendedDataTableName(), null, null);
            this.connector.tableOperations().deleteRows(getMetadataTableName(), null, null);
            if (isHistoryInSeparateTable()) {
                this.connector.tableOperations().deleteRows(getHistoryEdgesTableName(), null, null);
                this.connector.tableOperations().deleteRows(getHistoryVerticesTableName(), null, null);
            }
            getSearchIndex().truncate(this);
        } catch (Exception ex) {
            throw new GeException("Could not delete rows", ex);
        }
    }

    @Override
    public void shutdown() {
        try {
            flush();
            super.shutdown();
            this.streamingPropertyValueStorageStrategy.close();
            this.graphMetadataStore.close();
            this.batchWriter.close();
        } catch (Exception ex) {
            throw new GeException(ex);
        }
    }

    @Override
    public void drop() {
        try {
            flush();
            graphMetadataStore.drop();
            this.streamingPropertyValueStorageStrategy.close();
            this.graphMetadataStore.close();
            this.batchWriter.close();
            dropTableIfExists(getDataTableName());
            dropTableIfExists(getEdgesTableName());
            dropTableIfExists(getVerticesTableName());
            dropTableIfExists(getMetadataTableName());
            dropTableIfExists(getExtendedDataTableName());
            if (isHistoryInSeparateTable()) {
                dropTableIfExists(getHistoryEdgesTableName());
                dropTableIfExists(getHistoryVerticesTableName());
            }
            getSearchIndex().drop(this);
        } catch (Exception ex) {
            throw new GeException("Could not drop tables", ex);
        }
    }

    private void dropTableIfExists(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (this.connector.tableOperations().exists(tableName)) {
            this.connector.tableOperations().delete(tableName);
        }
    }

    @Override
    public Iterable<String> findRelatedEdgeIds(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        Set<String> vertexIdsSet = IterableUtils.toSet(vertexIds);
        Span trace = Trace.start("findRelatedEdges");
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("findRelatedEdges:\n  %s", IterableUtils.join(vertexIdsSet, "\n  "));
            }

            if (vertexIdsSet.size() == 0) {
                return new HashSet<>();
            }

            List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
            for (String vertexId : vertexIdsSet) {
                ranges.add(RangeUtils.createRangeFromString(vertexId));
            }

            Long startTime = null;
            int maxVersions = 1;
            FetchHints fetchHints = FetchHints.builder()
                    .setIncludeOutEdgeRefs(true)
                    .build();
            ScannerBase scanner = createElementScanner(
                    fetchHints,
                    ElementType.VERTEX,
                    maxVersions,
                    startTime,
                    endTime,
                    ranges,
                    false,
                    authorizations
            );

            IteratorSetting edgeRefFilterSettings = new IteratorSetting(
                    1000,
                    EdgeRefFilter.class.getSimpleName(),
                    EdgeRefFilter.class
            );
            EdgeRefFilter.setVertexIds(edgeRefFilterSettings, vertexIdsSet);
            scanner.addScanIterator(edgeRefFilterSettings);

            IteratorSetting vertexEdgeIdIteratorSettings = new IteratorSetting(
                    1001,
                    VertexEdgeIdIterator.class.getSimpleName(),
                    VertexEdgeIdIterator.class
            );
            scanner.addScanIterator(vertexEdgeIdIteratorSettings);

            final long timerStartTime = System.currentTimeMillis();
            try {
                Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
                List<String> edgeIds = new ArrayList<>();
                while (it.hasNext()) {
                    Map.Entry<Key, Value> c = it.next();
                    for (ByteArrayWrapper edgeId : VertexEdgeIdIterator.decodeValue(c.getValue())) {
                        edgeIds.add(new Text(edgeId.getData()).toString());
                    }
                }
                return edgeIds;
            } finally {
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public Iterable<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        Set<String> vertexIdsSet = IterableUtils.toSet(vertexIds);
        Span trace = Trace.start("findRelatedEdgeSummary");
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("findRelatedEdgeSummary:\n  %s", IterableUtils.join(vertexIdsSet, "\n  "));
            }

            if (vertexIdsSet.size() == 0) {
                return new ArrayList<>();
            }

            List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
            for (String vertexId : vertexIdsSet) {
                ranges.add(RangeUtils.createRangeFromString(vertexId));
            }

            Long startTime = null;
            int maxVersions = 1;
            FetchHints fetchHints = FetchHints.builder()
                    .setIncludeOutEdgeRefs(true)
                    .build();
            ScannerBase scanner = createElementScanner(
                    fetchHints,
                    ElementType.VERTEX,
                    maxVersions,
                    startTime,
                    endTime,
                    ranges,
                    false,
                    authorizations
            );

            IteratorSetting edgeRefFilterSettings = new IteratorSetting(
                    1000,
                    EdgeRefFilter.class.getSimpleName(),
                    EdgeRefFilter.class
            );
            EdgeRefFilter.setVertexIds(edgeRefFilterSettings, vertexIdsSet);
            scanner.addScanIterator(edgeRefFilterSettings);

            final long timerStartTime = System.currentTimeMillis();
            try {
                List<RelatedEdge> results = new ArrayList<>();
                Map<String, Long> edgeAddTimestamps = new HashMap<>();
                Map<String, Long> edgeHideOrDeleteTimestamps = new HashMap<>();
                for (Map.Entry<Key, Value> row : scanner) {
                    Text columnFamily = row.getKey().getColumnFamily();
                    Long timestamp = row.getKey().getTimestamp();
                    if (!columnFamily.equals(new Text(StorableVertex.CF_OUT_EDGE))) {
                        if (columnFamily.equals(new Text(StorableVertex.CF_OUT_EDGE_SOFT_DELETE))
                                || columnFamily.equals(new Text(StorableVertex.CF_OUT_EDGE_HIDDEN))) {
                            String edgeId = row.getKey().getColumnQualifier().toString();
                            edgeHideOrDeleteTimestamps.merge(edgeId, timestamp, Math::max);
                        }
                        continue;
                    }

                    StorableEdgeInfo edgeInfo
                            = new StorableEdgeInfo(row.getValue().get(), row.getKey().getTimestamp());
                    String edgeId = row.getKey().getColumnQualifier().toString();
                    String outVertexId = row.getKey().getRow().toString();
                    String inVertexId = edgeInfo.getVertexId();
                    String label = getNameSubstitutionStrategy().inflate(edgeInfo.getLabel());

                    edgeAddTimestamps.merge(edgeId, timestamp, Math::max);

                    results.add(new RelatedEdgeImpl(edgeId, label, outVertexId, inVertexId));
                }
                return results.stream().filter(relatedEdge -> {
                    Long edgeAddedTime = edgeAddTimestamps.get(relatedEdge.getEdgeId());
                    Long edgeDeletedOrHiddenTime = edgeHideOrDeleteTimestamps.get(relatedEdge.getEdgeId());
                    return edgeDeletedOrHiddenTime == null || edgeAddedTime > edgeDeletedOrHiddenTime;
                }).collect(Collectors.toList());
            } finally {
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public Iterable<Path> findPaths(FindPathOptions options, Authorizations authorizations) {
        ProgressCallback progressCallback = options.getProgressCallback();
        if (progressCallback == null) {
            progressCallback = new ProgressCallback() {
                @Override
                public void progress(double progressPercent, Step step, Integer edgeIndex, Integer vertexCount) {
                    LOGGER.debug("findPaths progress %d%%: %s", (int) (progressPercent * 100.0), step.formatMessage(edgeIndex, vertexCount));
                }
            };
        }

        return new AccumuloFindPathStrategy(this, options, progressCallback, authorizations).findPaths();
    }

    @Override
    public Iterable<String> filterEdgeIdsByAuthorization(Iterable<String> edgeIds, String authorizationToMatch, EnumSet<com.mware.ge.ElementFilter> filters, Authorizations authorizations) {
        return filterElementIdsByAuthorization(
                ElementType.EDGE,
                edgeIds,
                authorizationToMatch,
                filters,
                authorizations
        );
    }

    @Override
    public Iterable<String> filterVertexIdsByAuthorization(Iterable<String> vertexIds, String authorizationToMatch, EnumSet<com.mware.ge.ElementFilter> filters, Authorizations authorizations) {
        return filterElementIdsByAuthorization(
                ElementType.VERTEX,
                vertexIds,
                authorizationToMatch,
                filters,
                authorizations
        );
    }

    private Iterable<String> filterElementIdsByAuthorization(ElementType elementType, Iterable<String> elementIds, String authorizationToMatch, EnumSet<com.mware.ge.ElementFilter> filters, Authorizations authorizations) {
        Set<String> elementIdsSet = IterableUtils.toSet(elementIds);
        Span trace = Trace.start("filterElementIdsByAuthorization");
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("filterElementIdsByAuthorization:\n  %s", IterableUtils.join(elementIdsSet, "\n  "));
            }

            if (elementIdsSet.size() == 0) {
                return new ArrayList<>();
            }

            List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
            for (String elementId : elementIdsSet) {
                ranges.add(RangeUtils.createRangeFromString(elementId));
            }

            Long startTime = null;
            Long endTime = null;
            int maxVersions = 1;
            ScannerBase scanner = createElementScanner(
                    FetchHints.ALL_INCLUDING_HIDDEN,
                    elementType,
                    maxVersions,
                    startTime,
                    endTime,
                    ranges,
                    false,
                    authorizations
            );

            IteratorSetting hasAuthorizationFilterSettings = new IteratorSetting(
                    1000,
                    HasAuthorizationFilter.class.getSimpleName(),
                    HasAuthorizationFilter.class
            );
            HasAuthorizationFilter.setAuthorizationToMatch(hasAuthorizationFilterSettings, authorizationToMatch);
            HasAuthorizationFilter.setFilters(hasAuthorizationFilterSettings, filters);
            scanner.addScanIterator(hasAuthorizationFilterSettings);

            final long timerStartTime = System.currentTimeMillis();
            try {
                Set<String> results = new HashSet<>();
                for (Map.Entry<Key, Value> row : scanner) {
                    results.add(row.getKey().getRow().toString());
                }
                return results;
            } finally {
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    protected CloseableIterable<ExtendedDataRow> getExtendedDataRowsInRange(
            List<IdRange> ranges,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        final long timerStartTime = System.currentTimeMillis();

        return new LookAheadIterable<Map.Entry<Key, Value>, ExtendedDataRow>() {
            public ScannerBase scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, ExtendedDataRow dest) {
                return dest != null;
            }

            @Override
            protected ExtendedDataRow convert(Map.Entry<Key, Value> next) {
                try {
                    SortedMap<Key, Value> row = WholeRowIterator.decodeRow(next.getKey(), next.getValue());
                    ExtendedDataRowId extendedDataRowId = StorableKeyHelper.parseExtendedDataRowId(next.getKey().getRow().toString());
                    return createExtendedDataRow(
                            extendedDataRowId,
                            row,
                            fetchHints,
                            geSerializer
                    );
                } catch (IOException e) {
                    throw new GeException("Could not decode row", e);
                }
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                try {
                    scanner = createExtendedDataRowScanner(ranges, authorizations);
                    return scanner.iterator();
                } catch (RuntimeException ex) {
                    if (ex.getCause() instanceof AccumuloSecurityException) {
                        throw new SecurityGeException("Could not get vertices with authorizations: " + authorizations, authorizations, ex.getCause());
                    }
                    throw ex;
                }
            }

            @Override
            public void close() {
                super.close();
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        };
    }

    private ScannerBase createExtendedDataRowScanner(List<IdRange> geRanges, Authorizations authorizations) {
        try {
            String tableName = getExtendedDataTableName();
            ScannerBase scanner;
            List<Range> ranges = geRanges.parallelStream()
                    .map(this::toAccumuloRange)
                    .collect(Collectors.toList());

            if (ranges.size() == 1) {
                org.apache.accumulo.core.data.Range range = ranges.iterator().next();
                scanner = createScanner(tableName, range, authorizations);
            } else {
                scanner = createBatchScanner(tableName, ranges, authorizations);
            }

            IteratorSetting versioningIteratorSettings = new IteratorSetting(
                    90, // versioning needs to happen before combining into one row
                    VersioningIterator.class.getSimpleName(),
                    VersioningIterator.class
            );
            VersioningIterator.setMaxVersions(versioningIteratorSettings, 1);
            scanner.addScanIterator(versioningIteratorSettings);

            IteratorSetting rowIteratorSettings = new IteratorSetting(
                    100,
                    WholeRowIterator.class.getSimpleName(),
                    WholeRowIterator.class
            );
            scanner.addScanIterator(rowIteratorSettings);

            GRAPH_LOGGER.logStartIterator(tableName, scanner);
            return scanner;
        } catch (TableNotFoundException e) {
            throw new GeException(e);
        }
    }

    @Override
    public CloseableIterable<Vertex> getVerticesInRange(
            final IdRange range,
            final FetchHints fetchHints,
            final Long endTime,
            final Authorizations authorizations
    ) {
        final long timerStartTime = System.currentTimeMillis();

        return new LookAheadIterable<Map.Entry<Key, Value>, Vertex>() {
            public ScannerBase scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, Vertex dest) {
                return dest != null;
            }

            @Override
            protected Vertex convert(Map.Entry<Key, Value> next) {
                return createVertexFromIteratorValue(AccumuloGraph.this, next.getKey(), next.getValue(), fetchHints, authorizations);
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                try {
                    scanner = createVertexScanner(fetchHints, SINGLE_VERSION, null, endTime, toAccumuloRange(range), authorizations);
                    return scanner.iterator();
                } catch (RuntimeException ex) {
                    if (ex.getCause() instanceof AccumuloSecurityException) {
                        throw new SecurityGeException("Could not get vertices with authorizations: " + authorizations, authorizations, ex.getCause());
                    }
                    throw ex;
                }
            }

            @Override
            public void close() {
                super.close();
                if (scanner != null) {
                    scanner.close();
                }
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        };
    }

    public static Edge createEdgeFromEdgeIteratorValue(StorableGraph graph, Key key, Value value, FetchHints fetchHints, Authorizations authorizations) {
        try {
            String edgeId;
            Visibility vertexVisibility;
            Iterable<Property> properties;
            Iterable<Visibility> hiddenVisibilities;
            long timestamp;

            ByteArrayInputStream bain = new ByteArrayInputStream(value.get());

            try (DataInputStream in = DataInputStreamUtils.decodeHeader(bain, ElementData.TYPE_ID_EDGE)) {
                edgeId = DataInputStreamUtils.decodeString(in);
                timestamp = in.readLong();
                vertexVisibility = new Visibility(DataInputStreamUtils.decodeString(in));
                hiddenVisibilities = Iterables.transform(DataInputStreamUtils.decodeStringSet(in), new Function<String, Visibility>() {
                    @Nullable
                    @Override
                    public Visibility apply(String input) {
                        return new Visibility(input);
                    }
                });
                List<MetadataEntry> metadataEntries = DataInputStreamUtils.decodeMetadataEntries(in);
                properties = DataInputStreamUtils.decodeProperties(graph, in, metadataEntries, fetchHints);
                ImmutableSet<String> extendedDataTableNames = DataInputStreamUtils.decodeStringSet(in);
                String inVertexId = DataInputStreamUtils.decodeString(in);
                String outVertexId = DataInputStreamUtils.decodeString(in);
                String label = graph.getNameSubstitutionStrategy().inflate(DataInputStreamUtils.decodeString(in));

                return new StorableEdge(
                        graph,
                        edgeId,
                        outVertexId,
                        inVertexId,
                        label,
                        null,
                        vertexVisibility,
                        properties,
                        null,
                        null,
                        hiddenVisibilities,
                        extendedDataTableNames,
                        timestamp,
                        fetchHints,
                        authorizations
                );
            }
        } catch (IOException ex) {
            throw new GeException("Could not read vertex", ex);
        }
    }

    @Override
    public CloseableIterable<Vertex> getVertices(Iterable<String> ids, final FetchHints fetchHints, final Long endTime, final Authorizations authorizations) {
        final List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
        int idCount = 0;
        StringBuilder result = new StringBuilder();
        for (String id : ids) {
            if (idCount > 0) {
                result.append(",");
            }
            ranges.add(RangeUtils.createRangeFromString(id));
            idCount++;
            result.append(id);
        }
        if (ranges.size() == 0) {
            return new EmptyClosableIterable<>();
        }

        final long timerStartTime = System.currentTimeMillis();

        return new LookAheadIterable<Map.Entry<Key, Value>, Vertex>() {
            public ScannerBase scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, Vertex dest) {
                return dest != null;
            }

            @Override
            protected Vertex convert(Map.Entry<Key, Value> row) {
                return createVertexFromIteratorValue(AccumuloGraph.this, row.getKey(), row.getValue(), fetchHints, authorizations);
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                Long startTime = null;
                scanner = createVertexScanner(fetchHints, 1, startTime, endTime, ranges, authorizations);
                return scanner.iterator();
            }

            @Override
            public void close() {
                super.close();
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        };
    }

    @Override
    public CloseableIterable<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        final List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
        int idCount = 0;
        for (String id : ids) {
            ranges.add(RangeUtils.createRangeFromString(id));
            idCount++;
        }
        if (ranges.size() == 0) {
            return new EmptyClosableIterable<>();
        }

        final long timerStartTime = System.currentTimeMillis();

        return new LookAheadIterable<Map.Entry<Key, Value>, Edge>() {
            public ScannerBase scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, Edge dest) {
                return dest != null;
            }

            @Override
            protected Edge convert(Map.Entry<Key, Value> row) {
                return createEdgeFromEdgeIteratorValue(AccumuloGraph.this, row.getKey(), row.getValue(), fetchHints, authorizations);
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                Long startTime = null;
                scanner = createEdgeScanner(fetchHints, 1, startTime, endTime, ranges, authorizations);
                return scanner.iterator();
            }

            @Override
            public void close() {
                super.close();
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        };
    }

    private org.apache.accumulo.core.data.Range toAccumuloRange(IdRange range) {
        if (range == null)
            return null;

        if (range.getPrefix() != null) {
            return Range.prefix(range.getPrefix());
        } else {
            return new org.apache.accumulo.core.data.Range(
                    range.getStart(),
                    range.isInclusiveStart(),
                    range.getEnd(),
                    range.isInclusiveEnd()
            );
        }
    }

    @Override
    public CloseableIterable<Edge> getEdgesInRange(IdRange idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        final long timerStartTime = System.currentTimeMillis();

        return new LookAheadIterable<Map.Entry<Key, Value>, Edge>() {
            public ScannerBase scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, Edge dest) {
                return dest != null;
            }

            @Override
            protected Edge convert(Map.Entry<Key, Value> next) {
                return createEdgeFromEdgeIteratorValue(AccumuloGraph.this, next.getKey(), next.getValue(), fetchHints, authorizations);
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                scanner = createEdgeScanner(fetchHints, SINGLE_VERSION, null, endTime, toAccumuloRange(idRange), authorizations);
                return scanner.iterator();
            }

            @Override
            public void close() {
                super.close();
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        };
    }

    @Override
    protected long getRowCountFromTable(String tableName, String signalColumn, Authorizations authorizations) {
        try {
            LOGGER.debug("BEGIN getRowCountFromTable(%s)", tableName);
            Scanner scanner = createScanner(tableName, null, authorizations);
            try {
                scanner.fetchColumnFamily(new Text(signalColumn));

                IteratorSetting countingIterator = new IteratorSetting(
                        100,
                        CountingIterator.class.getSimpleName(),
                        CountingIterator.class
                );
                scanner.addScanIterator(countingIterator);

                GRAPH_LOGGER.logStartIterator(tableName, scanner);

                long count = 0;
                for (Map.Entry<Key, Value> entry : scanner) {
                    Long countForKey = LongCombiner.FIXED_LEN_ENCODER.decode(entry.getValue().get());
                    LOGGER.debug("getRowCountFromTable(%s): %s: %d", tableName, entry.getKey().getRow(), countForKey);
                    count += countForKey;
                }
                LOGGER.debug("getRowCountFromTable(%s): TOTAL: %d", tableName, count);
                return count;
            } finally {
                scanner.close();
            }
        } catch (TableNotFoundException ex) {
            throw new GeException("Could not get count from table: " + tableName, ex);
        }
    }

    @Override
    public void traceOn(String description) {
        traceOn(description, new HashMap<>());
    }

    @Override
    public void traceOn(String description, Map<String, String> data) {
        if (!distributedTraceEnabled) {
            try {
                ClientConfiguration conf = getConfiguration().getClientConfiguration();
                DistributedTrace.enable(null, AccumuloGraph.class.getSimpleName(), conf);
                distributedTraceEnabled = true;
            } catch (Exception e) {
                throw new GeException("Could not enable DistributedTrace", e);
            }
        }
        if (Trace.isTracing()) {
            throw new GeException("Trace already running");
        }
        Span span = Trace.on(description);
        for (Map.Entry<String, String> dataEntry : data.entrySet()) {
            span.data(dataEntry.getKey(), dataEntry.getValue());
        }

        LOGGER.info("Started trace '%s'", description);
    }

    @Override
    public void traceOff() {
        if (!Trace.isTracing()) {
            throw new GeException("No trace currently running");
        }
        Trace.off();
    }

    private void traceDataFetchHints(Span trace, FetchHints fetchHints) {
        if (Trace.isTracing()) {
            trace.data("fetchHints", fetchHints.toString());
        }
    }

    @Override
    protected Class<? extends com.mware.ge.values.storable.Value> getValueType(com.mware.ge.values.storable.Value value) {
        if (value instanceof StreamingPropertyValueTableRef) {
            return ((StreamingPropertyValueTableRef) value).getValueType();
        }
        return super.getValueType(value);
    }


    private String getHadoopFS() {
        return this.getConfiguration().getHdfsRootDir();
    }

    private String getHadoopUser() {
        return this.getConfiguration().getHdfsUser();
    }

    public String getBackupDir() {
        return this.getConfiguration().getBackupDir();
    }

    @Override
    public GraphBackup getBackupTool(String outputFile) {
        return new HDFSGraphBackup(getHadoopFS(), getBackupDir(), true, getHadoopUser(), outputFile);
    }

    @Override
    public GraphRestore getRestoreTool() {
        return new HDFSGraphRestore(getHadoopFS(), getBackupDir(), true, getHadoopUser());
    }

    public static Vertex createVertexFromIteratorValue(
            StorableGraph graph,
            Key key,
            Value value,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        try {
            String vertexId;
            Visibility vertexVisibility;
            Iterable<Property> properties;
            Set<Visibility> hiddenVisibilities;
            Edges inEdges;
            Edges outEdges;
            long timestamp;

            ByteArrayInputStream bain = new ByteArrayInputStream(value.get());

            try (DataInputStream in = DataInputStreamUtils.decodeHeader(bain, ElementData.TYPE_ID_VERTEX)) {
                vertexId = DataInputStreamUtils.decodeString(in);
                timestamp = in.readLong();
                vertexVisibility = new Visibility(DataInputStreamUtils.decodeString(in));

                ImmutableSet<String> hiddenVisibilityStrings = DataInputStreamUtils.decodeStringSet(in);
                hiddenVisibilities = hiddenVisibilityStrings != null ?
                        hiddenVisibilityStrings.stream().map(Visibility::new).collect(Collectors.toSet()) :
                        null;

                List<MetadataEntry> metadataEntries = DataInputStreamUtils.decodeMetadataEntries(in);
                properties = DataInputStreamUtils.decodeProperties(graph, in, metadataEntries, fetchHints);

                ImmutableSet<String> extendedDataTableNames = DataInputStreamUtils.decodeStringSet(in);
                outEdges = DataInputStreamUtils.decodeEdges(in, graph.getNameSubstitutionStrategy(), fetchHints);
                inEdges = DataInputStreamUtils.decodeEdges(in, graph.getNameSubstitutionStrategy(), fetchHints);
                String conceptType = graph.getNameSubstitutionStrategy().inflate(DataInputStreamUtils.decodeString(in));

                return new StorableVertex(
                        graph,
                        vertexId,
                        conceptType,
                        null,
                        vertexVisibility,
                        properties,
                        null,
                        null,
                        hiddenVisibilities,
                        extendedDataTableNames,
                        inEdges,
                        outEdges,
                        timestamp,
                        fetchHints,
                        authorizations
                );
            }
        } catch (IOException ex) {
            throw new GeException("Could not read vertex", ex);
        }
    }

    public static StorableExtendedDataRow createExtendedDataRow (
            ExtendedDataRowId rowId,
            SortedMap<Key, Value> row,
            FetchHints fetchHints,
            GeSerializer geSerializer
    ) {
        Set<Property> properties = new HashSet<>();
        List<Map.Entry<Key, Value>> entries = new ArrayList<>(row.entrySet());
        entries.sort(Comparator.comparingLong(o -> o.getKey().getTimestamp()));
        for (Map.Entry<Key, Value> rowEntry : entries) {
            Text columnFamily = rowEntry.getKey().getColumnFamily();
            if (columnFamily.equals(AccumuloElement.CF_EXTENDED_DATA)) {
                String[] columnQualifierParts = KeyBase.splitOnValueSeparator(rowEntry.getKey().getColumnQualifier().toString());
                if (columnQualifierParts.length != 1 && columnQualifierParts.length != 2) {
                    throw new GeException("Invalid column qualifier for extended data row: " + rowId + " (expected 1 or 2 parts, found " + columnQualifierParts.length + ")");
                }
                String propertyName = columnQualifierParts[0];
                String propertyKey = columnQualifierParts.length > 1 ? columnQualifierParts[1] : null;
                com.mware.ge.values.storable.Value propertyValue = geSerializer.bytesToObject(rowId, rowEntry.getValue().get());
                long timestamp = rowEntry.getKey().getTimestamp();
                Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(rowEntry.getKey().getColumnVisibility());
                StorableExtendedDataRow.StorableExtendedDataRowProperty prop = new StorableExtendedDataRow.StorableExtendedDataRowProperty(
                        propertyName,
                        propertyKey,
                        propertyValue,
                        fetchHints,
                        timestamp,
                        visibility
                );
                properties.add(prop);
            } else {
                throw new GeException("unhandled column family: " + columnFamily);
            }
        }

        return new StorableExtendedDataRow(rowId, properties, fetchHints);
    }
}
