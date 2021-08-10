package com.mware.ge.hbase;

import com.mware.ge.*;
import com.mware.ge.store.AbstractStorableGraph;
import com.mware.ge.store.StorableEdge;
import com.mware.ge.store.StorableVertex;
import com.mware.ge.store.mutations.StoreColumnUpdate;
import com.mware.ge.store.mutations.StoreMutation;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HBaseGraph extends AbstractStorableGraph<StorableVertex, StorableEdge> implements Traceable {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(HBaseGraph.class);
    private Connection connection;

    protected HBaseGraph(HBaseGraphConfiguration config, Connection connection) {
        super(config);

        this.connection = connection;
        setGraphMetadataStore(new HBaseMetadataStore(this));
    }

    public static HBaseGraph create(HBaseGraphConfiguration config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }

        Connection connection = config.createConnection();
        createMetadataTable(connection, getMetadataTableName(config.getTableNamePrefix()), config.isCreateTables());

//        ensureTableExists(connection, getVerticesTableName(config.getTableNamePrefix()), config.isCreateTables());
//        ensureTableExists(connection, getEdgesTableName(config.getTableNamePrefix()), config.isCreateTables());
//        ensureTableExists(connection, getExtendedDataTableName(config.getTableNamePrefix()), config.isCreateTables());
//        ensureTableExists(connection, getDataTableName(config.getTableNamePrefix()), config.isCreateTables());

        HBaseGraph graph = new HBaseGraph(config, connection);
        graph.setup();
        return graph;
    }

    @Override
    protected void addMutations(GeObjectType objectType, StoreMutation... mutations) {
        _addMutations(getWriterFromElementType(objectType), mutations);
        if (isHistoryInSeparateTable() && objectType != GeObjectType.EXTENDED_DATA && objectType != GeObjectType.STREAMING_DATA) {
            _addMutations(getHistoryWriterFromElementType(objectType), mutations);
        }
    }

    public BufferedMutator getHistoryWriterFromElementType(GeObjectType objectType) {
        switch (objectType) {
            case VERTEX:
                return getHistoryVerticesWriter();
            case EDGE:
                return getHistoryEdgesWriter();
            default:
                throw new GeException("Unexpected object type: " + objectType);
        }
    }

    public BufferedMutator getWriterFromElementType(GeObjectType objectType) {
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

    protected void _addMutations(BufferedMutator writer, StoreMutation... mutations) {
        try {
            for (StoreMutation mutation : mutations) {
                writer.mutate(toHBaseMutations(mutation));
            }
            if (getConfiguration().isAutoFlush()) {
                flush();
            }
        } catch (IOException ex) {
            throw new GeException("Could not add mutation", ex);
        }
    }

    private static void createMetadataTable(Connection connection, String strTableName, boolean createTable) {
        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(strTableName);
            if (!admin.tableExists(tableName)) {
                if (!createTable) {
                    throw new GeException("Table '" + tableName + "' does not exist and 'graph." + GraphConfiguration.CREATE_TABLES + "' is set to false");
                }

                admin.createTable(
                        TableDescriptorBuilder.newBuilder(tableName)
                                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HBaseMetadataStore.METADATA_COLUMN_FAMILY))
                                .build()
                );
            }
        } catch (Exception e) {
            throw new GeException("Unable to create table " + strTableName, e);
        }
    }

    public static List<Mutation> toHBaseMutations(StoreMutation sm) {
        List<StoreColumnUpdate> updates = sm.getUpdates();
        List<Mutation> mutations = new ArrayList<>();
        for (int i = 0; i < updates.size(); i++) {
            StoreColumnUpdate update = updates.get(i);
            if (update.isDeleted()) {
                if (update.hasTimestamp()) {
                    mutations.add(new Delete(sm.getRow(), update.getTimestamp())
                            .addColumn(update.getColumnFamily(), update.getColumnQualifier())
                            .setCellVisibility(new CellVisibility(new String(update.getColumnVisibility())))
                    );
                } else {
                    mutations.add(new Delete(sm.getRow())
                            .addColumn(update.getColumnFamily(), update.getColumnQualifier())
                            .setCellVisibility(new CellVisibility(new String(update.getColumnVisibility())))
                    );
                }
            } else {
                if (update.hasTimestamp()) {
                    mutations.add(new Put(sm.getRow(), update.getTimestamp())
                            .addColumn(update.getColumnFamily(), update.getColumnQualifier(), update.getValue())
                            .setCellVisibility(new CellVisibility(new String(update.getColumnVisibility())))
                    );
                } else {
                    mutations.add(new Put(sm.getRow())
                            .addColumn(update.getColumnFamily(), update.getColumnQualifier(), update.getValue())
                            .setCellVisibility(new CellVisibility(new String(update.getColumnVisibility())))
                    );
                }
            }
        }
        return mutations;
    }

    protected BufferedMutator getMetadataWriter() {
        return getWriterForTable(getMetadataTableName());
    }

    private BufferedMutator getWriterForTable(String tableName) {
        try {
            return connection.getBufferedMutator(TableName.valueOf(tableName));
        } catch (Exception e) {
            throw new GeException("Unable to get writer for table " + tableName, e);
        }
    }

    @Override
    public void drop() {

    }

    @Override
    public void truncate() {

    }

    @Override
    public void flushGraph() {

    }

    @Override
    public void traceOn(String description) {

    }

    @Override
    public void traceOn(String description, Map<String, String> data) {

    }

    @Override
    public void traceOff() {

    }

    @Override
    protected Iterable<ExtendedDataRow> getExtendedDataRowsInRange(List<IdRange> ranges, FetchHints fetchHints, Authorizations authorizations) {
        return null;
    }

    @Override
    public Iterable<Edge> getEdgesInRange(IdRange range, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return null;
    }

    @Override
    public Iterable<Vertex> getVerticesInRange(IdRange idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return null;
    }

    @Override
    protected long getRowCountFromTable(String tableName, String signalColumn, Authorizations authorizations) {
        return 0;
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(Element element, String key, String name, Visibility visibility, Long startTime, Long endTime, Authorizations authorizations) {
        return null;
    }

    public ResultScanner createScanner(String tableName, Authorizations authorizations) throws IOException {
        return connection.getTable(TableName.valueOf(tableName))
                .getScanner(
                        new Scan()
                                .setAuthorizations(new org.apache.hadoop.hbase.security.visibility.Authorizations(authorizations.getAuthorizations())))
                ;
    }
}
