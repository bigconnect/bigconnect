/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.bigconnect.biggraph.backend.store.rocksdb;

import io.bigconnect.biggraph.BigGraphException;
import io.bigconnect.biggraph.backend.BackendException;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.query.Query;
import io.bigconnect.biggraph.backend.serializer.MergeIterator;
import io.bigconnect.biggraph.backend.store.*;
import io.bigconnect.biggraph.backend.store.rocksdb.RocksDBSessions.Session;
import io.bigconnect.biggraph.config.CoreOptions;
import io.bigconnect.biggraph.config.BigConfig;
import io.bigconnect.biggraph.exception.ConnectionException;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.util.*;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class RocksDBStore extends AbstractBackendStore<Session> {

    private static final Logger LOG = Log.logger(RocksDBStore.class);

    private static final BackendFeatures FEATURES = new RocksDBFeatures();

    private final String store;
    private final String database;

    private final BackendStoreProvider provider;
    private final Map<BigType, RocksDBTable> tables;
    private final Map<String, RocksDBTable> olapTables;

    private String dataPath;
    private RocksDBSessions sessions;
    private final Map<BigType, String> tableDiskMapping;
    // DataPath:RocksDB mapping
    private final ConcurrentMap<String, RocksDBSessions> dbs;
    private final ReadWriteLock storeLock;

    private static final String TABLE_GENERAL_KEY = "general";
    private static final String DB_OPEN = "db-open-%s";
    private static final long OPEN_TIMEOUT = 600L;
    /*
     * This is threads number used to concurrently opening RocksDB dbs,
     * 8 is supposed enough due to configurable data disks and
     * disk number of one machine
     */
    private static final int OPEN_POOL_THREADS = 8;
    private boolean isGraphStore;

    public RocksDBStore(final BackendStoreProvider provider,
                        final String database, final String store) {
        this.tables = new HashMap<>();
        this.olapTables = new HashMap<>();

        this.provider = provider;
        this.database = database;
        this.store = store;
        this.sessions = null;
        this.tableDiskMapping = new HashMap<>();
        this.dbs = new ConcurrentHashMap<>();
        this.storeLock = new ReentrantReadWriteLock();

        this.registerMetaHandlers();
    }

    private void registerMetaHandlers() {
        Supplier<List<RocksDBSessions>> dbsGet = () -> {
            List<RocksDBSessions> dbs = new ArrayList<>();
            dbs.add(this.sessions);
            dbs.addAll(this.tableDBMapping().values());
            return dbs;
        };

        this.registerMetaHandler("metrics", (session, meta, args) -> {
            RocksDBMetrics metrics = new RocksDBMetrics(dbsGet.get(), session);
            return metrics.metrics();
        });

        this.registerMetaHandler("compact", (session, meta, args) -> {
            RocksDBMetrics metrics = new RocksDBMetrics(dbsGet.get(), session);
            return metrics.compact();
        });
    }

    protected void registerTableManager(BigType type, RocksDBTable table) {
        this.tables.put(type, table);
    }

    protected void registerTableManager(String name, RocksDBTable table) {
        this.olapTables.put(name, table);
    }

    protected void unregisterTableManager(String name) {
        this.olapTables.remove(name);
    }

    @Override
    protected final RocksDBTable table(BigType type) {
        RocksDBTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table: '%s'", type);
        }
        return table;
    }

    protected final RocksDBTable table(String name) {
        RocksDBTable table = this.olapTables.get(name);
        if (table == null) {
            throw new BackendException("Unsupported table: '%s'", name);
        }
        return table;
    }

    protected List<String> tableNames() {
        List<String> tables = this.tables.values().stream()
                                         .map(BackendTable::table)
                                         .collect(Collectors.toList());
        tables.addAll(this.olapTables());
        return tables;
    }

    protected List<String> olapTables() {
        return this.olapTables.values().stream().map(RocksDBTable::table)
                              .collect(Collectors.toList());
    }

    protected List<String> tableNames(BigType type) {
        return type != BigType.OLAP ? Arrays.asList(this.table(type).table()) :
                                       this.olapTables();
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.database;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public synchronized void open(BigConfig config) {
        LOG.debug("Store open: {}", this.store);

        E.checkNotNull(config, "config");
        String graphStore = config.get(CoreOptions.STORE_GRAPH);
        this.isGraphStore = this.store.equals(graphStore);
        this.dataPath = config.get(RocksDBOptions.DATA_PATH);

        if (this.sessions != null && !this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.store);
            this.useSessions();
            return;
        }

        List<Future<?>> futures = new ArrayList<>();
        ExecutorService openPool = ExecutorUtil.newFixedThreadPool(
                                   OPEN_POOL_THREADS, DB_OPEN);
        // Open base disk
        futures.add(openPool.submit(() -> {
            this.sessions = this.open(config, this.tableNames());
        }));

        // Open tables with optimized disk
        Map<String, String> disks = config.getMap(RocksDBOptions.DATA_DISKS);
        Set<String> openedDisks = new HashSet<>();
        if (!disks.isEmpty()) {
            this.parseTableDiskMapping(disks, this.dataPath);
            for (Entry<BigType, String> e : this.tableDiskMapping.entrySet()) {
                String disk = e.getValue();
                if (openedDisks.contains(disk)) {
                    continue;
                }
                openedDisks.add(disk);
                List<String> tables = this.tableNames(e.getKey());
                futures.add(openPool.submit(() -> {
                    this.open(config, disk, disk, tables);
                }));
            }
        }
        this.waitOpenFinish(futures, openPool);
    }

    private void waitOpenFinish(List<Future<?>> futures,
                                ExecutorService openPool) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Throwable e) {
                throw new BackendException("Failed to open RocksDB store", e);
            }
        }
        if (openPool.isShutdown()) {
            return;
        }

        /*
         * Transfer the session holder from db-open thread to main thread,
         * otherwise once the db-open thread pool is closed, we can no longer
         * close the session created by it, which will cause the rocksdb
         * instance fail to close
         */
        this.useSessions();
        try {
            Consumers.executeOncePerThread(openPool, OPEN_POOL_THREADS,
                                           this::closeSessions);
        } catch (InterruptedException e) {
            throw new BackendException("Failed to close session opened by " +
                                       "open-pool");
        }

        boolean terminated = false;
        openPool.shutdown();
        try {
            terminated = openPool.awaitTermination(OPEN_TIMEOUT,
                                                   TimeUnit.SECONDS);
        } catch (Throwable e) {
            throw new BackendException(
                      "Failed to wait db-open thread pool shutdown", e);
        }
        if (!terminated) {
            LOG.warn("Timeout when waiting db-open thread pool shutdown");
        }
        openPool.shutdownNow();
    }

    protected RocksDBSessions open(BigConfig config, List<String> tableNames) {
        String dataPath = this.wrapPath(config.get(RocksDBOptions.DATA_PATH));
        String walPath = this.wrapPath(config.get(RocksDBOptions.WAL_PATH));
        return this.open(config, dataPath, walPath, tableNames);
    }

    protected RocksDBSessions open(BigConfig config, String dataPath,
                                   String walPath, List<String> tableNames) {
        LOG.info("Opening RocksDB with data path: {}", dataPath);

        RocksDBSessions sessions = null;
        try {
            sessions = this.openSessionPool(config, dataPath,
                                            walPath, tableNames);
        } catch (RocksDBException e) {
            RocksDBSessions origin = this.dbs.get(dataPath);
            if (origin != null) {
                if (e.getMessage().contains("No locks available")) {
                    /*
                     * Open twice, copy a RocksDBSessions reference, since from
                     * v0.11.2 release we don't support multi graphs share
                     * rocksdb instance (before v0.11.2 graphs with different
                     * CF-prefix share one rocksdb instance and data path),
                     * so each graph has its independent data paths, but multi
                     * CFs may share same optimized disk(or optimized disk path).
                     */
                    sessions = origin.copy(config, this.database, this.store);
                }
            }

            if (e.getMessage().contains("Column family not found")) {
                if (this.isSchemaStore()) {
                    LOG.info("Failed to open RocksDB '{}' with database '{}'," +
                             " try to init CF later", dataPath, this.database);
                }
                List<String> none;
                boolean existsOtherKeyspace = existsOtherKeyspace(dataPath);
                if (existsOtherKeyspace) {
                    // Open a keyspace after other keyspace closed
                    // Set to empty list to open old CFs(of other keyspace)
                    none = ImmutableList.of();
                } else {
                    // Before init the first keyspace
                    none = null;
                }
                try {
                    sessions = this.openSessionPool(config, dataPath,
                                                    walPath, none);
                } catch (RocksDBException e1) {
                    e = e1;
                }
                if (sessions == null && !existsOtherKeyspace) {
                    LOG.error("Failed to open RocksDB with default CF, " +
                              "is there data for other programs: {}", dataPath);
                }
            }

            if (sessions == null) {
                // Error after trying other ways
                LOG.error("Failed to open RocksDB '{}'", dataPath, e);
                throw new ConnectionException("Failed to open RocksDB '%s'",
                                              e, dataPath);
            }
        }

        if (sessions != null) {
            // May override the original session pool
            this.dbs.put(dataPath, sessions);
            sessions.session();
            LOG.debug("Store opened: {}", dataPath);
        }

        return sessions;
    }

    protected RocksDBSessions openSessionPool(BigConfig config,
                                              String dataPath, String walPath,
                                              List<String> tableNames)
                                              throws RocksDBException {
        if (tableNames == null) {
            return new RocksDBStdSessions(config, this.database, this.store,
                                          dataPath, walPath);
        } else {
            return new RocksDBStdSessions(config, this.database, this.store,
                                          dataPath, walPath, tableNames);
        }
    }

    protected String wrapPath(String path) {
        // Ensure the `path` exists
        try {
            FileUtils.forceMkdir(FileUtils.getFile(path));
        } catch (IOException e) {
            throw new BackendException(e.getMessage(), e);
        }
        // Join with store type
        return Paths.get(path, this.store).toString();
    }

    protected Map<String, RocksDBSessions> tableDBMapping() {
        Map<String, RocksDBSessions> tableDBMap = InsertionOrderUtil.newMap();
        for (Entry<BigType, String> e : this.tableDiskMapping.entrySet()) {
            BigType type = e.getKey();
            RocksDBSessions db = this.db(e.getValue());
            String key = type != BigType.OLAP ? this.table(type).table() :
                                                 type.string();
            tableDBMap.put(key, db);
        }
        return tableDBMap;
    }

    @Override
    public void close() {
        LOG.debug("Store close: {}", this.store);

        this.checkOpened();
        this.closeSessions();
    }

    @Override
    public boolean opened() {
        this.checkDbOpened();
        return this.sessions.session().opened();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Store {} mutation: {}", this.store, mutation);
            }

            for (BigType type : mutation.types()) {
                Session session = this.session(type);
                for (Iterator<BackendAction> it = mutation.mutation(type);
                     it.hasNext();) {
                    this.mutate(session, it.next());
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    private void mutate(Session session, BackendAction item) {
        BackendEntry entry = item.entry();

        RocksDBTable table;
        if (!entry.olap()) {
            // Oltp table
            table = this.table(entry.type());
        } else {
            if (entry.type().isIndex()) {
                // Olap index
                table = this.table(this.olapTableName(entry.type()));
            } else {
                // Olap vertex
                table = this.table(this.olapTableName(entry.subId()));
            }
            session = this.session(BigType.OLAP);
        }
        switch (item.action()) {
            case INSERT:
                table.insert(session, entry);
                break;
            case DELETE:
                table.delete(session, entry);
                break;
            case APPEND:
                table.append(session, entry);
                break;
            case ELIMINATE:
                table.eliminate(session, entry);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported mutate action: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();

            BigType tableType = RocksDBTable.tableType(query);
            RocksDBTable table;
            Session session;
            if (query.olap()) {
                table = this.table(this.olapTableName(tableType));
                session = this.session(BigType.OLAP);
            } else {
                table = this.table(tableType);
                session = this.session(tableType);
            }

            Iterator<BackendEntry> entries = table.query(session, query);
            // Merge olap results as needed
            Set<Id> olapPks = query.olapPks();
            if (this.isGraphStore && !olapPks.isEmpty()) {
                List<Iterator<BackendEntry>> iterators = new ArrayList<>();
                for (Id pk : olapPks) {
                    Query q = query.copy();
                    table = this.table(this.olapTableName(pk));
                    iterators.add(table.query(this.session(BigType.OLAP), q));
                }
                entries = new MergeIterator<>(entries, iterators,
                                              BackendEntry::mergable);
            }
            return entries;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Number queryNumber(Query query) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();

            BigType tableType = RocksDBTable.tableType(query);
            RocksDBTable table = this.table(tableType);
            return table.queryNumber(this.session(tableType), query);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public synchronized void init() {
        Lock writeLock = this.storeLock.writeLock();
        writeLock.lock();
        try {
            this.checkDbOpened();

            // Create tables with main disk
            this.createTable(this.sessions,
                             this.tableNames().toArray(new String[0]));

            // Create table with optimized disk
            Map<String, RocksDBSessions> tableDBMap = this.tableDBMapping();
            for (Entry<String, RocksDBSessions> e : tableDBMap.entrySet()) {
                if (e.getKey().equals(BigType.OLAP.string())) {
                    for (String olapTable : this.olapTables()) {
                        this.createTable(e.getValue(), olapTable);
                    }
                } else {
                    this.createTable(e.getValue(), e.getKey());
                }
            }

            LOG.debug("Store initialized: {}", this.store);
        } finally {
            writeLock.unlock();
        }
    }

    protected void createTable(RocksDBSessions db, String... tables) {
        try {
            db.createTable(tables);
        } catch (RocksDBException e) {
            throw new BackendException("Failed to create tables %s for '%s'",
                                       e, Arrays.asList(tables), this.store);
        }
    }

    @Override
    public synchronized void clear(boolean clearSpace) {
        Lock writeLock = this.storeLock.writeLock();
        writeLock.lock();
        try {
            this.checkDbOpened();

            // Drop tables with main disk
            this.dropTable(this.sessions,
                           this.tableNames().toArray(new String[0]));

            // Drop tables with optimized disk
            Map<String, RocksDBSessions> tableDBMap = this.tableDBMapping();
            for (Entry<String, RocksDBSessions> e : tableDBMap.entrySet()) {
                if (e.getKey().equals(BigType.OLAP.string())) {
                    for (String olapTable : this.olapTables()) {
                        this.dropTable(e.getValue(), olapTable);
                    }
                } else {
                    this.dropTable(e.getValue(), e.getKey());
                }
            }

            LOG.debug("Store cleared: {}", this.store);
        } finally {
            writeLock.unlock();
        }
    }

    protected void dropTable(RocksDBSessions db, String... tables) {
        try {
            db.dropTable(tables);
        } catch (BackendException e) {
            if (e.getMessage().contains("is not opened")) {
                return;
            }
            throw e;
        } catch (RocksDBException e) {
            throw new BackendException("Failed to drop tables %s for '%s'",
                                       e, Arrays.asList(tables), this.store);
        }
    }

    @Override
    public boolean initialized() {
        this.checkDbOpened();

        if (!this.opened()) {
            return false;
        }
        for (String table : this.tableNames()) {
            if (!this.sessions.existsTable(table)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized void truncate() {
        Lock writeLock = this.storeLock.writeLock();
        writeLock.lock();
        try {
            this.checkOpened();

            this.clear(false);
            this.init();
            // Clear write-batch
            this.dbs.values().forEach(BackendSessionPool::forceResetSessions);
            LOG.debug("Store truncated: {}", this.store);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void beginTx() {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();

            for (Session session : this.session()) {
                assert !session.hasChanges();
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void commitTx() {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();
            // Unable to guarantee atomicity when committing multi sessions
            for (Session session : this.session()) {
                Object count = session.commit();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Store {} committed {} items", this.store, count);
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void rollbackTx() {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();

            for (Session session : this.session()) {
                session.rollback();
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    protected Session session(BigType tableType) {
        this.checkOpened();

        // Optimized disk
        String disk = this.tableDiskMapping.get(tableType);
        if (disk != null) {
            return this.db(disk).session();
        }

        return this.sessions.session();
    }

    @Override
    public Map<String, String> createSnapshot(String snapshotPrefix) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            Map<String, String> uniqueSnapshotDirMaps = new HashMap<>();
            // Every rocksdb instance should create an snapshot
            for (Entry<String, RocksDBSessions> entry : this.dbs.entrySet()) {
                // Like: parent_path/rocksdb-data/*, * maybe g,m,s
                Path originDataPath = Paths.get(entry.getKey()).toAbsolutePath();
                Path parentParentPath = originDataPath.getParent().getParent();
                // Like: rocksdb-data/*
                Path pureDataPath = parentParentPath.relativize(originDataPath);
                // Like: parent_path/snapshot_rocksdb-data/*
                Path snapshotPath = parentParentPath.resolve(snapshotPrefix +
                                                             "_" + pureDataPath);
                LOG.debug("Create snapshot '{}' for origin data path '{}'",
                          snapshotPath, originDataPath);
                RocksDBSessions sessions = entry.getValue();
                sessions.createSnapshot(snapshotPath.toString());

                String snapshotDir = snapshotPath.getParent().toString();
                // Find correspond data HugeType key
                String diskTableKey = this.findDiskTableKeyByPath(
                                      entry.getKey());
                uniqueSnapshotDirMaps.put(snapshotDir, diskTableKey);
            }
            LOG.info("The store '{}' create snapshot successfully", this);
            return uniqueSnapshotDirMaps;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void resumeSnapshot(String snapshotPrefix, boolean deleteSnapshot) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            if (!this.opened()) {
                return;
            }
            Map<String, RocksDBSessions> snapshotPaths = new HashMap<>();
            for (Entry<String, RocksDBSessions> entry : this.dbs.entrySet()) {
                RocksDBSessions sessions = entry.getValue();
                String snapshotPath = sessions.buildSnapshotPath(snapshotPrefix);
                LOG.debug("The origin data path: {}", entry.getKey());
                if (!deleteSnapshot) {
                    snapshotPath = sessions.hardLinkSnapshot(snapshotPath);
                }
                LOG.debug("The snapshot data path: {}", snapshotPath);
                snapshotPaths.put(snapshotPath, sessions);
            }

            for (Entry<String, RocksDBSessions> entry :
                 snapshotPaths.entrySet()) {
                String snapshotPath = entry.getKey();
                RocksDBSessions sessions = entry.getValue();
                sessions.resumeSnapshot(snapshotPath);

                if (deleteSnapshot) {
                    // Delete empty snapshot parent directory
                    Path parentPath = Paths.get(snapshotPath).getParent();
                    if (Files.list(parentPath).count() == 0) {
                        FileUtils.deleteDirectory(parentPath.toFile());
                    }
                }
            }
            LOG.info("The store '{}' resume snapshot successfully", this);
        } catch (RocksDBException | IOException e) {
            throw new BackendException("Failed to resume snapshot", e);
        } finally {
            readLock.unlock();
        }
    }

    private final void useSessions() {
        for (RocksDBSessions sessions : this.sessions()) {
            sessions.useSession();
        }
    }

    private final void closeSessions() {
        Iterator<Entry<String, RocksDBSessions>> iter = this.dbs.entrySet()
                                                                    .iterator();
        while (iter.hasNext()) {
            Entry<String, RocksDBSessions> entry = iter.next();
            RocksDBSessions sessions = entry.getValue();
            boolean closed = sessions.close();
            if (closed) {
                iter.remove();
            }
        }
    }

    private final List<Session> session() {
        this.checkOpened();

        if (this.tableDiskMapping.isEmpty()) {
            return Arrays.asList(this.sessions.session());
        }

        // Collect session of each table with optimized disk
        List<Session> list = new ArrayList<>(this.tableDiskMapping.size() + 1);
        list.add(this.sessions.session());
        for (String disk : this.tableDiskMapping.values()) {
            list.add(db(disk).session());
        }
        return list;
    }

    private final Collection<RocksDBSessions> sessions() {
        return this.dbs.values();
    }

    private final void parseTableDiskMapping(Map<String, String> disks,
                                             String dataPath) {
        this.tableDiskMapping.clear();
        for (Entry<String, String> disk : disks.entrySet()) {
            // The format of `disk` like: `graph/vertex: /path/to/disk1`
            String name = disk.getKey();
            String path = disk.getValue();
            E.checkArgument(!dataPath.equals(path), "Invalid disk path" +
                            "(can't be the same as data_path): '%s'", path);
            E.checkArgument(!name.isEmpty() && !path.isEmpty(),
                            "Invalid disk format: '%s', expect `NAME:PATH`",
                            disk);
            String[] pair = name.split("/", 2);
            E.checkArgument(pair.length == 2,
                            "Invalid disk key format: '%s', " +
                            "expect `STORE/TABLE`", name);
            String store = pair[0].trim();
            BigType table = BigType.valueOf(pair[1].trim().toUpperCase());
            if (this.store.equals(store)) {
                path = this.wrapPath(path);
                this.tableDiskMapping.put(table, path);
            }
        }
    }

    @SuppressWarnings("unused")
    private Map<String, String> reportDiskMapping() {
        Map<String, String> diskMapping = new HashMap<>();
        diskMapping.put(TABLE_GENERAL_KEY, this.dataPath);
        for (Entry<BigType, String> e : this.tableDiskMapping.entrySet()) {
            String key = this.store + "/" + e.getKey().name();
            String value = Paths.get(e.getValue()).getParent().toString();
            diskMapping.put(key, value);
        }
        return diskMapping;
    }

    private String findDiskTableKeyByPath(String diskPath) {
        String diskTableKey = TABLE_GENERAL_KEY;
        for (Entry<BigType, String> e : this.tableDiskMapping.entrySet()) {
            if (diskPath.equals(e.getValue())) {
                diskTableKey = this.store + "/" + e.getKey().name();
                break;
            }
        }
        return diskTableKey;
    }

    private final void checkDbOpened() {
        E.checkState(this.sessions != null && !this.sessions.closed(),
                     "RocksDB has not been opened");
    }

    protected RocksDBSessions db(BigType tableType) {
        this.checkOpened();

        // Optimized disk
        String disk = this.tableDiskMapping.get(tableType);
        if (disk != null) {
            return this.db(disk);
        }

        return this.sessions;
    }

    private RocksDBSessions db(String disk) {
        RocksDBSessions db = this.dbs.get(disk);
        E.checkState(db != null && !db.closed(),
                     "RocksDB store has not been opened: %s", disk);
        return db;
    }

    private static boolean existsOtherKeyspace(String dataPath) {
        Set<String> cfs;
        try {
            cfs = RocksDBStdSessions.listCFs(dataPath);
        } catch (RocksDBException e) {
            return false;
        }

        int matched = 0;
        for (String cf : cfs) {
            if (cf.endsWith(RocksDBTables.PropertyKey.TABLE) ||
                cf.endsWith(RocksDBTables.VertexLabel.TABLE) ||
                cf.endsWith(RocksDBTables.EdgeLabel.TABLE) ||
                cf.endsWith(RocksDBTables.IndexLabel.TABLE) ||
                cf.endsWith(RocksDBTables.SecondaryIndex.TABLE) ||
                cf.endsWith(RocksDBTables.SearchIndex.TABLE) ||
                cf.endsWith(RocksDBTables.RangeIntIndex.TABLE) ||
                cf.endsWith(RocksDBTables.RangeFloatIndex.TABLE) ||
                cf.endsWith(RocksDBTables.RangeLongIndex.TABLE) ||
                cf.endsWith(RocksDBTables.RangeDoubleIndex.TABLE)) {
                if (++matched >= 3) {
                    return true;
                }
            }
        }
        return false;
    }

    /***************************** Store defines *****************************/

    public static class RocksDBSchemaStore extends RocksDBStore {

        private final RocksDBTables.Counters counters;

        public RocksDBSchemaStore(BackendStoreProvider provider,
                                  String database, String store) {
            super(provider, database, store);

            this.counters = new RocksDBTables.Counters(database);

            registerTableManager(BigType.VERTEX_LABEL,
                                 new RocksDBTables.VertexLabel(database));
            registerTableManager(BigType.EDGE_LABEL,
                                 new RocksDBTables.EdgeLabel(database));
            registerTableManager(BigType.PROPERTY_KEY,
                                 new RocksDBTables.PropertyKey(database));
            registerTableManager(BigType.INDEX_LABEL,
                                 new RocksDBTables.IndexLabel(database));
            registerTableManager(BigType.SECONDARY_INDEX,
                                 new RocksDBTables.SecondaryIndex(database));
        }

        @Override
        protected List<String> tableNames() {
            List<String> tableNames = super.tableNames();
            tableNames.add(this.counters.table());
            return tableNames;
        }

        @Override
        public void increaseCounter(BigType type, long increment) {
            Lock readLock = super.storeLock.readLock();
            readLock.lock();
            try {
                super.checkOpened();
                Session session = super.sessions.session();
                this.counters.increaseCounter(session, type, increment);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public long getCounter(BigType type) {
            Lock readLock = super.storeLock.readLock();
            readLock.lock();
            try {
                super.checkOpened();
                Session session = super.sessions.session();
                return this.counters.getCounter(session, type);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class RocksDBGraphStore extends RocksDBStore {

        public RocksDBGraphStore(BackendStoreProvider provider,
                                 String database, String store) {
            super(provider, database, store);

            registerTableManager(BigType.VERTEX,
                                 new RocksDBTables.Vertex(database));

            registerTableManager(BigType.EDGE_OUT,
                                 RocksDBTables.Edge.out(database));
            registerTableManager(BigType.EDGE_IN,
                                 RocksDBTables.Edge.in(database));

            registerTableManager(BigType.SECONDARY_INDEX,
                                 new RocksDBTables.SecondaryIndex(database));
            registerTableManager(BigType.VERTEX_LABEL_INDEX,
                                 new RocksDBTables.VertexLabelIndex(database));
            registerTableManager(BigType.EDGE_LABEL_INDEX,
                                 new RocksDBTables.EdgeLabelIndex(database));
            registerTableManager(BigType.RANGE_INT_INDEX,
                                 new RocksDBTables.RangeIntIndex(database));
            registerTableManager(BigType.RANGE_FLOAT_INDEX,
                                 new RocksDBTables.RangeFloatIndex(database));
            registerTableManager(BigType.RANGE_LONG_INDEX,
                                 new RocksDBTables.RangeLongIndex(database));
            registerTableManager(BigType.RANGE_DOUBLE_INDEX,
                                 new RocksDBTables.RangeDoubleIndex(database));
            registerTableManager(BigType.SEARCH_INDEX,
                                 new RocksDBTables.SearchIndex(database));
            registerTableManager(BigType.SHARD_INDEX,
                                 new RocksDBTables.ShardIndex(database));
            registerTableManager(BigType.UNIQUE_INDEX,
                                 new RocksDBTables.UniqueIndex(database));

            registerTableManager(this.olapTableName(BigType.SECONDARY_INDEX),
                                 new RocksDBTables.OlapSecondaryIndex(store));
            registerTableManager(this.olapTableName(BigType.RANGE_INT_INDEX),
                                 new RocksDBTables.OlapRangeIntIndex(store));
            registerTableManager(this.olapTableName(BigType.RANGE_LONG_INDEX),
                                 new RocksDBTables.OlapRangeLongIndex(store));
            registerTableManager(this.olapTableName(BigType.RANGE_FLOAT_INDEX),
                                 new RocksDBTables.OlapRangeFloatIndex(store));
            registerTableManager(this.olapTableName(BigType.RANGE_DOUBLE_INDEX),
                                 new RocksDBTables.OlapRangeDoubleIndex(store));
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(BigType type) {
            throw new UnsupportedOperationException(
                      "RocksDBGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(BigType type, long num) {
            throw new UnsupportedOperationException(
                      "RocksDBGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(BigType type) {
            throw new UnsupportedOperationException(
                      "RocksDBGraphStore.getCounter()");
        }

        @Override
        public void createOlapTable(Id pkId) {
            RocksDBTable table = new RocksDBTables.OlapTable(this.store(), pkId);
            this.createTable(this.db(BigType.OLAP), table.table());
            registerTableManager(this.olapTableName(pkId), table);
        }

        @Override
        public void checkAndRegisterOlapTable(Id pkId) {
            RocksDBTable table = new RocksDBTables.OlapTable(this.store(), pkId);
            if (!super.sessions.existsTable(table.table())) {
                throw new BigGraphException("Not exist table '%s''", table.table());
            }
            registerTableManager(this.olapTableName(pkId), table);
        }

        @Override
        public void clearOlapTable(Id pkId) {
            String name = this.olapTableName(pkId);
            RocksDBTable table = this.table(name);
            RocksDBSessions db = this.db(BigType.OLAP);
            if (table == null || !db.existsTable(table.table())) {
                throw new BigGraphException("Not exist table '%s''", name);
            }
            this.dropTable(db, table.table());
            this.createTable(db, table.table());
        }

        @Override
        public void removeOlapTable(Id pkId) {
            String name = this.olapTableName(pkId);
            RocksDBTable table = this.table(name);
            RocksDBSessions db = this.db(BigType.OLAP);
            if (table == null || !db.existsTable(table.table())) {
                throw new BigGraphException("Not exist table '%s''", name);
            }
            this.dropTable(db, table.table());
            this.unregisterTableManager(this.olapTableName(pkId));
        }
    }
}
