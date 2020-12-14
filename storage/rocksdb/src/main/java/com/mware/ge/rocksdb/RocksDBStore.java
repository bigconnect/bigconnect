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
package com.mware.ge.rocksdb;

import com.google.common.collect.ImmutableList;
import com.mware.ge.GeException;
import com.mware.ge.IdRange;
import com.mware.ge.collection.Pair;
import com.mware.ge.store.kv.KVKeyUtils;
import com.mware.ge.store.kv.KVStore;
import com.mware.ge.store.kv.ScanIterator;
import com.mware.ge.util.Bytes;
import com.mware.ge.util.Preconditions;
import org.rocksdb.*;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RocksDBStore implements KVStore {
    private final RocksDBGraph graph;
    private final RocksDBGraphConfiguration config;
    private RocksDB rocksdb;
    private Map<String, CFHandle> cfs;
    private AtomicInteger refCount;
    
    public RocksDBStore(RocksDBGraph graph) {
        this.graph = graph;
        this.config = (RocksDBGraphConfiguration) graph.getConfiguration();
    }
    
    @Override
    public void open() {
        try {
            openWithCFs();
        } catch (RocksDBException e) {
            if (e.getMessage().contains("Column family not found")) {
                try {
                    openClean();
                    return;
                } catch (RocksDBException e2) {
                    throw new GeException(e2);
                }
            }

            throw new GeException(e);
        }
    }

    public void openWithCFs() throws RocksDBException {
        // Old CFs should always be opened
        Set<String> mergedCFs = this.mergeOldCFs(config.getDataPath(), Arrays.asList(
                graph.getVerticesTableName(),
                graph.getEdgesTableName(),
                graph.getExtendedDataTableName(),
                graph.getMetadataTableName()
        ));
        List<String> cfs = ImmutableList.copyOf(mergedCFs);
        // Init CFs options
        List<ColumnFamilyDescriptor> cfds = new ArrayList<>(cfs.size());
        for (String cf : cfs) {
            ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(cf.getBytes(StandardCharsets.UTF_8));
            ColumnFamilyOptions options = cfd.getOptions();
            initOptions( null, null, options, options);
            cfds.add(cfd);
        }

        // Init DB options
        DBOptions options = new DBOptions();
        initOptions(options, options, null, null);
        options.setWalDir(config.getWalPath());

        // Open RocksDB with CFs
        List<ColumnFamilyHandle> cfhs = new ArrayList<>();
        this.rocksdb = RocksDB.open(options, config.getDataPath(), cfds, cfhs);
        Preconditions.checkState(cfhs.size() == cfs.size(),
                "Expect same size of cf-handles and cf-names");

        // Collect CF Handles
        this.cfs = new ConcurrentHashMap<>();
        for (int i = 0; i < cfs.size(); i++) {
            this.cfs.put(cfs.get(i), new CFHandle(cfhs.get(i)));
        }

        this.refCount = new AtomicInteger(1);
    }

    public void openClean() throws RocksDBException {
        // Init options
        Options options = new Options();
        initOptions(options, options, options, options);
        options.setWalDir(config.getWalPath());

        /*
         * Open RocksDB at the first time
         * Don't merge old CFs, we expect a clear DB when using this one
         */
        this.rocksdb = RocksDB.open(options, config.getDataPath());

        this.cfs = new ConcurrentHashMap<>();
        this.refCount = new AtomicInteger(1);

        createTables(
                graph.getVerticesTableName(),
                graph.getEdgesTableName(),
                graph.getExtendedDataTableName(),
                graph.getMetadataTableName()
        );
    }

    private void createTables(String... tables) throws RocksDBException {
        this.checkValid();

        List<ColumnFamilyDescriptor> cfds = new ArrayList<>();
        for (String table : tables) {
            if (this.cfs.containsKey(table)) {
                continue;
            }
            ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(table.getBytes(StandardCharsets.UTF_8));
            ColumnFamilyOptions options = cfd.getOptions();
            initOptions(null, null, options, options);
            cfds.add(cfd);
        }

        /*
         * To speed up the creation of tables, like truncate() for tinkerpop
         * test, we call createColumnFamilies instead of createColumnFamily.
         */
        List<ColumnFamilyHandle> cfhs = this.rocksdb.createColumnFamilies(cfds);

        for (ColumnFamilyHandle cfh : cfhs) {
            String table = new String(cfh.getName(), StandardCharsets.UTF_8);
            this.cfs.put(table, new CFHandle(cfh));
        }
    }

    private void checkValid() {
        Preconditions.checkState(this.rocksdb.isOwningHandle(),
                "It seems RocksDB has been closed");
    }

    private RocksDB rocksdb() {
        this.checkValid();
        return this.rocksdb;
    }

    private Set<String> mergeOldCFs(String path, List<String> cfNames) throws RocksDBException {
        Set<String> cfs = listCFs(path);
        cfs.addAll(cfNames);
        return cfs;
    }

    public static Set<String> listCFs(String path) throws RocksDBException {
        Set<String> cfs = new HashSet<>();

        List<byte[]> oldCFs = RocksDB.listColumnFamilies(new Options(), path);
        if (oldCFs.isEmpty()) {
            cfs.add("default");
        } else {
            for (byte[] oldCF : oldCFs) {
                cfs.add(new String(oldCF, StandardCharsets.UTF_8));
            }
        }
        return cfs;
    }

    public Pair<byte[], byte[]> keyRange(RocksDB table) {
        byte[] startKey, endKey;
        try (RocksIterator iter = table.newIterator()) {
            iter.seekToFirst();
            if (!iter.isValid()) {
                return null;
            }
            startKey = iter.key();
            iter.seekToLast();
            if (!iter.isValid()) {
                return Pair.of(startKey, null);
            }
            endKey = iter.key();
        }
        return Pair.of(startKey, endKey);
    }

    @Override
    public ScanIterator scan(String table, IdRange idRange) {
        try (CFHandle cf = cf(table)) {

            if (idRange == null)
                return scan(cf);

            if (idRange.getPrefix() != null) {
                return scan(cf, idRange.getPrefix().getBytes());
            } else if (idRange.getStart() != null && idRange.getEnd() != null) {
                byte[] start = KVKeyUtils.encodeId(idRange.getStart().getBytes());
                byte[] end = KVKeyUtils.encodeId(idRange.getEnd().getBytes());

                int type = idRange.isInclusiveStart() ? ScanIterator.SCAN_GTE_BEGIN : ScanIterator.SCAN_GT_BEGIN;
                type |= idRange.isInclusiveEnd() ? ScanIterator.SCAN_LTE_END : ScanIterator.SCAN_LT_END;
                return scan(cf, start, end, type);
            } else if (idRange.getStart() == null && idRange.getEnd() != null) {
                byte[] end = KVKeyUtils.encodeId(idRange.getEnd().getBytes());
                int type = idRange.isInclusiveEnd() ? ScanIterator.SCAN_LTE_END : ScanIterator.SCAN_LT_END;
                return scan(cf, null, end, type);
            } else if (idRange.getStart() != null && idRange.getEnd() == null) {
                byte[] start = KVKeyUtils.encodeId(idRange.getStart().getBytes());
                int type = idRange.isInclusiveStart() ? ScanIterator.SCAN_GTE_BEGIN : ScanIterator.SCAN_GT_BEGIN;
                return scan(cf, start, null, type);
            } else {
                return scan(cf);
            }
        }
    }

    public ScanIterator scan(CFHandle table) {
        RocksIterator iter = rocksdb().newIterator(table.get());
        return new RocksDBScanIterator(iter, null, null, ScanIterator.SCAN_ANY);
    }

    public ScanIterator scan(CFHandle table, byte[] prefix) {
        RocksIterator iter = rocksdb().newIterator(table.get(), new ReadOptions().setPrefixSameAsStart(true));
        return new RocksDBScanIterator(iter, prefix, null, ScanIterator.SCAN_PREFIX_BEGIN);
    }

    public ScanIterator scan(CFHandle table, byte[] keyFrom, byte[] keyTo, int scanType) {
        /*
         * Not sure if setTotalOrderSeek(true) must be set:
         * ReadOptions options = new ReadOptions();
         * options.setTotalOrderSeek(true);
         */
        RocksIterator iter = rocksdb().newIterator(table.get(), new ReadOptions().setTotalOrderSeek(true));
        return new RocksDBScanIterator(iter, keyFrom, keyTo, scanType);
    }

    @Override
    public void delete(String tableName, byte[] key) {
        try (CFHandle cf = cf(tableName)) {
            rocksdb().delete(cf.get(), key);
        } catch (RocksDBException ex) {
            throw new GeException(ex);
        }
    }

    @Override
    public void put(String tableName, byte[] key, byte[] value) {
        try (CFHandle cf = cf(tableName)) {
            rocksdb().put(cf.get(), key, value);
        } catch (RocksDBException ex) {
            throw new GeException(ex);
        }
    }

    private void initOptions(
            DBOptionsInterface<?> db,
            MutableDBOptionsInterface<?> mdb,
            ColumnFamilyOptionsInterface<?> cf,
            MutableColumnFamilyOptionsInterface<?> mcf
    ) {
        if (db != null) {
            /*
             * Set true then the database will be created if it is missing.
             * should we use options.setCreateMissingColumnFamilies()?
             */
            db.setCreateIfMissing(true);

            if (config.isOptimizeMode()) {
                int processors = Runtime.getRuntime().availableProcessors();
                db.setIncreaseParallelism(Math.max(processors / 2, 1));
                db.setAllowConcurrentMemtableWrite(true);
                db.setEnableWriteThreadAdaptiveYield(true);
            }

            db.setInfoLogLevel(InfoLogLevel.valueOf(
                    config.getLogLevel() + "_LEVEL")
            );
            db.setMaxSubcompactions(4);
            db.setAllowMmapWrites(false);
            db.setAllowMmapReads(false);

            db.setUseDirectReads(false);
            db.setUseDirectIoForFlushAndCompaction(false);

            db.setMaxManifestFileSize(100 * Bytes.MB);

            db.setSkipStatsUpdateOnDbOpen(false);

            db.setMaxFileOpeningThreads(16);

            db.setDbWriteBufferSize(0L);
        }

        if (mdb != null) {
            mdb.setMaxBackgroundJobs(8);
            mdb.setDelayedWriteRate(16L * Bytes.MB);
            mdb.setMaxOpenFiles(-1);
            mdb.setMaxTotalWalSize(-1);
            mdb.setDeleteObsoleteFilesPeriodMicros(1000000 * 6L * 60 * 60);
        }
        
        if (cf != null) {
            if (config.isOptimizeMode()) {
                cf.optimizeLevelStyleCompaction();
                cf.optimizeUniversalStyleCompaction();
            }

            int numLevels = 7;
            List<CompressionType> compressions = Arrays.asList(
                    CompressionType.NO_COMPRESSION,
                    CompressionType.NO_COMPRESSION,
                    CompressionType.SNAPPY_COMPRESSION,
                    CompressionType.SNAPPY_COMPRESSION,
                    CompressionType.SNAPPY_COMPRESSION,
                    CompressionType.SNAPPY_COMPRESSION,
                    CompressionType.SNAPPY_COMPRESSION
            );
            cf.setNumLevels(numLevels);
            cf.setCompactionStyle(CompactionStyle.LEVEL);
            cf.setBottommostCompressionType(CompressionType.NO_COMPRESSION);
            cf.setCompressionPerLevel(compressions);
            cf.setMinWriteBufferNumberToMerge(2);
            cf.setMaxWriteBufferNumberToMaintain(0);

            // https://github.com/facebook/rocksdb/wiki/Block-Cache
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
            long cacheCapacity = 8L * Bytes.MB;
            tableConfig.setBlockCache(new LRUCache(cacheCapacity));

            tableConfig.setPinL0FilterAndIndexBlocksInCache(false);
            tableConfig.setCacheIndexAndFilterBlocks(false);

            tableConfig.setWholeKeyFiltering(true);
            cf.setTableFormatConfig(tableConfig);

            cf.setOptimizeFiltersForHits(true);

            // https://github.com/facebook/rocksdb/tree/master/utilities/merge_operators
            cf.setMergeOperatorName("uint64add"); // uint64add/stringappend
        }

        if (mcf != null) {
            mcf.setCompressionType(CompressionType.SNAPPY_COMPRESSION);

            mcf.setWriteBufferSize(0L);
            mcf.setMaxWriteBufferNumber(6);

            mcf.setMaxBytesForLevelBase(512L * Bytes.MB);
            mcf.setMaxBytesForLevelMultiplier(10.0);

            mcf.setTargetFileSizeBase(64L * Bytes.MB);
            mcf.setTargetFileSizeMultiplier(1);

            mcf.setLevel0FileNumCompactionTrigger(2);
            mcf.setLevel0SlowdownWritesTrigger(20);
            mcf.setLevel0StopWritesTrigger(36);

            mcf.setSoftPendingCompactionBytesLimit(64L * Bytes.GB);
            mcf.setHardPendingCompactionBytesLimit(256L * Bytes.GB);

            if (config.isBulkLoad()) {
                // Disable automatic compaction
                mcf.setDisableAutoCompactions(true);

                int trigger = Integer.MAX_VALUE;
                mcf.setLevel0FileNumCompactionTrigger(trigger);
                mcf.setLevel0SlowdownWritesTrigger(trigger);
                mcf.setLevel0StopWritesTrigger(trigger);

                long limit = Long.MAX_VALUE;
                mcf.setSoftPendingCompactionBytesLimit(limit);
                mcf.setHardPendingCompactionBytesLimit(limit);
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.checkValid();

        if (this.refCount.decrementAndGet() > 0) {
            return;
        }
        assert this.refCount.get() == 0;

        for (CFHandle cf : this.cfs.values()) {
            cf.close();
        }
        this.cfs.clear();

        this.rocksdb.close();
    }

    private CFHandle cf(String cf) {
        CFHandle cfh = this.cfs.get(cf);
        if (cfh == null) {
            throw new GeException(String.format("Table '%s' is not opened", cf));
        }
        cfh.open();
        return cfh;
    }

    private class CFHandle implements Closeable {
        private final ColumnFamilyHandle handle;
        private final AtomicInteger refs;

        public CFHandle(ColumnFamilyHandle handle) {
            Preconditions.checkNotNull(handle, "handle");
            this.handle = handle;
            this.refs = new AtomicInteger(1);
        }

        public synchronized ColumnFamilyHandle get() {
            Preconditions.checkState(this.handle.isOwningHandle(),
                    "It seems CF has been closed");
            return this.handle;
        }

        public synchronized void open() {
            this.refs.incrementAndGet();
        }

        @Override
        public void close() {
            if (this.refs.decrementAndGet() <= 0) {
                this.handle.close();
            }
        }

        public synchronized ColumnFamilyHandle waitForDrop() {
            assert this.refs.get() >= 1;
            // When entering this method, the refs won't increase any more
            final long timeout = TimeUnit.MINUTES.toMillis(30L);
            final long unit = 100L;
            for (long i = 1; this.refs.get() > 1; i++) {
                try {
                    Thread.sleep(unit);
                } catch (InterruptedException ignored) {
                    // 30s rest api timeout may cause InterruptedException
                }
                if (i * unit > timeout) {
                    throw new GeException(String.format("Timeout after %sms to drop CF", timeout));
                }
            }
            assert this.refs.get() == 1;
            return this.handle;
        }

        public synchronized void destroy() {
            this.close();
            assert this.refs.get() == 0 && !this.handle.isOwningHandle();
        }
    }
}
