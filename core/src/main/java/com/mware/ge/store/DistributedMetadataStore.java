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
package com.mware.ge.store;

import com.mware.ge.GeException;
import com.mware.ge.GraphMetadataEntry;
import com.mware.ge.GraphMetadataStore;
import com.mware.ge.store.mutations.StoreMutation;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.JavaSerializableUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.mware.ge.GraphBase.METADATA_DEFINE_PROPERTY_PREFIX;

public abstract class DistributedMetadataStore extends GraphMetadataStore {
    private final GeLogger LOGGER = GeLoggerFactory.getLogger(DistributedMetadataStore.class);
    private final String ZK_PATH_REPLACEMENT = "[^a-zA-Z]+";
    private final Pattern ZK_PATH_REPLACEMENT_PATTERN = Pattern.compile(ZK_PATH_REPLACEMENT);
    private final String ZK_DEFINE_PROPERTY = METADATA_DEFINE_PROPERTY_PREFIX.replaceAll(ZK_PATH_REPLACEMENT, "");
    private final static String ZK_PATH = "/ge/metadata";

    private final CuratorFramework curatorFramework;
    protected final AbstractStorableGraph graph;
    private final TreeCache treeCache;
    private final Map<String, GraphMetadataEntry> entries = Collections.synchronizedMap(new HashMap<>());
    private final StampedLock stampedLock = new StampedLock();

    public DistributedMetadataStore(AbstractStorableGraph graph) {
        this.graph = graph;

        StorableGraphConfiguration config = (StorableGraphConfiguration) graph.getConfiguration();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        curatorFramework = CuratorFrameworkFactory.newClient(config.getZookeeperServers(), retryPolicy);
        curatorFramework.start();

        this.treeCache = new TreeCache(curatorFramework, ZK_PATH);
        this.treeCache.getListenable().addListener((client, event) -> {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("treeCache event, clearing cache %s", event);
            }
            writeValues(entries::clear);

            if (graph.getSearchIndex() != null)
                graph.getSearchIndex().clearCache();

            invalidatePropertyDefinitions(event);
        });
        try {
            this.treeCache.start();
        } catch (Exception e) {
            throw new GeException("Could not start metadata sync", e);
        }
    }

    protected abstract void write(StoreMutation m) throws IOException;
    protected abstract void delete(StoreMutation m) throws IOException;
    protected abstract Iterable<GraphMetadataEntry> getAllMetadata();

    public void close() {
        this.treeCache.close();
        this.curatorFramework.close();
    }

    public void drop() {
        close();
        try {
            curatorFramework.delete().guaranteed().forPath(ZK_PATH);
        } catch (Exception e) {
        }

        curatorFramework.close();
    }

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("getMetadata");
        }
        return readValues(() -> new ArrayList<>(entries.values()));
    }

    private void ensureMetadataLoaded() {
        if (entries.size() > 0) {
            return;
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("metadata is stale... loading");
        }

        Iterable<GraphMetadataEntry> metadata = getAllMetadata();

        for (GraphMetadataEntry graphMetadataEntry : metadata) {
            entries.put(graphMetadataEntry.getKey(), graphMetadataEntry);
        }
    }


    @Override
    public void reloadMetadata() {
        LOGGER.trace("forcing immediate reload of metadata");
        writeValues(() -> {
            entries.clear();
            ensureMetadataLoaded();
        });
    }

    @Override
    public void setMetadata(String key, Object value) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("setMetadata: %s = %s", key, value);
        }
        try {
            StoreMutation m = new StoreMutation(key);
            byte[] valueBytes = JavaSerializableUtils.objectToBytes(value);
            m.put(StorableElement.METADATA_COLUMN_FAMILY, StorableElement.METADATA_COLUMN_QUALIFIER, valueBytes);
            write(m);
            graph.flush();
        } catch (IOException ex) {
            throw new GeException("Could not add metadata " + key, ex);
        }

        writeValues(() -> {
            entries.clear();
            try {
                signalMetadataChange(key);
            } catch (Exception e) {
                LOGGER.error("Could not notify other nodes via ZooKeeper", e);
            }
        });
    }

    @Override
    public void removeMetadata(String key) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("deleteMetadata: %s", key);
        }
        try {
            StoreMutation m = new StoreMutation(key);
            m.putDelete(StorableElement.DELETE_ROW_COLUMN_FAMILY, StorableElement.DELETE_ROW_COLUMN_QUALIFIER);
            delete(m);
            graph.flush();
        } catch (IOException ex) {
            throw new GeException("Could not add metadata " + key, ex);
        }

        synchronized (entries) {
            entries.clear();
            try {
                signalMetadataChange(key);
            } catch (Exception e) {
                LOGGER.error("Could not notify other nodes via ZooKeeper", e);
            }
        }
    }

    private void invalidatePropertyDefinitions(TreeCacheEvent event) {
        if (event == null || event.getData() == null) {
            return;
        }
        String path = event.getData().getPath();
        byte[] bytes = event.getData().getData();
        if (path == null || bytes == null) {
            return;
        }
        if (!path.startsWith(ZK_PATH + "/" + ZK_DEFINE_PROPERTY)) {
            return;
        }
        String key = new String(bytes, StandardCharsets.UTF_8);
        if (key == null) {
            return;
        }
        String propertyName = key.substring(METADATA_DEFINE_PROPERTY_PREFIX.length());
        LOGGER.debug("invalidating property definition: %s", propertyName);
        graph.invalidatePropertyDefinition(propertyName);
    }

    private void signalMetadataChange(String key) throws Exception {
        String path = ZK_PATH + "/" + ZK_PATH_REPLACEMENT_PATTERN.matcher(key).replaceAll("_");
        LOGGER.debug("signaling change to metadata via path: %s", path);
        byte[] data = key.getBytes(StandardCharsets.UTF_8);
        this.curatorFramework.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(path, data);
    }

    @Override
    public Object getMetadata(String key) {
        return readValues(() -> {
            GraphMetadataEntry e = entries.get(key);
            return e != null ? e.getValue() : null;
        });
    }

    private <T> T readValues(Supplier<T> reader) {
        T result = null;
        long stamp = stampedLock.tryOptimisticRead();
        if (entries.size() > 0) {
            result = reader.get();
        } else {
            stamp = 0;
        }
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.writeLock();
            try {
                ensureMetadataLoaded();
                result = reader.get();
            } finally {
                stampedLock.unlockWrite(stamp);
            }
        }
        return result;
    }

    private void writeValues(Runnable writer) {
        long stamp = stampedLock.writeLock();
        try {
            writer.run();
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }
}
