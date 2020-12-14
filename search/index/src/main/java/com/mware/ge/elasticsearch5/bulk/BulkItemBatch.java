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
package com.mware.ge.elasticsearch5.bulk;

import com.mware.ge.GeException;
import com.mware.ge.util.GeReadWriteLock;
import com.mware.ge.util.GeStampedLock;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class BulkItemBatch {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(BulkItemBatch.class);
    private final GeReadWriteLock lock = new GeStampedLock();
    private final int maxBatchSize;
    private final int maxBatchSizeInBytes;
    private final long batchWindowTimeMillis;
    private final Integer logRequestSizeLimit;
    private long lastFlush;
    private LinkedHashMap<String, BulkItem<?>> batch = new LinkedHashMap<>();
    private int currentBatchSizeInBytes = 0;

    public BulkItemBatch(
            int maxBatchSize,
            int maxBatchSizeInBytes,
            Duration batchWindowTime,
            Integer logRequestSizeLimit
    ) {
        this.maxBatchSize = maxBatchSize;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.batchWindowTimeMillis = batchWindowTime.toMillis();
        this.logRequestSizeLimit = logRequestSizeLimit;
        this.lastFlush = System.currentTimeMillis();
    }

    public boolean add(Item item) {
        return lock.executeInWriteLock(() -> {
            String batchKey = getBatchKey(item);
            BulkItem<?> bulkItem = batch.get(batchKey);
            if (!canAdd(item)) {
                return false;
            }

            if (bulkItem != null) {
                // subtract the old size, after we add this item we need to add the new size back in
                currentBatchSizeInBytes -= bulkItem.getSize();
            } else {
                if (item instanceof DeleteItem) {
                    bulkItem = new BulkDeleteItem(
                            item.getIndexName(),
                            item.getType(),
                            item.getDocumentId(),
                            item.getGeObjectId()
                    );
                } else if (item instanceof UpdateItem) {
                    bulkItem = new BulkUpdateItem(
                            item.getIndexName(),
                            item.getType(),
                            item.getDocumentId(),
                            item.getGeObjectId(),
                            ((UpdateItem) item).getSourceElementLocation()
                    );
                } else {
                    throw new GeException("Unhandled item type: " + item.getClass().getName());
                }
                batch.put(batchKey, bulkItem);
            }

            addToBulkItemUnsafe(bulkItem, item);
            logRequestSize(item);
            currentBatchSizeInBytes += bulkItem.getSize();
            return true;
        });
    }

    private String getBatchKey(Item item) {
        return String.format("%s:%s:%s:%s", item.getIndexName(), item.getType(), item.getDocumentId(), item.getClass().getName());
    }

    private boolean canAdd(Item item) {
        if (batch.size() == 0) {
            return true;
        }

        if (batch.size() >= maxBatchSize) {
            return false;
        }

        if (currentBatchSizeInBytes + item.getSize() >= maxBatchSizeInBytes) {
            return false;
        }

        return true;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void addToBulkItemUnsafe(BulkItem<?> bulkItem, Item item) {
        ((BulkItem) bulkItem).add(item);
    }

    private void logRequestSize(Item item) {
        if (logRequestSizeLimit == null) {
            return;
        }
        int sizeInBytes = item.getSize();
        if (sizeInBytes > logRequestSizeLimit) {
            LOGGER.warn("Large document detected (id: %s). Size in bytes: %d", item.getGeObjectId(), sizeInBytes);
        }
    }

    public boolean shouldFlushByTime() {
        return lock.executeInReadLock(() ->
                batch.size() > 0 && ((System.currentTimeMillis() - lastFlush) > batchWindowTimeMillis)
        );
    }

    public int size() {
        return batch.size();
    }

    public List<BulkItem<?>> getItemsAndClear() {
        return lock.executeInWriteLock(() -> {
            List<BulkItem<?>> results = new ArrayList<>(batch.values());
            batch = new LinkedHashMap<>();
            currentBatchSizeInBytes = 0;
            lastFlush = System.currentTimeMillis();
            return results;
        });
    }
}
