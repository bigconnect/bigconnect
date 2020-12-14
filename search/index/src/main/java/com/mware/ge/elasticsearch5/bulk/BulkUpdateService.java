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

import com.google.common.collect.Lists;
import com.mware.ge.ElementId;
import com.mware.ge.ElementLocation;
import com.mware.ge.ExtendedDataRowId;
import com.mware.ge.GeException;
import com.mware.ge.elasticsearch5.Elasticsearch5SearchIndex;
import com.mware.ge.elasticsearch5.IndexRefreshTracker;
import com.mware.ge.metric.GeMetricRegistry;
import com.mware.ge.metric.Histogram;
import com.mware.ge.metric.Timer;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.LimitedLinkedBlockingQueue;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * All updates to Elasticsearch are sent using bulk requests to speed up indexing.
 * <p>
 * Duplicate element updates are collapsed into single updates to reduce the number of refreshes Elasticsearch
 * has to perform. See
 * - https://github.com/elastic/elasticsearch/issues/23792#issuecomment-296149685
 * - https://github.com/debadair/elasticsearch/commit/54cdf40bc5fdecce180ba2e242abca59c7bd1f11
 */
public class BulkUpdateService {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(BulkUpdateService.class);
    private static final String LOGGER_STACK_TRACE_NAME = BulkUpdateService.class.getName() + ".STACK_TRACE";
    static final GeLogger LOGGER_STACK_TRACE = GeLoggerFactory.getLogger(LOGGER_STACK_TRACE_NAME);
    private final Elasticsearch5SearchIndex searchIndex;
    private final IndexRefreshTracker indexRefreshTracker;
    private final LimitedLinkedBlockingQueue<Item> incomingItems = new LimitedLinkedBlockingQueue<>();
    private final OutstandingItemsList outstandingItems = new OutstandingItemsList();
    private final Thread processItemsThread;
    private final Timer flushTimer;
    private final Histogram batchSizeHistogram;
    private final Timer processBatchTimer;
    private final Duration bulkRequestTimeout;
    private final ThreadPoolExecutor ioExecutor;
    private final int maxFailCount;
    private final BulkItemBatch batch;
    private volatile boolean shutdown;

    public BulkUpdateService(
            Elasticsearch5SearchIndex searchIndex,
            IndexRefreshTracker indexRefreshTracker,
            BulkUpdateServiceConfiguration configuration
    ) {
        this.searchIndex = searchIndex;
        this.indexRefreshTracker = indexRefreshTracker;

        this.ioExecutor = new ThreadPoolExecutor(
                configuration.getPoolSize(),
                configuration.getPoolSize(),
                10,
                TimeUnit.SECONDS,
                new LimitedLinkedBlockingQueue<>(configuration.getBacklogSize()),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setDaemon(true);
                    thread.setName("ge-es-processItems-io-" + thread.getId());
                    return thread;
                }
        );

        this.processItemsThread = new Thread(this::processIncomingItemsIntoBatches);
        this.processItemsThread.setName("ge-es-processItems");
        this.processItemsThread.setDaemon(true);
        this.processItemsThread.start();

        this.bulkRequestTimeout = configuration.getBulkRequestTimeout();
        this.maxFailCount = configuration.getMaxFailCount();
        this.batch = new BulkItemBatch(
                configuration.getMaxBatchSize(),
                configuration.getMaxBatchSizeInBytes(),
                configuration.getBatchWindowTime(),
                configuration.getLogRequestSizeLimit()
        );

        GeMetricRegistry metricRegistry = searchIndex.getMetricsRegistry();
        this.flushTimer = metricRegistry.getTimer(BulkUpdateService.class, "flush", "timer");
        this.processBatchTimer = metricRegistry.getTimer(BulkUpdateService.class, "processBatch", "timer");
        this.batchSizeHistogram = metricRegistry.getHistogram(BulkUpdateService.class, "batch", "histogram");
        metricRegistry.getGauge(metricRegistry.createName(BulkUpdateService.class, "outstandingItems", "size"), outstandingItems::size);
    }

    public CompletableFuture<Void> addDelete(
            String indexName,
            String type,
            String docId,
            ElementId elementId
    ) {
        return add(new DeleteItem(indexName, type, docId, elementId));
    }

    public CompletableFuture<Void> addElementUpdate(
            String indexName,
            String type,
            String docId,
            ElementLocation elementLocation,
            Map<String, String> source,
            Map<String, Object> fieldsToSet,
            Collection<String> fieldsToRemove,
            Map<String, String> fieldsToRename,
            boolean existingElement
    ) {
        return add(new UpdateItem(
                indexName,
                type,
                docId,
                elementLocation,
                elementLocation,
                source,
                fieldsToSet,
                fieldsToRemove,
                fieldsToRename,
                existingElement
        ));
    }

    private CompletableFuture<Void> add(Item bulkItem) {
        outstandingItems.add(bulkItem);
        incomingItems.add(bulkItem);
        return bulkItem.getCompletedFuture();
    }

    public CompletableFuture<Void> addExtendedDataUpdate(
            String indexName,
            String type,
            String docId,
            ExtendedDataRowId extendedDataRowId,
            ElementLocation sourceElementLocation,
            Map<String, String> source,
            Map<String, Object> fieldsToSet,
            Collection<String> fieldsToRemove,
            Map<String, String> fieldsToRename,
            boolean existingElement
    ) {
        return add(new UpdateItem(
                indexName,
                type,
                docId,
                extendedDataRowId,
                sourceElementLocation,
                source,
                fieldsToSet,
                fieldsToRemove,
                fieldsToRename,
                existingElement
        ));
    }

    private void complete(BulkItem<?> bulkItem, Exception exception) {
        outstandingItems.removeAll(bulkItem.getItems());
        if (exception == null) {
            bulkItem.complete();
        } else {
            bulkItem.completeExceptionally(exception);
        }
    }

    private boolean filterByRetryTime(Item bulkItem) {
        if (bulkItem.getFailCount() == 0) {
            return true;
        }
        long nextRetryTime = (long) (bulkItem.getCreatedOrLastTriedTime() + (10 * Math.pow(2, bulkItem.getFailCount())));
        long currentTime = System.currentTimeMillis();
        if (nextRetryTime > currentTime) {
            // add it back into incomingItems, it will already be in outstandingItems
            incomingItems.add(bulkItem);
            return false;
        }
        return true;
    }

    public void flush() {
        flushTimer.time(() -> {
            try {
                List<Item> items = outstandingItems.getCopyOfItems();

                // wait for the items to be added to batches
                CompletableFuture.allOf(
                        items.stream()
                                .map(Item::getAddedToBatchFuture)
                                .toArray(CompletableFuture[]::new)
                ).get();

                // flush the current batch
                flushBatch();

                // wait for the items to complete
                CompletableFuture.allOf(
                        items.stream()
                                .map(Item::getCompletedFuture)
                                .toArray(CompletableFuture[]::new)
                ).get();
            } catch (Exception ex) {
                throw new GeException("failed to flush", ex);
            }
        });
    }

    private void flushBatch() {
        List<BulkItem<?>> batchItems = batch.getItemsAndClear();
        if (batchItems.size() > 0) {
            ioExecutor.execute(() -> processBatch(batchItems));
        }
    }

    private void handleFailure(BulkItem<?> bulkItem, BulkItemResponse bulkItemResponse) {
        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
        bulkItem.incrementFailCount();
        if (bulkItem.getFailCount() >= maxFailCount) {
            complete(bulkItem, new BulkGeException("fail count exceeded the max number of failures", failure));
        } else {
            AtomicBoolean retry = new AtomicBoolean(false);
            try {
                searchIndex.handleBulkFailure(bulkItem, bulkItemResponse, retry);
            } catch (Exception ex) {
                complete(bulkItem, ex);
                return;
            }
            if (retry.get()) {
                incomingItems.addAll(bulkItem.getItems());
            } else {
                complete(bulkItem, null);
            }
        }
    }

    private void handleSuccess(BulkItem<?> bulkItem) {
        complete(bulkItem, null);
    }

    private void processBatch(List<BulkItem<?>> bulkItems) {
        processBatchTimer.time(() -> {
            try {
                batchSizeHistogram.update(bulkItems.size());

                BulkRequestBuilder bulkRequestBuilder = searchIndex.getClient().prepareBulk();
                for (BulkItem<?> bulkItem : bulkItems) {
                    bulkItem.addToBulkRequest(searchIndex.getClient(), bulkRequestBuilder);
                }

                outstandingItems.waitForItemToNotBeInflightAndMarkThemAsInflight(bulkItems);
                BulkResponse bulkResponse;
                try {
                    bulkResponse = searchIndex.getClient()
                            .bulk(bulkRequestBuilder.request())
                            .get(bulkRequestTimeout.toMillis(), TimeUnit.MILLISECONDS);
                } finally {
                    outstandingItems.markItemsAsNotInflight(bulkItems);
                }

                Set<String> indexNames = bulkItems.stream()
                        .peek(BulkItem::updateLastTriedTime)
                        .map(BulkItem::getIndexName)
                        .collect(Collectors.toSet());
                indexRefreshTracker.pushChanges(indexNames);

                int itemIndex = 0;
                for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                    BulkItem<?> bulkItem = bulkItems.get(itemIndex++);
                    if (bulkItemResponse.isFailed()) {
                        handleFailure(bulkItem, bulkItemResponse);
                    } else {
                        handleSuccess(bulkItem);
                    }
                }
            } catch (Exception ex) {
                LOGGER.error("bulk request failed", ex);
                // if bulk failed try each item individually
                if (bulkItems.size() > 1) {
                    for (BulkItem<?> bulkItem : bulkItems) {
                        processBatch(Lists.newArrayList(bulkItem));
                    }
                } else {
                    complete(bulkItems.get(0), ex);
                }
            }
        });
    }

    private void processIncomingItemsIntoBatches() {
        while (true) {
            try {
                if (shutdown) {
                    return;
                }

                Item item = incomingItems.poll(100, TimeUnit.MILLISECONDS);
                if (batch.shouldFlushByTime()) {
                    flushBatch();
                }
                if (item == null) {
                    continue;
                }
                try {
                    if (filterByRetryTime(item)) {
                        while (!batch.add(item)) {
                            flushBatch();
                        }
                        item.getAddedToBatchFuture().complete(null);
                    }
                } catch (Exception ex) {
                    LOGGER.error("process item (%s) failed", item, ex);
                    outstandingItems.remove(item);
                    item.completeExceptionally(new GeException("Failed to process item", ex));
                }
            } catch (InterruptedException ex) {
                // we are shutting down so return
                return;
            } catch (Exception ex) {
                LOGGER.error("process items failed", ex);
            }
        }
    }

    public void shutdown() {
        this.shutdown = true;
        try {
            this.processItemsThread.join(10_000);
        } catch (InterruptedException e) {
            // OK
        }

        ioExecutor.shutdown();
    }
}
