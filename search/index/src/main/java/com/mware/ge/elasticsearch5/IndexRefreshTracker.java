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

import com.google.common.collect.Lists;
import com.mware.ge.util.GeReadWriteLock;
import com.mware.ge.util.GeStampedLock;
import com.mware.ge.metric.Counter;
import com.mware.ge.metric.GeMetricRegistry;
import com.mware.ge.metric.Timer;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.elasticsearch.client.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IndexRefreshTracker {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(IndexRefreshTracker.class);
    private final GeReadWriteLock lock = new GeStampedLock();
    private final Map<String, Long> indexToMaxRefreshTime = new HashMap<>();
    private final Counter pushCounter;
    private final Timer refreshTimer;

    public IndexRefreshTracker(GeMetricRegistry metricRegistry) {
        this.pushCounter = metricRegistry.getCounter(IndexRefreshTracker.class, "push", "counter");
        this.refreshTimer = metricRegistry.getTimer(IndexRefreshTracker.class, "refresh", "timer");
    }

    public void pushChange(String indexName) {
        lock.executeInWriteLock(() -> {
            pushCounter.increment();
            LOGGER.trace("index added for refresh: %s", indexName);
            indexToMaxRefreshTime.put(indexName, getTime());
        });
    }

    public void pushChanges(Set<String> indexNames) {
        lock.executeInWriteLock(() -> {
            for (String indexName : indexNames) {
                pushCounter.increment();
                LOGGER.trace("index added for refresh: %s", indexName);
                indexToMaxRefreshTime.put(indexName, getTime());
            }
        });
    }

    public void refresh(Client client) {
        long time = getTime();

        Set<String> indexNamesNeedingRefresh = getIndexNamesNeedingRefresh(time);
        if (indexNamesNeedingRefresh.size() > 0) {
            refresh(client, indexNamesNeedingRefresh);
            removeRefreshedIndexNames(indexNamesNeedingRefresh, time);
        }
    }

    protected long getTime() {
        return System.currentTimeMillis();
    }

    public void refresh(Client client, String... indexNames) {
        long time = getTime();

        Set<String> indexNamesNeedingRefresh = getIndexNamesNeedingRefresh(time);
        indexNamesNeedingRefresh.retainAll(Lists.newArrayList(indexNames));
        if (indexNamesNeedingRefresh.size() > 0) {
            refresh(client, indexNamesNeedingRefresh);
            removeRefreshedIndexNames(indexNamesNeedingRefresh, time);
        }
    }

    protected void refresh(Client client, Set<String> indexNamesNeedingRefresh) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("refreshing: %s", String.join(", ", indexNamesNeedingRefresh));
        }
        refreshTimer.time(() -> {
            client.admin().indices().prepareRefresh(indexNamesNeedingRefresh.toArray(new String[0])).execute().actionGet();
        });
    }

    private Set<String> getIndexNamesNeedingRefresh(long time) {
        return lock.executeInReadLock(() ->
                indexToMaxRefreshTime.entrySet().stream()
                        .filter(e -> e.getValue() <= time)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet())
        );
    }

    private void removeRefreshedIndexNames(Set<String> indexNamesNeedingRefresh, long time) {
        lock.executeInWriteLock(() -> {
            for (String indexName : indexNamesNeedingRefresh) {
                if (indexToMaxRefreshTime.getOrDefault(indexName, Long.MAX_VALUE) <= time) {
                    indexToMaxRefreshTime.remove(indexName);
                }
            }
        });
    }
}
