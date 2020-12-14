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

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class OutstandingItemsList {
    private final Map<String, Item> outstandingItems = new LinkedHashMap<>();
    private final Set<String> inflightItems = new HashSet<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition itemsChanged = lock.newCondition();

    public void add(Item bulkItem) {
        lock.lock();
        try {
            outstandingItems.put(bulkItem.getDocumentId(), bulkItem);
            itemsChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void remove(Item item) {
        lock.lock();
        try {
            outstandingItems.remove(item.getDocumentId());
            itemsChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void removeAll(Collection<? extends Item> items) {
        lock.lock();
        try {
            outstandingItems.keySet().removeAll(items.stream().map(Item::getDocumentId).collect(Collectors.toSet()));
            itemsChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        return outstandingItems.size();
    }

    public void markItemsAsNotInflight(List<BulkItem<?>> bulkItems) {
        lock.lock();
        try {
            for (BulkItem<?> bulkItem : bulkItems) {
                inflightItems.remove(getInflightKey(bulkItem));
            }
            itemsChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private String getInflightKey(BulkItem<?> bulkItem) {
        return String.format("%s:%s:%s", bulkItem.getIndexName(), bulkItem.getType(), bulkItem.getDocumentId());
    }

    public void waitForItemToNotBeInflightAndMarkThemAsInflight(List<BulkItem<?>> bulkItems) {
        lock.lock();
        try {
            while (true) {
                boolean hasInflightItems = false;
                for (BulkItem<?> bulkItem : bulkItems) {
                    if (inflightItems.contains(getInflightKey(bulkItem))) {
                        hasInflightItems = true;
                        break;
                    }
                }
                if (hasInflightItems) {
                    itemsChanged.await(1, TimeUnit.SECONDS);
                } else {
                    for (BulkItem<?> bulkItem : bulkItems) {
                        inflightItems.add(getInflightKey(bulkItem));
                    }
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GeException("Failed to wait for items to not be inflight");
        } finally {
            lock.unlock();
        }
    }

    public List<Item> getCopyOfItems() {
        lock.lock();
        try {
            return new ArrayList<>(outstandingItems.values());
        } finally {
            lock.unlock();
        }
    }
}
