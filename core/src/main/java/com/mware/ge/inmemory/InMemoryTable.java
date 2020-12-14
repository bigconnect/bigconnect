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
package com.mware.ge.inmemory;

import com.mware.ge.Authorizations;
import com.mware.ge.FetchHints;
import com.mware.ge.inmemory.mutations.Mutation;
import com.mware.ge.util.StreamUtils;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public abstract class InMemoryTable<TElement extends InMemoryElement> {
    private ReadWriteLock rowsLock = new ReentrantReadWriteLock();
    private Map<String, InMemoryTableElement<TElement>> rows;

    protected InMemoryTable(Map<String, InMemoryTableElement<TElement>> rows) {
        this.rows = rows;
    }

    protected InMemoryTable() {
        this(new ConcurrentSkipListMap<>());
    }

    public TElement get(InMemoryGraph graph, String id, FetchHints fetchHints, Authorizations authorizations) {
        InMemoryTableElement<TElement> inMemoryTableElement = getTableElement(id);
        if (inMemoryTableElement == null) {
            return null;
        }
        return inMemoryTableElement.createElement(graph, fetchHints, authorizations);
    }

    public InMemoryTableElement<TElement> getTableElement(String id) {
        if (null == id) {
            return null;
        }
        rowsLock.readLock().lock();
        try {
            return rows.get(id);
        } finally {
            rowsLock.readLock().unlock();
        }
    }

    public void append(String id, Mutation... newMutations) {
        if (null == id) {
            return;
        }
        rowsLock.writeLock().lock();
        try {
            InMemoryTableElement<TElement> inMemoryTableElement = rows.get(id);
            if (inMemoryTableElement == null) {
                inMemoryTableElement = createInMemoryTableElement(id);
                rows.put(id, inMemoryTableElement);
            }
            inMemoryTableElement.addAll(newMutations);
        } finally {
            rowsLock.writeLock().unlock();
        }
    }

    protected abstract InMemoryTableElement<TElement> createInMemoryTableElement(String id);

    public void remove(String id) {
        if (null == id) {
            return;
        }
        rowsLock.writeLock().lock();
        try {
            rows.remove(id);
        } finally {
            rowsLock.writeLock().unlock();
        }
    }

    public void clear() {
        rowsLock.writeLock().lock();
        try {
            rows.clear();
        } finally {
            rowsLock.writeLock().unlock();
        }
    }

    public Iterable<TElement> getAll(
            InMemoryGraph graph,
            FetchHints fetchHints,
            Long endTime,
            Authorizations authorizations
    ) {
        return StreamUtils.stream(getRowValues())
                .filter(element -> graph.isIncludedInTimeSpan(element, fetchHints, endTime, authorizations))
                .map(element -> element.createElement(graph, fetchHints, endTime, authorizations))
                .collect(Collectors.toList());
    }

    public Iterable<InMemoryTableElement<TElement>> getRowValues() {
        rowsLock.readLock().lock();
        try {
            return new ArrayList<>(this.rows.values());
        } finally {
            rowsLock.readLock().unlock();
        }
    }
}
