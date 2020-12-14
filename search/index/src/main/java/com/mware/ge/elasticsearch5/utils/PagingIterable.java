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
package com.mware.ge.elasticsearch5.utils;

import com.mware.ge.GeException;
import com.mware.ge.elasticsearch5.ElasticsearchGraphQueryIterable;
import com.mware.ge.query.*;
import com.mware.ge.query.aggregations.AggregationResult;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class PagingIterable<T> implements
        Iterable<T>,
        IterableWithTotalHits<T>,
        IterableWithScores<T>,
        QueryResultsIterable<T> {
    private final long skip;
    private final long limit;
    private boolean isFirstCallToIterator;
    private final ElasticsearchGraphQueryIterable<T> firstIterable;
    private final int pageSize;

    public PagingIterable(long skip, Long limit, int pageSize) {
        this.skip = skip;
        this.limit = limit == null ? Long.MAX_VALUE : limit;
        this.pageSize = pageSize;

        // This is a bit of a hack. Because the underlying iterable is the iterable with geohash results, histogram results, etc.
        //   we need to grab the first iterable to get the results out.
        long firstIterableLimit = Math.min(pageSize, this.limit);
        this.firstIterable = getPageIterable((int) this.skip, (int)firstIterableLimit, true);
        this.isFirstCallToIterator = true;
    }

    @Override
    public Double getScore(Object id) {
        return this.firstIterable.getScore(id);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        return this.firstIterable.getAggregationResult(name, resultType);
    }

    @Override
    public long getTotalHits() {
        return this.firstIterable.getTotalHits();
    }

    protected abstract ElasticsearchGraphQueryIterable<T> getPageIterable(int skip, int limit, boolean includeAggregations);

    @Override
    public Iterator<T> iterator() {
        MyIterator it = new MyIterator(isFirstCallToIterator ? firstIterable : null);
        isFirstCallToIterator = false;
        return it;
    }

    private class MyIterator implements Iterator<T> {
        private ElasticsearchGraphQueryIterable<T> firstIterable;
        private long currentResultNumber = 0;
        private long lastIterableResultNumber = 0;
        private long lastPageSize = 0;
        private Iterator<T> currentIterator;

        public MyIterator(ElasticsearchGraphQueryIterable<T> firstIterable) {
            this.firstIterable = firstIterable;
            this.currentResultNumber = skip;
            this.currentIterator = getNextIterator();
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (currentIterator == null) {
                    currentIterator = getNextIterator();
                    if (currentIterator == null) {
                        return false;
                    }
                }
                if (currentIterator.hasNext()) {
                    return true;
                }
                currentIterator = null;
            }
        }

        @Override
        public T next() {
            if (hasNext()) {
                currentResultNumber++;
                return currentIterator.next();
            }
            throw new NoSuchElementException();
        }

        private Iterator<T> getNextIterator() {
            long totalReturned = currentResultNumber - skip;
            long lastIterableCount = currentResultNumber - lastIterableResultNumber;
            if (totalReturned >= limit || currentResultNumber >= getTotalHits() || lastIterableCount < lastPageSize) {
                return null;
            }
            long nextPageSize = lastPageSize = Math.min(pageSize, limit - currentResultNumber);
            if (firstIterable == null) {
                if (nextPageSize <= 0) {
                    return null;
                }
                firstIterable = getPageIterable((int)currentResultNumber, (int)nextPageSize, false);
            }
            Iterator<T> it = firstIterable.iterator();
            firstIterable = null;
            lastIterableResultNumber = currentResultNumber;
            return it;
        }

        @Override
        public void remove() {
            throw new GeException("remove not implemented");
        }
    }
}
