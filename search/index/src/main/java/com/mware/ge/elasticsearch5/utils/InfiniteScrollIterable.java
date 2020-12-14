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

import com.mware.ge.elasticsearch5.ElasticsearchGraphQueryIdIterable;
import com.mware.ge.elasticsearch5.IdStrategy;
import com.mware.ge.query.aggregations.AggregationResult;
import com.mware.ge.query.IterableWithScores;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.util.CloseableIterator;
import com.mware.ge.util.CloseableUtils;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.util.*;
import java.util.stream.Collectors;

public abstract class InfiniteScrollIterable<T> implements QueryResultsIterable<T>, IterableWithScores<T> {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(InfiniteScrollIterable.class);
    private static final String SCROLL_API_STACK_TRACE_LOGGER_NAME = "com.mware.ge.elasticsearch5.SCROLL_API_STACK_TRACE";
    private static final GeLogger SCROLL_API_STACK_TRACE_LOGGER = GeLoggerFactory.getLogger(SCROLL_API_STACK_TRACE_LOGGER_NAME);

    private final Long limit;
    private QueryResultsIterable<T> firstIterable;
    private boolean initCalled;
    private boolean firstCall;
    private SearchResponse response;
    private List<String> scrollIds = new ArrayList<>();
    private Map<String, StackTraceElement[]> stackTraces = new HashMap<>();

    protected InfiniteScrollIterable(Long limit) {
        this.limit = limit;
    }

    protected abstract SearchResponse getInitialSearchResponse();

    protected abstract SearchResponse getNextSearchResponse(String scrollId);

    protected abstract QueryResultsIterable<T> searchResponseToIterable(SearchResponse searchResponse);

    protected abstract void closeScroll(String scrollId);

    protected abstract IdStrategy getIdStrategy();

    @Override
    public void close() {
        scrollIds.forEach(this::closeScroll);
        scrollIds.clear();
        stackTraces.clear();
    }

    private void init() {
        if (initCalled) {
            return;
        }
        response = getInitialSearchResponse();
        if (response == null) {
            firstIterable = null;
        } else {
            firstIterable = searchResponseToIterable(response);
            scrollIds.add(response.getScrollId());
            if (SCROLL_API_STACK_TRACE_LOGGER.isTraceEnabled()) {
                stackTraces.put(response.getScrollId(), Thread.currentThread().getStackTrace());
            }
        }
        firstCall = true;
        initCalled = true;
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        init();
        if (firstIterable == null) {
            return AggregationResult.createEmptyResult(resultType);
        }
        return firstIterable.getAggregationResult(name, resultType);
    }

    @Override
    public long getTotalHits() {
        init();
        if (firstIterable == null) {
            return 0;
        }
        return firstIterable.getTotalHits();
    }

    @Override
    public Double getScore(Object id) {
        if (response == null) {
            return null;
        }
        for (SearchHit hit : response.getHits()) {
            Object hitId = ElasticsearchGraphQueryIdIterable.idFromSearchHit(hit, getIdStrategy());
            if (hitId == null) {
                continue;
            }
            if (id.equals(hitId)) {
                return (double) hit.getScore();
            }
        }
        return null;
    }

    @Override
    public Iterator<T> iterator() {
        init();
        if (response == null) {
            return Collections.emptyIterator();
        }

        Iterator<T> it;
        if (firstCall) {
            it = firstIterable.iterator();
            firstCall = false;
        } else {
            response = getInitialSearchResponse();
            scrollIds.add(response.getScrollId());
            if (SCROLL_API_STACK_TRACE_LOGGER.isTraceEnabled()) {
                stackTraces.put(response.getScrollId(), Thread.currentThread().getStackTrace());
            }
            it = searchResponseToIterable(response).iterator();
        }
        return new InfiniteIterator(response.getScrollId(), it);
    }

    @Override
    protected void finalize() throws Throwable {
        if (!scrollIds.isEmpty()) {
            LOGGER.warn(
                    "Elasticsearch scroll not closed. This can occur if you do not iterate completely or did not call close on the iterable.%s",
                    !stackTraces.isEmpty() && SCROLL_API_STACK_TRACE_LOGGER.isTraceEnabled()
                            ? ""
                            : String.format(" To enable stack traces enable trace logging on \"%s\"", SCROLL_API_STACK_TRACE_LOGGER_NAME)
            );
            if (!stackTraces.isEmpty()) {
                stackTraces.forEach((key, stackTrace) ->
                        SCROLL_API_STACK_TRACE_LOGGER.trace(
                                "Source of unclosed iterable:\n  %s",
                                Arrays.stream(stackTrace)
                                        .map(StackTraceElement::toString)
                                        .collect(Collectors.joining("\n  "))
                        ));
            }
            close();
        }
        super.finalize();
    }

    private class InfiniteIterator implements CloseableIterator<T> {
        private final String scrollId;
        private Iterator<T> it;
        private T next;
        private T current;
        private long currentResultNumber = 0;

        public InfiniteIterator(String scrollId, Iterator<T> it) {
            this.scrollId = scrollId;
            this.it = it;
        }

        @Override
        public boolean hasNext() {
            loadNext();
            if (next == null) {
                close();
            }
            return next != null;
        }

        @Override
        public T next() {
            loadNext();
            if (next == null) {
                throw new NoSuchElementException();
            }
            this.current = this.next;
            this.next = null;
            return this.current;
        }

        private void loadNext() {
            if (this.next != null || it == null) {
                return;
            }

            boolean isUnderLimit = limit == null || currentResultNumber < limit;
            if (isUnderLimit && it.hasNext()) {
                this.next = it.next();
                currentResultNumber++;
            } else {
                CloseableUtils.closeQuietly(it);
                it = null;

                if (isUnderLimit && getTotalHits() > currentResultNumber) {
                    QueryResultsIterable<T> iterable = searchResponseToIterable(getNextSearchResponse(scrollId));
                    it = iterable.iterator();
                    if (!it.hasNext()) {
                        it = null;
                    } else {
                        this.next = it.next();
                        currentResultNumber++;
                    }
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            CloseableUtils.closeQuietly(it);
            closeScroll(this.scrollId);
            scrollIds.remove(this.scrollId);
            stackTraces.remove(this.scrollId);
        }
    }
}
