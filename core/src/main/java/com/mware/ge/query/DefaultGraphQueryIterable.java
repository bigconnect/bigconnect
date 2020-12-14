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
package com.mware.ge.query;

import com.mware.ge.*;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.query.aggregations.AggregationResult;
import com.mware.ge.scoring.ScoringStrategy;
import com.mware.ge.util.CloseableIterator;
import com.mware.ge.util.CloseableUtils;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.util.Preconditions.checkNotNull;

public class DefaultGraphQueryIterable<T> implements
        Iterable<T>,
        QueryResultsIterable<T>,
        IterableWithScores<T> {
    private final QueryParameters parameters;
    private final Iterable<T> iterable;
    private final boolean evaluateQueryString;
    private final boolean evaluateHasContainers;

    public DefaultGraphQueryIterable(
            QueryParameters parameters,
            Iterable<T> iterable,
            boolean evaluateQueryString,
            boolean evaluateHasContainers,
            boolean evaluateSortContainers
    ) {
        checkNotNull(iterable, "iterable cannot be null");
        this.parameters = parameters;
        this.evaluateQueryString = evaluateQueryString;
        this.evaluateHasContainers = evaluateHasContainers;
        if (evaluateSortContainers && this.parameters.getSortContainers().size() > 0) {
            this.iterable = sortUsingSortContainers(iterable, parameters.getSortContainers());
        } else if (evaluateHasContainers && this.parameters.getScoringStrategy() != null) {
            this.iterable = sortUsingScoringStrategy(iterable, parameters.getScoringStrategy());
        } else {
            this.iterable = iterable;
        }
    }

    private Iterable<T> sortUsingScoringStrategy(Iterable<T> iterable, ScoringStrategy scoringStrategy) {
        List<T> list = toList(iterable);
        list.sort(new ScoringStrategyComparator<>(scoringStrategy));
        return list;
    }

    private List<T> sortUsingSortContainers(Iterable<T> iterable, List<QueryBase.SortContainer> sortContainers) {
        List<T> list = toList(iterable);
        list.sort(new SortContainersComparator<>(sortContainers));
        return list;
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(false);
    }

    protected Iterator<T> iterator(final boolean iterateAll) {
        final Iterator<T> it = iterable.iterator();

        return new CloseableIterator<T>() {
            public T next;
            public T current;
            public long count;

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

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                CloseableUtils.closeQuietly(it);
                DefaultGraphQueryIterable.this.close();
            }

            private void loadNext() {
                if (this.next != null) {
                    return;
                }

                if (!iterateAll && parameters.getLimit() != null && (this.count >= parameters.getSkip() + parameters.getLimit())) {
                    return;
                }

                while (it.hasNext()) {
                    T elem = it.next();
                    GeObject geElem = elem instanceof GeObject ? (GeObject) elem : null;

                    boolean match = true;
                    if (evaluateHasContainers && geElem != null) {
                        for (QueryBase.HasContainer has : parameters.getHasContainers()) {
                            if (!has.isMatch(geElem)) {
                                match = false;
                                break;
                            }
                        }
                        if (geElem instanceof Edge && parameters.getEdgeLabels().size() > 0) {
                            Edge edge = (Edge) geElem;
                            if (!parameters.getEdgeLabels().contains(edge.getLabel())) {
                                match = false;
                            }
                        }
                        if (geElem instanceof Vertex && parameters.getConceptTypes().size() > 0) {
                            Vertex vertex = (Vertex) geElem;
                            if (!parameters.getConceptTypes().contains(vertex.getConceptType())) {
                                match = false;
                            }
                        }
                        if (parameters.getIds() != null) {
                            if (geElem instanceof Element) {
                                if (!parameters.getIds().contains(((Element) geElem).getId())) {
                                    match = false;
                                }
                            } else if (geElem instanceof ExtendedDataRow) {
                                if (!parameters.getIds().contains(((ExtendedDataRow) geElem).getId().getElementId())) {
                                    match = false;
                                }
                            } else {
                                throw new GeException("Unhandled element type: " + geElem.getClass().getName());
                            }
                        }

                        if (parameters.getMinScore() != null) {
                            if (parameters.getScoringStrategy() == null) {
                                match = false;
                            } else {
                                Double elementScore = parameters.getScoringStrategy().getScore(geElem);
                                if (elementScore == null) {
                                    match = false;
                                } else {
                                    match = elementScore >= parameters.getMinScore();
                                }
                            }
                        }
                    }
                    if (!match) {
                        continue;
                    }
                    if (evaluateQueryString
                            && geElem != null
                            && parameters instanceof QueryStringQueryParameters
                            && ((QueryStringQueryParameters) parameters).getQueryString() != null
                            && !evaluateQueryString(geElem, ((QueryStringQueryParameters) parameters).getQueryString())
                            ) {
                        continue;
                    }

                    this.count++;
                    if (!iterateAll && (this.count <= parameters.getSkip())) {
                        continue;
                    }

                    this.next = elem;
                    break;
                }
            }
        };
    }

    protected boolean evaluateQueryString(GeObject geObject, String queryString) {
        if (geObject instanceof Element) {
            return evaluateQueryString((Element) geObject, queryString);
        } else if (geObject instanceof ExtendedDataRow) {
            return evaluateQueryString((ExtendedDataRow) geObject, queryString);
        } else {
            throw new GeException("Unhandled GeObject type: " + geObject.getClass().getName());
        }
    }

    private boolean evaluateQueryString(Element element, String queryString) {
        for (Property property : element.getProperties()) {
            if (evaluateQueryStringOnValue(property.getValue(), queryString)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateQueryString(ExtendedDataRow extendedDataRow, String queryString) {
        for (Property property : extendedDataRow.getProperties()) {
            if (evaluateQueryStringOnValue(property.getValue(), queryString)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateQueryStringOnValue(Object value, String queryString) {
        if (value == null) {
            return false;
        }
        if (queryString.equals("*")) {
            return true;
        }
        if (value instanceof StreamingPropertyValue) {
            value = ((StreamingPropertyValue) value).readToString();
        }
        String valueString = value.toString().toLowerCase();
        return valueString.contains(queryString.toLowerCase());
    }

    @Override
    public long getTotalHits() {
        // a limit could be set on a query which could prevent all items being returned
        return count(this.iterator(true));
    }

    @Override
    public void close() {
        CloseableUtils.closeQuietly(iterable);
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        throw new GeException("Could not find aggregation with name: " + name);
    }

    @Override
    public Double getScore(Object id) {
        if (parameters.getScoringStrategy() != null) {
            GeObject GeObject = findGeObjectById(id);
            if (GeObject != null) {
                return parameters.getScoringStrategy().getScore(GeObject);
            }
        }
        return 0.0;
    }
    private GeObject findGeObjectById(Object id) {
        Iterator<T> it = iterator(true);
        while (it.hasNext()) {
            T obj = it.next();
            if (obj instanceof GeObject) {
                GeObject GeObject = (GeObject) obj;
                if (GeObject.getId().equals(id)) {
                    return GeObject;
                }
            }
        }
        return null;
    }
}
