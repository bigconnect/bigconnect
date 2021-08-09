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
import com.mware.ge.query.aggregations.*;
import com.mware.ge.query.builder.GeQueryBuilder;
import com.mware.ge.search.SearchIndex;
import com.mware.ge.values.storable.DateTimeValue;
import com.mware.ge.values.storable.Value;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class DefaultGraphQueryIterableWithAggregations<T extends GeObject> extends DefaultGraphQueryIterable<T> {
    private final Collection<Aggregation> aggregations;

    public DefaultGraphQueryIterableWithAggregations(
            GeQueryBuilder query,
            Iterable<T> iterable,
            boolean evaluateQueryString,
            boolean evaluateHasContainers,
            boolean evaluateSortContainers,
            Collection<Aggregation> aggregations,
            Authorizations authorizations
    ) {
        super(query, iterable, evaluateQueryString, evaluateHasContainers, evaluateSortContainers, authorizations);
        this.aggregations = aggregations;
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        for (Aggregation agg : this.aggregations) {
            if (agg.getAggregationName().equals(name)) {
                return getAggregationResult(agg, this.iterator(true));
            }
        }
        return super.getAggregationResult(name, resultType);
    }

    public static boolean isAggregationSupported(Aggregation agg) {
        if (agg instanceof TermsAggregation) {
            return true;
        }
        if (agg instanceof ChronoFieldAggregation) {
            return true;
        }
        if (agg instanceof CardinalityAggregation) {
            return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public <TResult extends AggregationResult> TResult getAggregationResult(Aggregation agg, Iterator<T> it) {
        if (agg instanceof TermsAggregation) {
            return (TResult) getTermsAggregationResult((TermsAggregation) agg, it);
        }
        if (agg instanceof ChronoFieldAggregation) {
            return (TResult) getCalendarFieldHistogramResult((ChronoFieldAggregation) agg, it);
        }
        if (agg instanceof CardinalityAggregation) {
            return (TResult) getCardinalityAggregationResult((CardinalityAggregation) agg, it);
        }
        throw new GeException("Unhandled aggregation: " + agg.getClass().getName());
    }

    private CardinalityResult getCardinalityAggregationResult(CardinalityAggregation agg, Iterator<T> it) {
        String fieldName = agg.getPropertyName();

        Set<Value> values = new HashSet<>();
        while (it.hasNext()) {
            T geObject = it.next();
            Iterable<Value> propertyValues = geObject.getPropertyValues(fieldName);
            for (Value propertyValue : propertyValues) {
                values.add(propertyValue);
            }
        }
        return new CardinalityResult(values.stream().distinct().count());
    }

    private TermsResult getTermsAggregationResult(TermsAggregation agg, Iterator<T> it) {
        String propertyName = agg.getPropertyName();
        Map<Object, List<T>> elementsByProperty = getElementsByProperty(it, propertyName, o -> {
            if (o instanceof Value)
                return ((Value) o).asObjectCopy();
            else
                return o;
        });
        elementsByProperty = collapseBucketsByCase(elementsByProperty);

        long other = 0;
        List<TermsBucket> buckets = new ArrayList<>();
        for (Map.Entry<Object, List<T>> entry : elementsByProperty.entrySet()) {
            Object key = entry.getKey();
            int count = entry.getValue().size();
            if (agg.getSize() == null || buckets.size() < agg.getSize()) {
                Map<String, AggregationResult> nestedResults = getNestedResults(agg.getNestedAggregations(), entry.getValue());
                buckets.add(new TermsBucket(key, count, nestedResults));
            } else {
                other += count;
            }
        }
        return new TermsResult(buckets, other, 0);
    }

    private Map<Object, List<T>> collapseBucketsByCase(Map<Object, List<T>> elementsByProperty) {
        Map<String, List<Map.Entry<Object, List<T>>>> stringEntries = new HashMap<>();
        Map<Object, List<T>> results = new HashMap<>();
        // for strings first group them by there lowercase version
        for (Map.Entry<Object, List<T>> entry : elementsByProperty.entrySet()) {
            if (entry.getKey() instanceof String) {
                String lowerCaseKey = ((String) entry.getKey()).toLowerCase();
                List<Map.Entry<Object, List<T>>> l = stringEntries.computeIfAbsent(lowerCaseKey, s -> new ArrayList<>());
                l.add(entry);
            } else {
                results.put(entry.getKey(), entry.getValue());
            }
        }
        // for strings find the best key (the one with the most entries) and use that as the bucket name
        for (Map.Entry<String, List<Map.Entry<Object, List<T>>>> entry : stringEntries.entrySet()) {
            results.put(
                    findBestKey(entry.getValue()),
                    entry.getValue().stream()
                            .flatMap(l -> l.getValue().stream())
                            .collect(Collectors.toList())
            );
        }
        return results;
    }

    private Object findBestKey(List<Map.Entry<Object, List<T>>> value) {
        int longestListLength = 0;
        String longestString = null;
        for (Map.Entry<Object, List<T>> entry : value) {
            if (entry.getValue().size() >= longestListLength) {
                longestListLength = entry.getValue().size();
                longestString = (String) entry.getKey();
            }
        }
        return longestString;
    }

    private HistogramResult getCalendarFieldHistogramResult(final ChronoFieldAggregation agg, Iterator<T> it) {
        String propertyName = agg.getPropertyName();
        Map<Integer, List<T>> elementsByProperty = getElementsByProperty(it, propertyName, o -> {
            DateTimeValue d = (DateTimeValue) o;
            ZonedDateTime zdt = d.asObjectCopy().withZoneSameLocal(agg.getTimeZone().toZoneId());
            return zdt.get(agg.getChronoField());
        });

        Map<Integer, HistogramBucket> buckets = new HashMap<>(24);
        for (Map.Entry<Integer, List<T>> entry : elementsByProperty.entrySet()) {
            int key = entry.getKey();
            int count = entry.getValue().size();
            Map<String, AggregationResult> nestedResults = getNestedResults(agg.getNestedAggregations(), entry.getValue());
            buckets.put(key, new HistogramBucket(key, count, nestedResults));
        }
        return new HistogramResult(buckets.values());
    }

    private Map<String, AggregationResult> getNestedResults(Iterable<Aggregation> nestedAggregations, List<T> elements) {
        Map<String, AggregationResult> results = new HashMap<>();
        for (Aggregation nestedAggregation : nestedAggregations) {
            AggregationResult nestedResult = getAggregationResult(nestedAggregation, elements.iterator());
            results.put(nestedAggregation.getAggregationName(), nestedResult);
        }
        return results;
    }

    private <TKey> Map<TKey, List<T>> getElementsByProperty(Iterator<T> it, String propertyName, ValueConverter<TKey> valueConverter) {
        Map<TKey, List<T>> elementsByProperty = new HashMap<>();
        while (it.hasNext()) {
            T geObject = it.next();

            if (SearchIndex.CONCEPT_TYPE_FIELD_NAME.equals(propertyName) && geObject instanceof Vertex) {
                Vertex v = (Vertex) geObject;
                TKey convertedValue = valueConverter.convert(v.getConceptType());
                elementsByProperty.computeIfAbsent(convertedValue, k -> new ArrayList<>())
                        .add(geObject);
            } else {
                Iterable<Value> values = geObject.getPropertyValues(propertyName);
                for (Value value : values) {
                    TKey convertedValue = valueConverter.convert(value);
                    elementsByProperty.computeIfAbsent(convertedValue, k -> new ArrayList<>())
                            .add(geObject);
                }
            }
        }
        return elementsByProperty;
    }

    private interface ValueConverter<T> {
        T convert(Object o);
    }
}
