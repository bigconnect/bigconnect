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

import com.mware.ge.ElementType;
import com.mware.ge.ExtendedDataRow;
import com.mware.ge.query.aggregations.*;
import com.mware.ge.query.aggregations.Percentile;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.*;
import com.mware.ge.GeException;
import com.mware.ge.query.*;
import com.mware.ge.type.GeoPoint;
import com.mware.ge.type.GeoRect;
import com.mware.ge.util.StreamUtils;
import java.util.*;

public class ElasticsearchGraphQueryIterable<T> extends DefaultGraphQueryIterable<T> implements
        IterableWithTotalHits<T>,
        IterableWithSearchTime<T>,
        IterableWithScores<T> {
    private final long totalHits;
    private final long searchTimeInNanoSeconds;
    private final Map<Object, Double> scores = new HashMap<>();
    private final Map<String, AggregationResult> aggregationResults;

    public ElasticsearchGraphQueryIterable(
            ElasticsearchSearchQueryBase query,
            SearchResponse searchResponse,
            QueryParameters parameters,
            Iterable<T> iterable,
            long totalHits,
            long searchTimeInNanoSeconds,
            SearchHits hits
    ) {
        super(parameters, iterable, false, false, false);
        this.totalHits = totalHits;
        this.searchTimeInNanoSeconds = searchTimeInNanoSeconds;
        if (hits != null) {
            for (SearchHit hit : hits.getHits()) {
                scores.put(query.getIdStrategy().fromSearchHit(hit), (double) hit.getScore());
            }
        }
        this.aggregationResults = getAggregationResults(query, searchResponse);
    }

    @Override
    protected Iterator<T> iterator(boolean iterateAll) {
        return super.iterator(true); // Override to always pass true since Elasticsearch has done the skip for us
    }

    @Override
    public long getTotalHits() {
        return this.totalHits;
    }

    @Override
    public Double getScore(Object id) {
        return this.scores.get(id);
    }

    @Override
    public long getSearchTimeNanoSeconds() {
        return this.searchTimeInNanoSeconds;
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        AggregationResult result = this.aggregationResults.get(name);
        if (result == null) {
            return AggregationResult.createEmptyResult(resultType);
        }
        if (!resultType.isInstance(result)) {
            throw new GeException("Could not cast aggregation result of type " + result.getClass().getName() + " to type " + resultType.getName());
        }
        return resultType.cast(result);
    }

    private static Map<String, AggregationResult> getAggregationResults(ElasticsearchSearchQueryBase query, SearchResponse searchResponse) {
        if (searchResponse == null) {
            return new HashMap<>();
        }
        Map<String, List<Aggregation>> aggsByName = getAggregationResultsByName(query, searchResponse.getAggregations());
        return reduceAggregationResults(query, aggsByName);
    }

    private static Map<String, List<Aggregation>> getAggregationResultsByName(ElasticsearchSearchQueryBase query, Iterable<Aggregation> aggs) {
        Map<String, List<Aggregation>> aggsByName = new HashMap<>();
        if (aggs == null) {
            return aggsByName;
        }
        for (Aggregation agg : aggs) {
            if (agg.getName().equals(ElasticsearchSearchQueryBase.TOP_HITS_AGGREGATION_NAME)) {
                continue;
            }
            String aggName = query.getAggregationName(agg.getName());
            List<Aggregation> l = aggsByName.computeIfAbsent(aggName, k -> new ArrayList<>());
            l.add(agg);
        }
        return aggsByName;
    }

    private static Map<String, AggregationResult> reduceAggregationResults(ElasticsearchSearchQueryBase query, Map<String, List<Aggregation>> aggsByName) {
        Map<String, AggregationResult> results = new HashMap<>();
        for (Map.Entry<String, List<Aggregation>> entry : aggsByName.entrySet()) {
            com.mware.ge.query.aggregations.Aggregation requestedAgg = query.getAggregationByName(entry.getKey());
            AggregationResult result = reduceAggregationResults(query, entry.getValue());

            if (requestedAgg instanceof SupportOrderByAggregation) {
                SupportOrderByAggregation.AggregationSortContainer sortContainer = ((SupportOrderByAggregation)requestedAgg).getOrderBy();
                if (sortContainer != null) {
                    result.setOrderedByNestedAgg(true);
                }
            }
            results.put(entry.getKey(), result);
        }
        return results;
    }

    private static AggregationResult reduceAggregationResults(ElasticsearchSearchQueryBase query, List<Aggregation> aggs) {
        if (aggs.size() == 0) {
            throw new GeException("Cannot reduce zero sized aggregation list");
        }
        Aggregation first = aggs.get(0);
        if (first instanceof HistogramAggregation || first instanceof ChronoFieldAggregation ||
                first instanceof InternalHistogram || first instanceof InternalDateHistogram) {
            return reduceHistogramResults(query, aggs);
        }
        if (first instanceof RangeAggregation || first instanceof InternalRange) {
            return reduceRangeResults(query, aggs);
        }
        if (first instanceof PercentilesAggregation || first instanceof Percentiles) {
            return reducePercentilesResults(query, aggs);
        }
        if (first instanceof TermsAggregation || first instanceof InternalTerms) {
            return reduceTermsResults(query, aggs);
        }
        if (first instanceof GeohashAggregation || first instanceof InternalGeoHashGrid) {
            return reduceGeohashResults(query, aggs);
        }
        if (first instanceof StatisticsAggregation || first instanceof InternalExtendedStats) {
            return reduceStatisticsResults(aggs);
        }
        if (first instanceof CardinalityAggregation || first instanceof InternalCardinality) {
            return reduceCardinalityResults(query, aggs);
        }
        if (first instanceof Sum) {
            return reduceSumResults(query, aggs);
        }
        if (first instanceof Min) {
            return reduceMinResults(query, aggs);
        }
        if (first instanceof Max) {
            return reduceMaxResults(query, aggs);
        }
        if (first instanceof Avg) {
            return reduceAvgResults(query, aggs);
        }
        throw new GeException("Unhandled aggregation type: " + first.getClass().getName());
    }

    private static HistogramResult reduceHistogramResults(ElasticsearchSearchQueryBase query, List<Aggregation> aggs) {
        Map<Object, List<MultiBucketsAggregation.Bucket>> bucketsByKey = new HashMap<>();
        for (Aggregation agg : aggs) {
            if (agg instanceof Histogram) {
                Histogram h = (Histogram) agg;
                com.mware.ge.query.aggregations.Aggregation queryAgg = query.getAggregationByName(query.getAggregationName(h.getName()));
                boolean isCalendarFieldQuery = queryAgg != null && queryAgg instanceof ChronoFieldAggregation;
                for (Histogram.Bucket b : h.getBuckets()) {
                    if (isCalendarFieldQuery && b.getKey().toString().equals("-1.0")) {
                        continue;
                    }
                    List<MultiBucketsAggregation.Bucket> l = bucketsByKey.computeIfAbsent(b.getKey(), k -> new ArrayList<>());
                    l.add(b);
                }
            } else {
                throw new GeException("Aggregation is not a histogram: " + agg.getClass().getName());
            }
        }
        return new MultiBucketsAggregationReducer<HistogramResult, HistogramBucket>() {
            @Override
            protected HistogramBucket createBucket(Object key, long count, Map<String, AggregationResult> nestedResults, List<MultiBucketsAggregation.Bucket> buckets) {
                return new HistogramBucket(key, count, nestedResults);
            }

            @Override
            protected HistogramResult bucketsToResults(List<HistogramBucket> buckets) {
                return new HistogramResult(buckets);
            }
        }.reduce(query, bucketsByKey);
    }

    private static RangeResult reduceRangeResults(ElasticsearchSearchQueryBase query, List<Aggregation> aggs) {
        Map<Object, List<MultiBucketsAggregation.Bucket>> bucketsByKey = new HashMap<>();
        for (Aggregation agg : aggs) {
            if (agg instanceof Range) {
                Range r = (Range) agg;
                for (Range.Bucket b : r.getBuckets()) {
                    List<MultiBucketsAggregation.Bucket> l = bucketsByKey.computeIfAbsent(b.getKey(), k -> new ArrayList<>());
                    l.add(b);
                }
            } else {
                throw new GeException("Aggregation is not a range: " + agg.getClass().getName());
            }
        }
        return new MultiBucketsAggregationReducer<RangeResult, RangeBucket>() {
            @Override
            protected RangeBucket createBucket(Object key, long count, Map<String, AggregationResult> nestedResults, List<MultiBucketsAggregation.Bucket> buckets) {
                return new RangeBucket(key, count, nestedResults);
            }

            @Override
            protected RangeResult bucketsToResults(List<RangeBucket> buckets) {
                return new RangeResult(buckets);
            }
        }.reduce(query, bucketsByKey);
    }

    private static PercentilesResult reducePercentilesResults(ElasticsearchSearchQueryBase query, List<Aggregation> aggs) {
        List<Percentile> results = new ArrayList<>();
        if (aggs.size() != 1) {
            throw new GeException("Unexpected number of aggregations. Expected 1 but found: " + aggs.size());
        }
        Aggregation agg = aggs.get(0);
        if (agg instanceof Percentiles) {
            Percentiles percentiles = (Percentiles) agg;
            StreamUtils.stream(percentiles)
                    .filter(percentile -> !Double.isNaN(percentile.getValue()))
                    .forEach(percentile -> results.add(new Percentile(percentile.getPercent(), percentile.getValue())));
        } else {
            throw new GeException("Aggregation is not a percentile: " + agg.getClass().getName());
        }
        return new PercentilesResult(results);
    }

    private static CardinalityResult reduceCardinalityResults(ElasticsearchSearchQueryBase query, List<Aggregation> aggs) {
        if (aggs.size() == 0) {
            return new CardinalityResult(0);
        }

        if (aggs.size() == 1) {
            Aggregation agg = aggs.get(0);
            if (agg instanceof InternalCardinality) {
                return new CardinalityResult(((InternalCardinality) agg).getValue());
            } else {
                throw new GeException("Unhandled aggregation result type: " + agg.getClass().getName());
            }
        }
        throw new GeException("Cannot reduce multiple " + CardinalityAggregation.class + "(count: " + aggs.size() + ")");
    }

    private static SumResult reduceSumResults(ElasticsearchSearchQueryBase query, List<Aggregation> aggs) {
        double value = 0;

        for (Aggregation agg : aggs) {
            if (agg instanceof Sum) {
                value += ((Sum)agg).getValue();
            } else {
                throw new GeException("Unhandled aggregation result type: " + agg.getClass().getName());
            }
        }
        return new SumResult(value);
    }

    private static MinResult reduceMinResults(ElasticsearchSearchQueryBase query, List<Aggregation> aggs) {
        double value = Double.MAX_VALUE;

        for (Aggregation agg : aggs) {
            if (agg instanceof Min) {
                if ( ((Min)agg).getValue() < value ) {
                    value = ((Min)agg).getValue();
                }
            } else {
                throw new GeException("Unhandled aggregation result type: " + agg.getClass().getName());
            }
        }

        if (value == Double.MAX_VALUE) {
            // no min value available
            value = 0;
        }

        return new MinResult(value);
    }

    private static MaxResult reduceMaxResults(ElasticsearchSearchQueryBase query, List<Aggregation> aggs) {
        double value = Double.MIN_VALUE;

        for (Aggregation agg : aggs) {
            if (agg instanceof Max) {
                if ( ((Max)agg).getValue() > value ) {
                    value = ((Max)agg).getValue();
                }
            } else {
                throw new GeException("Unhandled aggregation result type: " + agg.getClass().getName());
            }
        }

        if (value == Double.MIN_VALUE) {
            // no max value available
            value = 0;
        }

        return new MaxResult(value);
    }

    private static AvgResult reduceAvgResults(ElasticsearchSearchQueryBase query, List<Aggregation> aggs) {
        double value = 0;
        int count = 0;

        for (Aggregation agg : aggs) {
            if (agg instanceof Avg) {
                Avg avg = (Avg) agg;
                if (!Double.isNaN(avg.getValue())) {
                    value = value + avg.getValue();
                    count++;
                }
            } else {
                throw new GeException("Unhandled aggregation result type: " + agg.getClass().getName());
            }
        }
        return new AvgResult(value / count);
    }

    private static TermsResult reduceTermsResults(ElasticsearchSearchQueryBase query, List<Aggregation> aggs) {
        Map<Object, List<MultiBucketsAggregation.Bucket>> bucketsByKey = new LinkedHashMap<>();
        long sumOther = 0;
        long error = 0;
        for (Aggregation agg : aggs) {
            if (agg instanceof Terms) {
                Terms h = (Terms) agg;
                sumOther += h.getSumOfOtherDocCounts();
                error += h.getDocCountError();
                for (Terms.Bucket b : h.getBuckets()) {
                    TopHits exactMatchTopHits = b.getAggregations().get(ElasticsearchSearchQueryBase.TOP_HITS_AGGREGATION_NAME);
                    String mapKey = bucketKeyToString(b.getKey(), exactMatchTopHits);
                    Map<String, Object> metadata = agg.getMetaData();
                    if (metadata != null) {
                        Object fieldName = metadata.get(ElasticsearchSearchQueryBase.AGGREGATION_METADATA_FIELD_NAME_KEY);
                        if (ExtendedDataRow.ELEMENT_TYPE.equals(fieldName)) {
                            if (ElasticsearchDocumentType.VERTEX_EXTENDED_DATA.getKey().equals(mapKey)) {
                                mapKey = ElementType.VERTEX.name();
                            } else if (ElasticsearchDocumentType.EDGE_EXTENDED_DATA.getKey().equals(mapKey)) {
                                mapKey = ElementType.EDGE.name();
                            }
                        }
                    }
                    List<MultiBucketsAggregation.Bucket> existingBucketByName = bucketsByKey.computeIfAbsent(mapKey, k -> new ArrayList<>());
                    existingBucketByName.add(b);
                }
            } else {
                throw new GeException("Aggregation is not a terms: " + agg.getClass().getName());
            }
        }

        final long sumOfOtherDocCounts = sumOther;
        final long docCountErrorUpperBound = error;

        return new MultiBucketsAggregationReducer<TermsResult, TermsBucket>() {
            @Override
            protected TermsBucket createBucket(Object key, long count, Map<String, AggregationResult> nestedResults, List<MultiBucketsAggregation.Bucket> buckets) {
                return new TermsBucket(key, count, nestedResults);
            }

            @Override
            protected TermsResult bucketsToResults(List<TermsBucket> buckets) {
                return new TermsResult(buckets, sumOfOtherDocCounts, docCountErrorUpperBound);
            }
        }.reduce(query, bucketsByKey);
    }

    private abstract static class MultiBucketsAggregationReducer<TResult, TBucket> {
        public TResult reduce(ElasticsearchSearchQueryBase query, Map<Object, List<MultiBucketsAggregation.Bucket>> bucketsByKey) {
            List<TBucket> buckets = new ArrayList<>();
            for (Map.Entry<Object, List<MultiBucketsAggregation.Bucket>> bucketsByKeyEntry : bucketsByKey.entrySet()) {
                String key = bucketKeyToString(bucketsByKeyEntry.getKey(), null);
                long count = 0;
                List<Aggregation> subAggs = new ArrayList<>();
                for (MultiBucketsAggregation.Bucket b : bucketsByKeyEntry.getValue()) {
                    count += b.getDocCount();
                    for (Aggregation subAgg : b.getAggregations()) {
                        subAggs.add(subAgg);
                    }
                }
                Map<String, AggregationResult> nestedResults = reduceAggregationResults(query, getAggregationResultsByName(query, subAggs));
                buckets.add(createBucket(key, count, nestedResults, bucketsByKeyEntry.getValue()));
            }
            return bucketsToResults(buckets);
        }

        protected abstract TBucket createBucket(Object key, long count, Map<String, AggregationResult> nestedResults, List<MultiBucketsAggregation.Bucket> buckets);

        protected abstract TResult bucketsToResults(List<TBucket> buckets);
    }

    private static String bucketKeyToString(Object bucketKey, TopHits exactMatchTopHits) {
        // to maintain backwards compatibility decimals ending in ".0" should not contain the decimal place
        if (bucketKey instanceof Number) {
            String strBucketKey = bucketKey.toString();
            if (strBucketKey.endsWith(".0")) {
                strBucketKey = strBucketKey.substring(0, strBucketKey.length() - 2);
            }
            return strBucketKey;
        }

        if (exactMatchTopHits != null) {
            if (exactMatchTopHits.getHits().getTotalHits().value > 0) {
                SearchHit hit = exactMatchTopHits.getHits().getAt(0);
                for (Object o : hit.getSourceAsMap().values()) {
                    // for multi-value properties find the item that matches regardless of case
                    if (o instanceof Iterable) {
                        for (Object oItem : ((Iterable) o)) {
                            String oItemString = oItem.toString();
                            if (bucketKey.equals(oItemString.toLowerCase())) {
                                return oItemString;
                            }
                        }
                    }

                    return o.toString();
                }
            }
        }

        if (bucketKey instanceof org.elasticsearch.common.geo.GeoPoint) {
            String geohash = ((org.elasticsearch.common.geo.GeoPoint) bucketKey).getGeohash();
            return geohash.replaceAll("0+$", "");
        }
        return bucketKey.toString();
    }

    private static GeohashResult reduceGeohashResults(ElasticsearchSearchQueryBase query, List<Aggregation> aggs) {
        Map<Object, List<MultiBucketsAggregation.Bucket>> bucketsByKey = new HashMap<>();
        for (Aggregation agg : aggs) {
            if (agg instanceof GeoGrid) {
                GeoGrid h = (GeoGrid) agg;
                for (GeoGrid.Bucket b : h.getBuckets()) {
                    List<MultiBucketsAggregation.Bucket> existingBucket = bucketsByKey.computeIfAbsent(b.getKey(), k -> new ArrayList<>());
                    existingBucket.add(b);
                }
            } else {
                throw new GeException("Aggregation is not a geohash: " + agg.getClass().getName());
            }
        }
        return new MultiBucketsAggregationReducer<GeohashResult, GeohashBucket>() {
            @Override
            protected GeohashBucket createBucket(final Object key, long count, Map<String, AggregationResult> nestedResults, List<MultiBucketsAggregation.Bucket> buckets) {
                GeoPoint geoPoint = getAverageGeoPointFromBuckets(buckets);
                return new GeohashBucket(key.toString(), count, geoPoint, nestedResults) {
                    @Override
                    public GeoRect getGeoCell() {
                        org.elasticsearch.common.geo.GeoPoint northWest = new org.elasticsearch.common.geo.GeoPoint();
                        org.elasticsearch.common.geo.GeoPoint southEast = new org.elasticsearch.common.geo.GeoPoint();
                        GeohashUtils.decodeCell(key.toString(), northWest, southEast);
                        return new GeoRect(new GeoPoint(northWest.getLat(), northWest.getLon()), new GeoPoint(southEast.getLat(), southEast.getLon()));
                    }
                };
            }

            @Override
            protected GeohashResult bucketsToResults(List<GeohashBucket> buckets) {
                return new GeohashResult(buckets);
            }
        }.reduce(query, bucketsByKey);
    }

    private static GeoPoint getAverageGeoPointFromBuckets(List<MultiBucketsAggregation.Bucket> buckets) {
        List<GeoPoint> geoPoints = new ArrayList<>();
        for (MultiBucketsAggregation.Bucket b : buckets) {
            GeoGrid.Bucket gb = (GeoGrid.Bucket) b;
            org.elasticsearch.common.geo.GeoPoint gp = (org.elasticsearch.common.geo.GeoPoint) gb.getKey();
            geoPoints.add(new GeoPoint(gp.getLat(), gp.getLon()));
        }
        return GeoPoint.calculateCenter(geoPoints);
    }

    private static StatisticsResult reduceStatisticsResults(List<Aggregation> aggs) {
        List<StatisticsResult> results = new ArrayList<>();
        for (Aggregation agg : aggs) {
            if (agg instanceof ExtendedStats) {
                ExtendedStats extendedStats = (ExtendedStats) agg;
                long count = extendedStats.getCount();
                double sum = extendedStats.getSum();
                double min = extendedStats.getMin();
                double max = extendedStats.getMax();
                double standardDeviation = extendedStats.getStdDeviation();
                results.add(new StatisticsResult(count, sum, min, max, standardDeviation));
            } else {
                throw new GeException("Aggregation is not a statistics: " + agg.getClass().getName());
            }
        }
        return StatisticsResult.combine(results);
    }
}
