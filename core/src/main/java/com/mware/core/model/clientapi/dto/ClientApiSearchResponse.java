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
 *
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
package com.mware.core.model.clientapi.dto;

import com.mware.core.model.clientapi.util.ClientApiConverter;
import com.mware.core.model.search.SearchResults;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@Getter
@Setter
public abstract class ClientApiSearchResponse extends SearchResults implements ClientApiObject {
    private Integer nextOffset = null;
    private Long retrievalTime = null;
    private Long totalTime = null;
    private Long totalHits = null;
    private Long searchTime = null;
    private Map<String, AggregateResult> aggregates = new HashMap<String, AggregateResult>();

    @Override
    public void close() throws Exception {

    }

    @Override
    public String toString() {
        return ClientApiConverter.clientApiToString(this);
    }

    public abstract int getItemCount();

    @Getter
    @Setter
    public abstract static class AggregateResult {
        private String type;
        private String field;
        private boolean orderedByNestedAgg = false;

        public AggregateResult(String type) {
            setType(type);
        }

        @Getter
        @Setter
        public abstract static class BucketBase {
            private final Map<String, AggregateResult> nestedResults;
            private String label;

            protected BucketBase(Map<String, AggregateResult> nestedResults) {
                this.nestedResults = nestedResults;
            }
        }
    }

    public static class TermsAggregateResult extends AggregateResult {
        private Map<String, Bucket> buckets = new LinkedHashMap<>();

        public TermsAggregateResult() {
            super("term");
        }

        public Map<String, Bucket> getBuckets() {
            return buckets;
        }

        public static class Bucket extends BucketBase {
            private final long count;
            private String label;

            public Bucket(long count, Map<String, AggregateResult> nestedResults) {
                super(nestedResults);
                this.count = count;
            }

            public long getCount() {
                return count;
            }
        }
    }

    @Getter
    @Setter
    public static class GeohashAggregateResult extends AggregateResult {
        private long maxCount;
        private Map<String, Bucket> buckets = new HashMap<String, Bucket>();

        public GeohashAggregateResult() {
            super("geohash");
        }

        @Getter
        public static class Bucket extends BucketBase {
            private final ClientApiGeoRect cell;
            private final ClientApiGeoPoint point;
            private final long count;

            public Bucket(ClientApiGeoRect cell, ClientApiGeoPoint point, long count, Map<String, AggregateResult> nestedResults) {
                super(nestedResults);
                this.cell = cell;
                this.point = point;
                this.count = count;
            }
        }
    }

    @Getter
    @Setter
    public static class HistogramAggregateResult extends AggregateResult {
        private Map<String, Bucket> buckets = new HashMap<String, Bucket>();
        private String fieldType;
        private String min;
        private String max;

        public HistogramAggregateResult() {
            super("histogram");
        }

        @Getter
        @Setter
        public static class Bucket extends BucketBase {
            private final long count;
            private String fromValue;
            private String toValue;

            public Bucket(long count, Map<String, AggregateResult> nestedResults) {
                super(nestedResults);
                this.count = count;
            }
        }
    }

    @Getter
    @Setter
    public static class StatisticsAggregateResult extends AggregateResult {
        private long count;
        private double average;
        private double min;
        private double max;
        private double standardDeviation;
        private double sum;

        public StatisticsAggregateResult() {
            super("statistics");
        }
    }

    @Getter
    @Setter
    public static class SumAggregateResult extends AggregateResult {
        private double value;

        public SumAggregateResult() {
            super("sum");
        }
    }

    @Getter
    @Setter
    public static class AvgAggregateResult extends AggregateResult {
        private double value;

        public AvgAggregateResult() {
            super("avg");
        }
    }

    @Getter
    @Setter
    public static class MinAggregateResult extends AggregateResult {
        private double value;

        public MinAggregateResult() {
            super("min");
        }
    }

    @Getter
    @Setter
    public static class MaxAggregateResult extends AggregateResult {
        private double value;

        public MaxAggregateResult() {
            super("max");
        }
    }

    public static ClientApiSearchResponse listToClientApiSearchResponse(List<List<Object>> rows) {
        ClientApiSearchResponse results;
        if (rows == null || rows.size() == 0) {
            results = new ClientApiElementSearchResponse();
        } else if (rows.get(0).size() == 1 && rows.get(0).get(0) instanceof ClientApiVertex) {
            results = new ClientApiElementSearchResponse();
            ((ClientApiElementSearchResponse) results).getElements().addAll(toClientApiVertex(rows));
        } else if (rows.get(0).size() == 1 && rows.get(0).get(0) instanceof ClientApiEdge) {
            results = new ClientApiEdgeSearchResponse();
            ((ClientApiEdgeSearchResponse) results).getResults().addAll(toClientApiEdge(rows));
        } else {
            results = new ClientApiScalarSearchResponse();
            ((ClientApiScalarSearchResponse) results).getResults().addAll(rows);
        }
        return results;
    }

    private static Collection<ClientApiVertex> toClientApiVertex(List<List<Object>> rows) {
        List<ClientApiVertex> results = new ArrayList<ClientApiVertex>();
        for (List<Object> row : rows) {
            results.add((ClientApiVertex) row.get(0));
        }
        return results;
    }

    private static Collection<ClientApiEdge> toClientApiEdge(List<List<Object>> rows) {
        List<ClientApiEdge> results = new ArrayList<ClientApiEdge>();
        for (List<Object> row : rows) {
            results.add((ClientApiEdge) row.get(0));
        }
        return results;
    }
}
