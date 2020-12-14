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
package com.mware.ge.query.aggregations;

import com.mware.ge.values.storable.Value;

import java.util.ArrayList;
import java.util.List;

public class RangeAggregation extends Aggregation implements SupportsNestedAggregationsAggregation {
    private final String aggregationName;
    private final String fieldName;
    private String format;

    private List<Range> ranges = new ArrayList<>();

    private final List<Aggregation> nestedAggregations = new ArrayList<>();

    public RangeAggregation(String aggregationName, String fieldName) {
        this.aggregationName = aggregationName;
        this.fieldName = fieldName;
    }

    public RangeAggregation(String aggregationName, String fieldName, String format) {
        this(aggregationName, fieldName);
        this.format = format;
    }

    @Override
    public String getAggregationName() {
        return aggregationName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getFormat() {
        return format;
    }

    @Override
    public void addNestedAggregation(Aggregation nestedAggregation) {
        this.nestedAggregations.add(nestedAggregation);
    }

    @Override
    public Iterable<Aggregation> getNestedAggregations() {
        return nestedAggregations;
    }

    public List<Range> getRanges() {
        return ranges;
    }

    public void addRange(Value from, Value to) {
        addRange(null, from, to);
    }

    public void addRange(String key, Value from, Value to) {
        ranges.add(new Range(key, from, to));
    }

    public void addUnboundedTo(Value to) {
        addRange(null, null, to);
    }

    public void addUnboundedTo(String key, Value to) {
        addRange(key, null, to);
    }

    public void addUnboundedFrom(Value from) {
        addRange(null, from, null);
    }

    public void addUnboundedFrom(String key, Value from) {
        addRange(key, from, null);
    }

    public class Range {
        private String key;
        private Value from;
        private Value to;

        public Range(String key, Value from, Value to) {
            this.key = key;
            this.from = from;
            this.to = to;
        }

        public String getKey() {
            return key;
        }

        public Value getFrom() {
            return from;
        }

        public Value getTo() {
            return to;
        }
    }
}
