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

import com.mware.ge.GeException;
import com.mware.ge.values.storable.Value;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HistogramAggregation extends Aggregation implements SupportsNestedAggregationsAggregation {
    private final String aggregationName;
    private final String fieldName;
    private final String interval;
    private final Long minDocumentCount;
    private Value missingValue;
    private final List<Aggregation> nestedAggregations = new ArrayList<>();
    private ExtendedBounds<?> extendedBounds;

    public HistogramAggregation(
            String aggregationName,
            String fieldName,
            String interval,
            Long minDocumentCount) {
        this.aggregationName = aggregationName;
        this.fieldName = fieldName;
        this.interval = interval;
        this.minDocumentCount = minDocumentCount;
    }

    public String getAggregationName() {
        return aggregationName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getInterval() {
        return interval;
    }

    public Long getMinDocumentCount() {
        return minDocumentCount;
    }

    public void setMissingValue(Value missingValue) {
        this.missingValue = missingValue;
    }

    public Value getMissingValue() {
        return missingValue;
    }

    @Override
    public void addNestedAggregation(Aggregation nestedAggregation) {
        this.nestedAggregations.add(nestedAggregation);
    }

    @Override
    public Iterable<Aggregation> getNestedAggregations() {
        return nestedAggregations;
    }

    public ExtendedBounds<?> getExtendedBounds() {
        return extendedBounds;
    }

    public void setExtendedBounds(ExtendedBounds<?> extendedBounds) {
        this.extendedBounds = extendedBounds;
    }

    public static class ExtendedBounds<T extends Value> implements Serializable {
        private static final long serialVersionUID = 6441762717687378245L;
        private final T min;
        private final T max;

        public ExtendedBounds(T min, T max) {
            if (min == null && max == null) {
                throw new GeException("Either min or max needs to not be null");
            }
            this.min = min;
            this.max = max;
        }

        public T getMin() {
            return min;
        }

        public T getMax() {
            return max;
        }

        @SuppressWarnings("unchecked")
        public Class<? extends Value> getMinMaxType() {
            if (min != null) {
                return min.getClass();
            }
            if (max != null) {
                return max.getClass();
            }
            throw new GeException("Invalid state. min or max must not be null.");
        }
    }
}
