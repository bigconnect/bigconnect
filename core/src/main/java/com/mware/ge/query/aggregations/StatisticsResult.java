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

import java.util.Collection;

public class StatisticsResult extends AggregationResult {
    private final long count;
    private final double sum;
    private final double min;
    private final double max;
    private final double standardDeviation;

    public StatisticsResult(long count, double sum, double min, double max, double standardDeviation) {
        this.count = count;
        this.sum = sum;
        this.min = min;
        this.max = max;
        this.standardDeviation = standardDeviation;
    }

    public long getCount() {
        return count;
    }

    public double getSum() {
        return sum;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getAverage() {
        if (getCount() == 0) {
            return 0.0;
        }
        return getSum() / (double) getCount();
    }

    public double getStandardDeviation() {
        return standardDeviation;
    }

    public static StatisticsResult combine(Collection<StatisticsResult> statisticsResults) {
        long count = 0;
        double sum = 0.0;
        double min = 0.0;
        double max = 0.0;
        boolean first = true;
        for (StatisticsResult statisticsResult : statisticsResults) {
            count += statisticsResult.getCount();
            sum += statisticsResult.getSum();
            if (first) {
                min = statisticsResult.getMin();
                max = statisticsResult.getMax();
            } else {
                min = Math.min(min, statisticsResult.getMin());
                max = Math.max(max, statisticsResult.getMax());
            }
            first = false;
        }

        double average = count == 0 ? 0.0 : sum / (double) count;

        double standardDeviationS1 = 0.0;
        double standardDeviationS2 = 0.0;
        for (StatisticsResult statisticsResult : statisticsResults) {
            standardDeviationS1 += statisticsResult.getCount() * Math.pow(statisticsResult.getStandardDeviation(), 2.0);
            standardDeviationS2 += statisticsResult.getCount() * Math.pow(statisticsResult.getAverage() - average, 2.0);
        }
        double variance = (standardDeviationS1 + standardDeviationS2) / (double) count;
        double standardDeviation = Math.sqrt(variance);

        return new StatisticsResult(count, sum, min, max, standardDeviation);
    }
}
