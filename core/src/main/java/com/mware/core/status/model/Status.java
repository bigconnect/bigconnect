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
package com.mware.core.status.model;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.ClientApiObject;

import java.util.Date;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
public abstract class Status implements ClientApiObject {
    private String className;
    private String name;
    private String description;
    private String projectVersion;
    private String gitRevision;
    private String builtBy;
    private Date builtOn;

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getProjectVersion() {
        return projectVersion;
    }

    public void setProjectVersion(String projectVersion) {
        this.projectVersion = projectVersion;
    }

    public String getGitRevision() {
        return gitRevision;
    }

    public void setGitRevision(String gitRevision) {
        this.gitRevision = gitRevision;
    }

    public String getBuiltBy() {
        return builtBy;
    }

    public void setBuiltBy(String builtBy) {
        this.builtBy = builtBy;
    }

    public Date getBuiltOn() {
        return builtOn;
    }

    public void setBuiltOn(Date builtOn) {
        this.builtOn = builtOn;
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = CounterMetric.class, name = "counter"),
            @JsonSubTypes.Type(value = TimerMetric.class, name = "timer")
    })
    public static abstract class Metric {
        public static Metric create(com.codahale.metrics.Metric metric) {
            if (metric instanceof Counter) {
                return new CounterMetric((Counter) metric);
            } else if (metric instanceof Timer) {
                return new TimerMetric((Timer) metric);
            }
            throw new BcException("Unhandled metric: " + metric.getClass().getName());
        }
    }

    @JsonTypeName("counter")
    public static class CounterMetric extends Metric {
        private long count;

        public CounterMetric() {

        }

        public CounterMetric(Counter counter) {
            this.count = counter.getCount();
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }

    @JsonTypeName("timer")
    public static class TimerMetric extends Metric {
        private long count;
        private double meanRate;
        private double oneMinuteRate;
        private double fiveMinuteRate;
        private double fifteenMinuteRate;

        public TimerMetric() {

        }

        public TimerMetric(Timer metric) {
            this.count = metric.getCount();
            this.meanRate = metric.getMeanRate();
            this.oneMinuteRate = metric.getOneMinuteRate();
            this.fiveMinuteRate = metric.getFiveMinuteRate();
            this.fifteenMinuteRate = metric.getFifteenMinuteRate();
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public double getMeanRate() {
            return meanRate;
        }

        public void setMeanRate(double meanRate) {
            this.meanRate = meanRate;
        }

        public double getOneMinuteRate() {
            return oneMinuteRate;
        }

        public void setOneMinuteRate(double oneMinuteRate) {
            this.oneMinuteRate = oneMinuteRate;
        }

        public double getFiveMinuteRate() {
            return fiveMinuteRate;
        }

        public void setFiveMinuteRate(double fiveMinuteRate) {
            this.fiveMinuteRate = fiveMinuteRate;
        }

        public double getFifteenMinuteRate() {
            return fifteenMinuteRate;
        }

        public void setFifteenMinuteRate(double fifteenMinuteRate) {
            this.fifteenMinuteRate = fifteenMinuteRate;
        }
    }
}
