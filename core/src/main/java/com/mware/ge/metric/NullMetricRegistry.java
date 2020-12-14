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
package com.mware.ge.metric;

import com.google.common.base.Joiner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class NullMetricRegistry implements GeMetricRegistry {
    private final Map<String, Counter> countersByName = new HashMap<>();
    private final Map<String, Timer> timersByName = new HashMap<>();
    private final Map<String, Histogram> histogramsByName = new HashMap<>();
    private final Map<String, Gauge> gaugesByName = new HashMap<>();
    private final Map<String, StackTraceTracker> stackTraceTrackersByName = new HashMap<>();

    @Override
    public String createName(Class clazz, String... nameParts) {
        return clazz.getName() + "." + Joiner.on(".").join(nameParts);
    }

    @Override
    public Counter getCounter(String name) {
        return countersByName.computeIfAbsent(name, s -> new Counter() {
            long count;

            @Override
            public void increment(long amount) {
                count += amount;
            }

            @Override
            public void decrement(long amount) {
                count += amount;
            }

            @Override
            public long getCount() {
                return count;
            }
        });
    }

    @Override
    public Iterable<Counter> getCounters() {
        return countersByName.values();
    }

    @Override
    public Timer getTimer(String name) {
        return timersByName.computeIfAbsent(name, s -> new Timer() {
            @Override
            public <T> T time(Supplier<T> supplier) {
                return supplier.get();
            }

            @Override
            public void update(long duration, TimeUnit unit) {
            }
        });
    }

    @Override
    public Iterable<Timer> getTimers() {
        return timersByName.values();
    }

    @Override
    public Histogram getHistogram(String name) {
        return histogramsByName.computeIfAbsent(name, s -> new Histogram() {
            @Override
            public void update(int value) {
            }

            @Override
            public void update(long value) {
            }
        });
    }

    @Override
    public Iterable<Histogram> getHistograms() {
        return histogramsByName.values();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Gauge<T> getGauge(String name, Supplier<T> supplier) {
        return gaugesByName.computeIfAbsent(name, s -> new Gauge<T>() {
        });
    }

    @Override
    public Iterable<Gauge> getGauges() {
        return gaugesByName.values();
    }

    @Override
    public StackTraceTracker getStackTraceTracker(String name) {
        return stackTraceTrackersByName.computeIfAbsent(name, s -> new StackTraceTracker());
    }

    @Override
    public Iterable<? extends StackTraceTracker> getStackTraceTrackers() {
        return stackTraceTrackersByName.values();
    }

    @Override
    public void shutdown() {
        countersByName.clear();
        timersByName.clear();
        histogramsByName.clear();
        gaugesByName.clear();
        stackTraceTrackersByName.clear();
    }
}
