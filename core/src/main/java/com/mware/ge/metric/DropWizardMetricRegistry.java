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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.mware.ge.GeException;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class DropWizardMetricRegistry implements GeMetricRegistry {
    private static final String START_CONSOLE_REPORTER_PROPERTY_NAME = "metricRegistryStartConsoleReporter";
    private final MetricRegistry metricRegistry;
    private final Map<String, Counter> countersByName = new ConcurrentHashMap<>();
    private final Map<String, Timer> timersByName = new ConcurrentHashMap<>();
    private final Map<String, Histogram> histogramsByName = new ConcurrentHashMap<>();
    private final Map<String, Gauge> gaugesByName = new ConcurrentHashMap<>();
    private final Map<String, StackTraceTracker> stackTraceTrackersByName = new ConcurrentHashMap<>();
    private boolean consoleReporterStarted;
    private ConsoleReporter consoleReporter;

    public DropWizardMetricRegistry() {
        this(new MetricRegistry());
    }

    public DropWizardMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        startConsoleReporterUsingSystemProperty();
    }

    private void startConsoleReporterUsingSystemProperty() {
        String startConsoleReporter = System.getProperty(START_CONSOLE_REPORTER_PROPERTY_NAME, "false").trim();
        if (startConsoleReporter.equalsIgnoreCase("false")) {
            return;
        }

        if (startConsoleReporter.equalsIgnoreCase("true") || startConsoleReporter.length() == 0) {
            startConsoleReporter(10, TimeUnit.SECONDS);
            return;
        }

        startConsoleReporter(startConsoleReporter);
    }

    private void startConsoleReporter(String period) {
        try {
            Duration duration = parseDuration(period);
            startConsoleReporter(duration.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new GeException("Could not parse period: " + period, ex);
        }
    }

    static Duration parseDuration(String period) {
        period = period.trim();
        try {
            return Duration.parse(period.toUpperCase());
        } catch (Exception ex) {
            return Duration.parse("PT" + period.toUpperCase());
        }
    }

    public void startConsoleReporter(long periodMillis) {
        startConsoleReporter(periodMillis, TimeUnit.MILLISECONDS);
    }

    public void startConsoleReporter(long period, TimeUnit timeUnit) {
        if (consoleReporterStarted) {
            throw new GeException("console reporter already started");
        }
        getConsoleReporter().start(period, timeUnit);
        consoleReporterStarted = true;
    }

    public void stopConsoleReporter() {
        if (!consoleReporterStarted) {
            throw new GeException("console reporter not started");
        }
        getConsoleReporter().stop();
        consoleReporterStarted = false;
    }

    private ConsoleReporter getConsoleReporter() {
        if (consoleReporter == null) {
            consoleReporter = ConsoleReporter.forRegistry(getMetricRegistry())
                    .build();
        }
        return consoleReporter;
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public boolean isConsoleReporterStarted() {
        return consoleReporterStarted;
    }

    @Override
    public String createName(Class clazz, String... nameParts) {
        return MetricRegistry.name(clazz, nameParts);
    }

    @Override
    public Counter getCounter(String name) {
        return countersByName.computeIfAbsent(name, n -> new Counter(metricRegistry.counter(n)));
    }

    @Override
    public Iterable<Counter> getCounters() {
        return countersByName.values();
    }

    @Override
    public Timer getTimer(String name) {
        return timersByName.computeIfAbsent(name, n -> new Timer(metricRegistry.timer(n)));
    }

    @Override
    public Iterable<Timer> getTimers() {
        return timersByName.values();
    }

    @Override
    public Histogram getHistogram(String name) {
        return histogramsByName.computeIfAbsent(name, n -> new Histogram(metricRegistry.histogram(n)));
    }

    @Override
    public Iterable<Histogram> getHistograms() {
        return histogramsByName.values();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Gauge<T> getGauge(String name, Supplier<T> supplier) {
        return gaugesByName.computeIfAbsent(name, n -> {
            com.codahale.metrics.Gauge<T> g = metricRegistry.register(name, (com.codahale.metrics.Gauge<T>) supplier::get);
            return new Gauge<T>(g);
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
        countersByName.keySet().forEach(metricRegistry::remove);
        timersByName.keySet().forEach(metricRegistry::remove);
        histogramsByName.keySet().forEach(metricRegistry::remove);
        gaugesByName.keySet().forEach(metricRegistry::remove);
        stackTraceTrackersByName.keySet().forEach(metricRegistry::remove);

        if (consoleReporter != null && consoleReporterStarted) {
            consoleReporter.report();
        }
    }

    public static class Gauge<T> implements com.mware.ge.metric.Gauge<T> {
        private final com.codahale.metrics.Gauge gauge;

        public Gauge(com.codahale.metrics.Gauge gauge) {
            this.gauge = gauge;
        }

        public com.codahale.metrics.Gauge getGauge() {
            return gauge;
        }
    }

    public static class Counter implements com.mware.ge.metric.Counter {
        private final com.codahale.metrics.Counter counter;

        public Counter(com.codahale.metrics.Counter counter) {
            this.counter = counter;
        }

        public com.codahale.metrics.Counter getCounter() {
            return counter;
        }

        @Override
        public void increment(long amount) {
            counter.inc(amount);
        }

        @Override
        public void decrement(long amount) {
            counter.dec(amount);
        }

        @Override
        public long getCount() {
            return counter.getCount();
        }
    }

    public static class Timer implements com.mware.ge.metric.Timer {
        private final com.codahale.metrics.Timer timer;

        public Timer(com.codahale.metrics.Timer timer) {
            this.timer = timer;
        }

        public com.codahale.metrics.Timer getTimer() {
            return timer;
        }

        @Override
        public <T> T time(Supplier<T> supplier) {
            com.codahale.metrics.Timer.Context ctx = timer.time();
            try {
                return supplier.get();
            } finally {
                ctx.stop();
            }
        }

        @Override
        public void update(long duration, TimeUnit unit) {
            timer.update(duration, unit);
        }
    }

    public static class Histogram implements com.mware.ge.metric.Histogram {
        private final com.codahale.metrics.Histogram histogram;

        public Histogram(com.codahale.metrics.Histogram histogram) {
            this.histogram = histogram;
        }

        public com.codahale.metrics.Histogram getHistogram() {
            return histogram;
        }

        @Override
        public void update(int value) {
            histogram.update(value);
        }

        @Override
        public void update(long value) {
            histogram.update(value);
        }
    }
}
