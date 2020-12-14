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
package com.mware.core.status;

import com.codahale.metrics.*;
import com.codahale.metrics.jmx.JmxReporter;

import java.util.concurrent.atomic.AtomicInteger;

public class JmxMetricsManager implements MetricsManager {
    private static final MetricRegistry REGISTRY;
    private static final JmxReporter JMX_REPORTER;
    private static final AtomicInteger ID = new AtomicInteger(0);

    static {
        REGISTRY = new MetricRegistry();
        JMX_REPORTER = JmxReporter.forRegistry(REGISTRY).build();
        JMX_REPORTER.start();
    }

    private static int nextId() {
        return ID.getAndIncrement();
    }

    @Override
    public String getNamePrefix(Object obj) {
        return String.format("%s.%d.", obj.getClass().getName(), JmxMetricsManager.nextId());
    }

    @Override
    public Counter counter(String name) {
        return REGISTRY.counter(name);
    }

    @Override
    public Timer timer(String name) {
        return REGISTRY.timer(name);
    }

    @Override
    public Meter meter(String name) {
        return REGISTRY.meter(name);
    }

    @Override
    public void removeMetric(String metricName) {
        REGISTRY.remove(metricName);
    }
}
