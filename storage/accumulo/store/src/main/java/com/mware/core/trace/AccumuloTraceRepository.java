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
package com.mware.core.trace;

import com.google.inject.Inject;
import com.mware.core.exception.BcException;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Graph;
import com.mware.ge.GeException;
import com.mware.ge.accumulo.AccumuloGraph;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;

import java.util.Map;

public class AccumuloTraceRepository extends TraceRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(AccumuloTraceRepository.class);
    private static boolean distributedTraceEnabled;
    private final AccumuloGraph accumuloGraph;

    @Inject
    public AccumuloTraceRepository(Graph graph) {
        if (graph instanceof AccumuloGraph) {
            accumuloGraph = (AccumuloGraph) graph;
        } else {
            throw new BcException("You cannot use the " + AccumuloTraceRepository.class.getName() + " unless you are using " + AccumuloGraph.class.getName());
        }
    }

    @Override
    public void enable() {
        if (!distributedTraceEnabled) {
            try {
                DistributedTrace.enable(null, "bigCONNECT", accumuloGraph.getConfiguration().getClientConfiguration());
                accumuloGraph.traceOn("bigCONNECT");
                distributedTraceEnabled = true;
            } catch (Exception e) {
                throw new GeException("Could not enable DistributedTrace", e);
            }
        }
    }

    @Override
    public TraceSpan on(String description, Map<String, String> data) {
        enable();

        if (Trace.isTracing()) {
            throw new GeException("Trace already running");
        }
        Span span = Trace.on(description);
        for (Map.Entry<String, String> dataEntry : data.entrySet()) {
            span.data(dataEntry.getKey(), dataEntry.getValue());
        }

        LOGGER.info("Started trace '%s'", description);
        return wrapSpan(span);
    }

    @Override
    public void off() {
        Trace.off();
    }

    @Override
    public TraceSpan start(String description) {
        final Span span = Trace.start(description);
        return wrapSpan(span);
    }


    private TraceSpan wrapSpan(final Span span) {
        return new TraceSpan() {

            @Override
            public TraceSpan data(String key, String value) {
                span.data(key, value);
                return this;
            }

            @Override
            public void close() {
                span.stop();
            }
        };
    }

    @Override
    public boolean isEnabled() {
        return Trace.isTracing();
    }
}
