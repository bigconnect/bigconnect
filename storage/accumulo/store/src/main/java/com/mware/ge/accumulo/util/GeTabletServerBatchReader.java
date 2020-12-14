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
package com.mware.ge.accumulo.util;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletServerBatchReaderIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.NamingThreadFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

// Based on org.apache.accumulo.core.client.impl.TabletServerBatchReader
public class GeTabletServerBatchReader extends ScannerOptions implements BatchScanner {
    private final String tableId;
    private final int numThreads;
    private final ClientContext context;
    private final Authorizations authorizations;
    private ArrayList<Range> ranges;

    private static final ExecutorService queryThreadPool = new ThreadPoolExecutor(
            0,
            Integer.MAX_VALUE,
            30L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new NamingThreadFactory("Accumulo batch scanner read ahead thread")
    );

    public GeTabletServerBatchReader(
            Connector connector,
            String tableName,
            Authorizations authorizations,
            int numQueryThreads
    ) throws TableNotFoundException {
        ClientContext context = ConnectorUtils.getContext(connector);
        String tableId = Tables.getTableId(connector.getInstance(), tableName);

        checkArgument(context != null, "context is null");
        checkArgument(tableId != null, "tableId is null");
        checkArgument(authorizations != null, "authorizations is null");
        this.context = context;
        this.authorizations = authorizations;
        this.tableId = tableId;
        this.numThreads = numQueryThreads;
        this.ranges = null;
    }

    @Override
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public void setRanges(Collection<Range> ranges) {
        if (ranges == null || ranges.size() == 0) {
            throw new IllegalArgumentException("ranges must be non null and contain at least 1 range");
        }
        this.ranges = new ArrayList<>(ranges);
    }

    @Override
    public Iterator<Map.Entry<Key, Value>> iterator() {
        if (ranges == null) {
            throw new IllegalStateException("ranges not set");
        }

        return new TabletServerBatchReaderIterator(
                context,
                tableId,
                authorizations,
                ranges,
                numThreads,
                queryThreadPool,
                this,
                timeOut
        );
    }
}
