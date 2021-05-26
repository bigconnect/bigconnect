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
package com.mware.ge.accumulo;

import com.google.common.collect.Lists;
import com.mware.ge.GeException;
import com.mware.ge.accumulo.keys.DataTableRowKey;
import com.mware.ge.accumulo.util.RangeUtils;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.values.storable.StreamingPropertyValue;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static com.mware.ge.accumulo.AccumuloGraph.GRAPH_LOGGER;

public class StreamingPropertyValueTable extends StreamingPropertyValue {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(StreamingPropertyValueTable.class);
    private static final long serialVersionUID = 400244414843534240L;
    private final AccumuloGraph graph;
    private final String dataRowKey;
    private final Long timestamp;
    private transient byte[] data;

    StreamingPropertyValueTable(AccumuloGraph graph, String dataRowKey, StreamingPropertyValueTableRef valueRef, Long timestamp) {
        super(valueRef.getValueType());
        this.timestamp = timestamp;
        this.searchIndex(valueRef.isSearchIndex());
        this.graph = graph;
        this.dataRowKey = dataRowKey;
        this.data = valueRef.getData();
    }

    @Override
    public Long getLength() {
        ensureDataLoaded();
        return (long) this.data.length;
    }

    public String getDataRowKey() {
        return dataRowKey;
    }

    public boolean isDataLoaded() {
        return this.data != null;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public InputStream getInputStream() {
        // we need to store the data here to handle the case that the mutation hasn't been flushed yet but the element is
        // passed to the search indexer to be indexed and we can't get the value yet.
        ensureDataLoaded();
        return new ByteArrayInputStream(this.data);
    }

    private void ensureDataLoaded() {
        if (!isDataLoaded()) {
            this.data = streamingPropertyValueTableData(this.dataRowKey, this.timestamp);
        }
    }

    public byte[] streamingPropertyValueTableData(String dataRowKey, Long timestamp) {
        try {
            List<Range> ranges = Lists.newArrayList(RangeUtils.createRangeFromString(dataRowKey));

            long timerStartTime = System.currentTimeMillis();
            ScannerBase scanner = graph.createBatchScanner(graph.getDataTableName(), ranges, new org.apache.accumulo.core.security.Authorizations());
            if (timestamp != null && !DataTableRowKey.isLegacy(dataRowKey) && graph.getConfiguration().getMaxVersions() > 1) {
                IteratorSetting iteratorSetting = new IteratorSetting(
                        80,
                        TimestampFilter.class.getSimpleName(),
                        TimestampFilter.class
                );
                TimestampFilter.setStart(iteratorSetting, timestamp, true);
                TimestampFilter.setEnd(iteratorSetting, timestamp, true);
                scanner.addScanIterator(iteratorSetting);
            }

            GRAPH_LOGGER.logStartIterator(graph.getDataTableName(), scanner);
            Span trace = Trace.start("streamingPropertyValueTableData");
            trace.data("dataRowKeyCount", Integer.toString(1));
            try {
                byte[] result = null;
                for (Map.Entry<Key, Value> col : scanner) {
                    String foundKey = col.getKey().getRow().toString();
                    byte[] value = col.getValue().get();
                    if (foundKey.equals(dataRowKey)) {
                        result = value;
                    }
                }
                if (result == null) {
                    LOGGER.warn("Could not find data with key: " + dataRowKey);
                    result = new byte[0];
                }
                return result;
            } finally {
                scanner.close();
                trace.stop();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } catch (Exception ex) {
            throw new GeException(ex);
        }
    }
}
