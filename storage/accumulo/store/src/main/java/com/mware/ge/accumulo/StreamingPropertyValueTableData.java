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
import com.google.common.primitives.Longs;
import com.mware.ge.GeException;
import com.mware.ge.accumulo.util.RangeUtils;
import com.mware.ge.util.ByteRingBuffer;
import com.mware.ge.values.storable.StreamingPropertyValue;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class StreamingPropertyValueTableData extends StreamingPropertyValue {
    private static final long serialVersionUID = 1897402273830254711L;
    public static final String METADATA_COLUMN_FAMILY = "a"; // this should sort before the data
    public static final String DATA_COLUMN_FAMILY = "d";
    public static final String METADATA_LENGTH_COLUMN_QUALIFIER = "length";
    private final AccumuloGraph graph;
    private final String dataRowKey;
    private Long length;
    private final Long timestamp;

    public StreamingPropertyValueTableData(
            AccumuloGraph graph,
            String dataRowKey,
            Class valueType,
            Long length,
            Long timestamp
    ) {
        super(valueType);
        this.graph = graph;
        this.dataRowKey = dataRowKey;
        this.length = length;
        this.timestamp = timestamp;
    }

    @Override
    public Long getLength() {
        return length;
    }

    @Override
    public InputStream getInputStream() {
        return new DataTableInputStream();
    }

    private class DataTableInputStream extends InputStream {
        private final ByteRingBuffer buffer = new ByteRingBuffer(1024 * 1024);
        private long timerStartTime;
        private Span trace;
        private ScannerBase scanner;
        private Iterator<Map.Entry<Key, Value>> scannerIterator;
        private long previousLoadedDataLength;
        private long loadedDataLength;
        private boolean closed;

        private long markRowIndex = 0;
        private long markByteOffsetInRow = 0;
        private long markLoadedDataLength = 0;
        private long currentDataRowIndex = -1;
        private long currentByteOffsetInRow;

        @Override
        public int read(byte[] dest, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            len = Math.min(len, buffer.getSize());
            while (buffer.getUsed() == 0 && loadMoreData()) {

            }
            if (buffer.getUsed() == 0) {
                return -1;
            }

            int bytesRead = buffer.read(dest, off, len);
            currentByteOffsetInRow += bytesRead;
            return bytesRead;
        }

        @Override
        public int read() throws IOException {
            if (buffer.getUsed() < 1) {
                loadMoreData();
                if (buffer.getUsed() == 0) {
                    return -1;
                }
            }
            currentByteOffsetInRow++;
            return buffer.read();
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            scannerIterator = null;
            if (scanner != null) {
                scanner.close();
                scanner = null;
            }
            if (trace != null) {
                trace.stop();
                trace = null;
            }

            graph.getGraphLogger().logEndIterator(System.currentTimeMillis() - timerStartTime);
            super.close();
            closed = true;
        }

        private boolean loadMoreData() throws IOException {
            if (closed) {
                return false;
            }

            Iterator<Map.Entry<Key, Value>> it = getScannerIterator();
            while (true) {
                if (!it.hasNext()) {
                    close();
                    return false;
                }
                Map.Entry<Key, Value> column = it.next();
                if (column.getKey().getColumnFamily().equals(new Text(METADATA_COLUMN_FAMILY))) {
                    if (column.getKey().getColumnQualifier().equals(new Text(METADATA_LENGTH_COLUMN_QUALIFIER))) {
                        length = Longs.fromByteArray(column.getValue().get());
                        continue;
                    }

                    throw new GeException("unexpected metadata column qualifier: " + column.getKey().getColumnQualifier() + " (row: " + column.getKey().getRow() + ")");
                }

                if (column.getKey().getColumnFamily().equals(new Text(DATA_COLUMN_FAMILY))) {
                    currentDataRowIndex++;
                    currentByteOffsetInRow = 0;

                    byte[] data = column.getValue().get();
                    if (length == null) {
                        throw new GeException("unexpected missing length (row: " + column.getKey().getRow() + ")");
                    }
                    long len = Math.min(data.length, length - loadedDataLength);
                    buffer.write(data, 0, (int) len);
                    previousLoadedDataLength = loadedDataLength;
                    loadedDataLength += len;
                    return true;
                }

                throw new GeException("unexpected column family: " + column.getKey().getColumnFamily() + " (row: " + column.getKey().getRow() + ")");
            }
        }

        private Iterator<Map.Entry<Key, Value>> getScannerIterator() throws IOException {
            if (closed) {
                throw new IOException("stream already closed");
            }

            if (scannerIterator != null) {
                return scannerIterator;
            }
            scannerIterator = getScanner().iterator();
            return scannerIterator;
        }

        private ScannerBase getScanner() throws IOException {
            if (closed) {
                throw new IOException("stream already closed");
            }

            if (scanner != null) {
                return scanner;
            }
            ArrayList<Range> ranges = Lists.newArrayList(RangeUtils.createRangeFromString(dataRowKey));

            timerStartTime = System.currentTimeMillis();
            try {
                scanner = graph.createBatchScanner(graph.getDataTableName(), ranges, new org.apache.accumulo.core.security.Authorizations());
            } catch (TableNotFoundException ex) {
                throw new GeException("Could not create scanner", ex);
            }

            if (timestamp != null && graph.getConfiguration().getMaxVersions() > 1) {
                IteratorSetting iteratorSetting = new IteratorSetting(
                        80,
                        TimestampFilter.class.getSimpleName(),
                        TimestampFilter.class
                );
                TimestampFilter.setStart(iteratorSetting, timestamp, true);
                TimestampFilter.setEnd(iteratorSetting, timestamp, true);
                scanner.addScanIterator(iteratorSetting);
            }

            graph.getGraphLogger().logStartIterator(graph.getDataTableName(), scanner);
            trace = Trace.start("streamingPropertyValueTableData");
            trace.data("dataRowKeyCount", Integer.toString(1));
            return scanner;
        }

        @Override
        public synchronized void mark(int readlimit) {
            markRowIndex = Math.max(0, currentDataRowIndex);
            markByteOffsetInRow = currentByteOffsetInRow;
            markLoadedDataLength = previousLoadedDataLength;
        }

        @Override
        public synchronized void reset() throws IOException {
            buffer.clear();
            if (scannerIterator != null) {
                scannerIterator = null;
            }

            closed = false;

            currentDataRowIndex = -1;
            currentByteOffsetInRow = 0;
            loadedDataLength = markLoadedDataLength;

            Iterator<Map.Entry<Key, Value>> it = getScannerIterator();
            while (true) {
                if (!it.hasNext()) {
                    close();
                    return;
                }
                Map.Entry<Key, Value> column = it.next();
                if (column.getKey().getColumnFamily().equals(DATA_COLUMN_FAMILY)) {
                    currentDataRowIndex++;
                    currentByteOffsetInRow = 0;
                    if (currentDataRowIndex == markRowIndex) {
                        byte[] data = column.getValue().get();
                        long len = Math.min(data.length, length - loadedDataLength);
                        buffer.write(data, 0, (int) len);
                        loadedDataLength += len;
                        while (currentByteOffsetInRow != markByteOffsetInRow) {
                            buffer.read();
                            currentByteOffsetInRow++;
                        }
                        return;
                    }
                }
            }
        }

        @Override
        public boolean markSupported() {
            return true;
        }
    }
}
