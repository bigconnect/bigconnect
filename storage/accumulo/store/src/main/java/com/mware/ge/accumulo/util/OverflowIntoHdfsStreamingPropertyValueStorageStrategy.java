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

import com.mware.ge.GeException;
import com.mware.ge.Graph;
import com.mware.ge.GraphConfiguration;
import com.mware.ge.Property;
import com.mware.ge.accumulo.*;
import com.mware.ge.accumulo.keys.DataTableRowKey;
import com.mware.ge.accumulo.mapreduce.ElementMapperGraph;
import com.mware.ge.store.mutations.ElementMutationBuilder;
import com.mware.ge.store.mutations.StoreMutation;
import com.mware.ge.store.util.StreamingPropertyValueStorageStrategy;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.IOUtils;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValueRef;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class OverflowIntoHdfsStreamingPropertyValueStorageStrategy implements StreamingPropertyValueStorageStrategy {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(OverflowIntoHdfsStreamingPropertyValueStorageStrategy.class);
    private static String EMPTY_TEXT = "";
    private final FileSystem fileSystem;
    private final long maxStreamingPropertyValueTableDataSize;
    private final String dataDir;
    private final Graph graph;
    private final AccumuloGraphConfiguration config;

    public OverflowIntoHdfsStreamingPropertyValueStorageStrategy(Graph graph, GraphConfiguration configuration) throws Exception {
        if (!(configuration instanceof AccumuloGraphConfiguration)) {
            throw new GeException("Expected " + AccumuloGraphConfiguration.class.getName() + " found " + configuration.getClass().getName());
        }
        if (!(graph instanceof AccumuloGraph) && !(graph instanceof ElementMapperGraph)) {
            throw new GeException("Expected " + AccumuloGraph.class.getName() + " or " + ElementMapperGraph.class.getName()+", found " + graph.getClass().getName());
        }
        this.graph = graph;
        this.config = (AccumuloGraphConfiguration) configuration;
        this.fileSystem = config.createFileSystem();
        this.maxStreamingPropertyValueTableDataSize = config.getMaxStreamingPropertyValueTableDataSize();
        this.dataDir = config.getDataDir();
    }

    @Override
    public StreamingPropertyValueRef saveStreamingPropertyValue(
            ElementMutationBuilder elementMutationBuilder,
            String rowKey,
            Property property,
            StreamingPropertyValue streamingPropertyValue
    ) {
        try {
            HdfsLargeDataStore largeDataStore = new HdfsLargeDataStore(this.fileSystem, this.dataDir, rowKey, property);
            LimitOutputStream out = new LimitOutputStream(largeDataStore, maxStreamingPropertyValueTableDataSize);
            try {
                InputStream is = streamingPropertyValue.getInputStream();
                if (is.markSupported()) {
                    is.reset();
                }
                IOUtils.copy(streamingPropertyValue.getInputStream(), out);
            } finally {
                out.close();
            }
            if (out.hasExceededSizeLimit()) {
                LOGGER.debug("saved large file to \"%s\" (length: %d)", largeDataStore.getFullHdfsPath(), out.getLength());
                return new StreamingPropertyValueHdfsRef(largeDataStore.getRelativeFileName(), streamingPropertyValue);
            } else {
                return saveStreamingPropertyValueSmall(elementMutationBuilder, rowKey, property, out.getSmall(), streamingPropertyValue);
            }
        } catch (IOException ex) {
            throw new GeException(ex);
        }
    }

    @Override
    public void close() {
        try {
            this.fileSystem.close();
        } catch (IOException ex) {
            throw new GeException("Could not close filesystem", ex);
        }
    }
    @Override
    public List<InputStream> getInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        List<StreamingPropertyValueTable> notLoadedTableSpvs = streamingPropertyValues.stream()
                .filter((spv) -> spv instanceof StreamingPropertyValueTable)
                .map((spv) -> (StreamingPropertyValueTable) spv)
                .filter((spv) -> !spv.isDataLoaded())
                .collect(Collectors.toList());
        List<String> dataRowKeys = notLoadedTableSpvs.stream()
                .map(StreamingPropertyValueTable::getDataRowKey)
                .collect(Collectors.toList());
        Map<String, byte[]> tableInputStreams = streamingPropertyValueTableDatas(dataRowKeys);
        notLoadedTableSpvs
                .forEach((spv) -> {
                    String dataRowKey = spv.getDataRowKey();
                    byte[] bytes = tableInputStreams.get(dataRowKey);
                    if (bytes == null) {
                        throw new GeException("Could not find StreamingPropertyValue data: " + dataRowKey);
                    }
                    spv.setData(bytes);
                });
        return streamingPropertyValues.stream()
                .map(StreamingPropertyValue::getInputStream)
                .collect(Collectors.toList());
    }

    private Map<String, byte[]> streamingPropertyValueTableDatas(List<String> dataRowKeys) {
        try {
            if (dataRowKeys.size() == 0) {
                return Collections.emptyMap();
            }
            List<org.apache.accumulo.core.data.Range> ranges = dataRowKeys.stream()
                    .map(RangeUtils::createRangeFromString)
                    .collect(Collectors.toList());
            final long timerStartTime = System.currentTimeMillis();
            final String dataTableName = AccumuloGraph.getDataTableName(config.getTableNamePrefix());
            ScannerBase scanner = createBatchScanner(dataTableName, ranges, new org.apache.accumulo.core.security.Authorizations());
            AccumuloGraph.GRAPH_LOGGER.logStartIterator(dataTableName, scanner);
            Span trace = Trace.start("streamingPropertyValueTableData");
            trace.data("dataRowKeyCount", Integer.toString(dataRowKeys.size()));
            try {
                Map<String, byte[]> results = new HashMap<>();
                for (Map.Entry<Key, Value> col : scanner) {
                    results.put(col.getKey().getRow().toString(), col.getValue().get());
                }
                return results;
            } finally {
                scanner.close();
                trace.stop();
                AccumuloGraph.GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } catch (Exception ex) {
            throw new GeException(ex);
        }
    }

    private StreamingPropertyValueRef saveStreamingPropertyValueSmall(
            ElementMutationBuilder elementMutationBuilder,
            String rowKey,
            Property property,
            byte[] data,
            StreamingPropertyValue propertyValue
    ) {
        String dataTableRowKey = new DataTableRowKey(rowKey, property).getRowKey();
        StoreMutation dataMutation = new StoreMutation(dataTableRowKey);
        if (property.getTimestamp() != null) {
            dataMutation.put(EMPTY_TEXT, EMPTY_TEXT, property.getTimestamp(), data);
        } else {
            dataMutation.put(EMPTY_TEXT, EMPTY_TEXT, data);
        }
        elementMutationBuilder.saveDataMutation(dataMutation);
        return new StreamingPropertyValueTableRef(dataTableRowKey, propertyValue, data);
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public String getDataDir() {
        return dataDir;
    }

    public GeTabletServerBatchReader createBatchScanner(String tableName, Collection<Range> ranges, org.apache.accumulo.core.security.Authorizations accumuloAuthorizations) throws TableNotFoundException {
        Connector connector = config.createConnector();
        GeTabletServerBatchReader scanner = new GeTabletServerBatchReader(
                connector,
                tableName,
                accumuloAuthorizations,
                config.getNumberOfQueryThreads()
        );
        scanner.setRanges(ranges);
        return scanner;
    }
}
