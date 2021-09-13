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

import com.google.common.primitives.Longs;
import com.mware.ge.GeException;
import com.mware.ge.Graph;
import com.mware.ge.GraphConfiguration;
import com.mware.ge.Property;
import com.mware.ge.accumulo.AccumuloGraphConfiguration;
import com.mware.ge.accumulo.StreamingPropertyValueTableDataRef;
import com.mware.ge.accumulo.keys.DataTableRowKey;
import com.mware.ge.store.mutations.ElementMutationBuilder;
import com.mware.ge.store.mutations.StoreMutation;
import com.mware.ge.store.util.StreamingPropertyValueStorageStrategy;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValueRef;
import org.apache.accumulo.core.data.Value;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import static com.mware.ge.accumulo.StreamingPropertyValueTableData.*;

public class DataInDataTableStreamingPropertyValueStorageStrategy implements StreamingPropertyValueStorageStrategy {
    public static final int DEFAULT_PART_SIZE = 10 * 1024;
    private final int dataInDataTablePartSize;

    public DataInDataTableStreamingPropertyValueStorageStrategy(Graph graph, GraphConfiguration configuration) {
        if (!(configuration instanceof AccumuloGraphConfiguration)) {
            throw new GeException("Expected " + AccumuloGraphConfiguration.class.getName() + " found " + configuration.getClass().getName());
        }
        AccumuloGraphConfiguration config = (AccumuloGraphConfiguration) configuration;
        this.dataInDataTablePartSize = DEFAULT_PART_SIZE;
    }

    @Override
    public StreamingPropertyValueRef saveStreamingPropertyValue(
            ElementMutationBuilder elementMutationBuilder,
            String rowKey,
            Property property,
            StreamingPropertyValue streamingPropertyValue
    ) {
        try {
            String dataTableRowKey = new DataTableRowKey(rowKey, property).getRowKey();
            InputStream in = streamingPropertyValue.getInputStream();
            if (in.markSupported()) {
                in.reset();
            }
            byte[] buffer = new byte[dataInDataTablePartSize];
            long offset = 0;
            while (true) {
                int read = in.read(buffer);
                if (read <= 0) {
                    break;
                }
                StoreMutation dataMutation = new StoreMutation(dataTableRowKey);
                String columnQualifier = String.format("%08x", offset);
                if (property.getTimestamp() != null) {
                    byte[] buf = new byte[read];
                    System.arraycopy(buffer, 0, buf, 0, read);
                    dataMutation.put(DATA_COLUMN_FAMILY, columnQualifier, property.getTimestamp(), buf);
                } else {
                    dataMutation.put(DATA_COLUMN_FAMILY, columnQualifier, new Value(buffer, 0, read).get());
                }
                elementMutationBuilder.saveDataMutation(dataMutation);
                offset += read;
            }

            StoreMutation dataMutation = new StoreMutation(dataTableRowKey);
            if (property.getTimestamp() != null)
                dataMutation.put(METADATA_COLUMN_FAMILY, METADATA_LENGTH_COLUMN_QUALIFIER, property.getTimestamp(), Longs.toByteArray(offset));
            else
                dataMutation.put(METADATA_COLUMN_FAMILY, METADATA_LENGTH_COLUMN_QUALIFIER, Longs.toByteArray(offset));
            elementMutationBuilder.saveDataMutation(dataMutation);

            return new StreamingPropertyValueTableDataRef(dataTableRowKey, streamingPropertyValue, offset);
        } catch (Exception ex) {
            throw new GeException("Could not store streaming property value", ex);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public List<InputStream> getInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        // TODO optimize performance similar to OverflowIntoHdfsStreamingPropertyValueStorageStrategy.getInputStreams()
        return streamingPropertyValues.stream()
                .map(StreamingPropertyValue::getInputStream)
                .collect(Collectors.toList());
    }
}
