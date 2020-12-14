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
package com.mware.core.orm.accumulo;

import com.mware.core.orm.*;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class AccumuloSimpleOrmSession extends SimpleOrmSession {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloSimpleOrmSession.class);
    public static final String ACCUMULO_INSTANCE_NAME = "simpleOrm.accumulo.instanceName";
    public static final String ACCUMULO_USER = "simpleOrm.accumulo.username";
    public static final String ACCUMULO_PASSWORD = "simpleOrm.accumulo.password";
    public static final String ZK_SERVER_NAMES = "simpleOrm.accumulo.zookeeperServerNames";
    private static final String ROW_DELETING_ITERATOR_NAME = RowDeletingIterator.class.getSimpleName();
    private static final int ROW_DELETING_ITERATOR_PRIORITY = 7;
    private Connector connector;
    private BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
    private final Set<String> initializedTables = new HashSet<>();
    private final Map<String, BatchWriter> batchWriters = new HashMap<>();
    private String tablePrefix;

    public AccumuloSimpleOrmSession() {

    }

    public AccumuloSimpleOrmSession(Map<String, Object> properties) {
        setTablePrefix(properties);
    }

    public void init(Map<String, Object> properties) {
        try {
            String zkServerNames;
            if (properties.get(ZK_SERVER_NAMES) instanceof Collection) {
                zkServerNames = StringUtils.join((Collection) properties.get(ZK_SERVER_NAMES), ",");
            } else {
                zkServerNames = (String) properties.get(ZK_SERVER_NAMES);
            }
            checkNotNull(zkServerNames, "Could not find config: " + ZK_SERVER_NAMES);

            String accumuloInstanceName = (String) properties.get(ACCUMULO_INSTANCE_NAME);
            checkNotNull(accumuloInstanceName, "Could not find config: " + ACCUMULO_INSTANCE_NAME);
            org.apache.commons.configuration.Configuration config = new ClientConfiguration(new ArrayList<org.apache.commons.configuration.Configuration>())
                    .withInstance(accumuloInstanceName)
                    .withZkHosts(zkServerNames);
            ZooKeeperInstance zk = new ZooKeeperInstance(config);
            String username = (String) properties.get(ACCUMULO_USER);
            String password = (String) properties.get(ACCUMULO_PASSWORD);
            connector = zk.getConnector(username, new PasswordToken(password));

            setTablePrefix(properties);
        } catch (Exception e) {
            throw new SimpleOrmException("Failed to init", e);
        }
    }

    @Override
    public SimpleOrmContext createContext(String... authorizations) {
        return new AccumuloSimpleOrmContext(authorizations);
    }

    @Override
    public void close() {
        for (BatchWriter writer : this.batchWriters.values()) {
            try {
                writer.close();
            } catch (MutationsRejectedException e) {
                throw new SimpleOrmException("Could not close batch writer", e);
            }
        }
    }

    private void setTablePrefix(Map<String, Object> properties) {
        tablePrefix = (String) properties.get(TABLE_PREFIX);
        if (tablePrefix == null) {
            tablePrefix = "";
        }
    }

    @Override
    public <T> Iterable<T> findAll(final Class<T> rowClass, SimpleOrmContext context) {
        try {
            ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(rowClass);
            final Scanner scanner = createScanner(getTableName(modelMetadata), (AccumuloSimpleOrmContext) context);
            return scannerToRows(scanner, modelMetadata);
        } catch (TableNotFoundException e) {
            throw new SimpleOrmException("Could not find all", e);
        }
    }

    public <T> Iterable<T> findAllInRange(String startKey, String endKey, final Class<T> rowClass, SimpleOrmContext context) {
        try {
            ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(rowClass);
            final Scanner scanner = createScanner(getTableName(modelMetadata), (AccumuloSimpleOrmContext) context);
            scanner.setRange(new Range(startKey,true,endKey,true));
            return scannerToRows(scanner, modelMetadata);
        } catch (TableNotFoundException e) {
            throw new SimpleOrmException("Could not find all in range", e);
        }
    }

    @Override
    public <T> void delete(final Class<T> rowClass, String id, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(rowClass);
        LOGGER.trace("deleteRow called with parameters: tableName=?, id=?", getTableName(modelMetadata), id);
        // In most instances (e.g., when reading is not necessary), the
        // RowDeletingIterator gives better performance than the deleting
        // mutation. This is due to the fact that Deleting mutations marks each
        // entry with a delete marker. Using the iterator marks a whole row with
        // a single mutation.
        try {
            BatchWriter writer = connector.createBatchWriter(getTableName(modelMetadata), batchWriterConfig);
            try {
                Mutation mutation = new Mutation(id);
                mutation.put(new byte[0], new byte[0], RowDeletingIterator.DELETE_ROW_VALUE.get());
                writer.addMutation(mutation);
                writer.flush();
            } catch (AccumuloException ae) {
                throw new SimpleOrmException("Could not delete", ae);
            } finally {
                writer.close();
            }
        } catch (Exception mre) {
            throw new SimpleOrmException("Could not delete: " + rowClass.getName() + " id " + id, mre);
        }
    }

    @Override
    public String getTablePrefix() {
        return tablePrefix;
    }

    @Override
    public Iterable<String> getTableList(SimpleOrmContext simpleOrmContext) {
        return this.connector.tableOperations().list();
    }

    @Override
    public void deleteTable(String table, SimpleOrmContext simpleOrmContext) {
        try {
            this.connector.tableOperations().delete(table);
            this.initializedTables.remove(table);
        } catch (Exception e) {
            throw new SimpleOrmException("Could not delete table: " + table, e);
        }
    }

    @Override
    public void clearTable(String table, SimpleOrmContext simpleOrmContext) {
        try {
            this.connector.tableOperations().deleteRows(table, null, null);
        } catch (Exception e) {
            throw new SimpleOrmException("Could not clear table: " + table, e);
        }
    }

    private <T> OrmCloseableIterable<T> scannerToRows(final Scanner scanner, final ModelMetadata<T> modelMetadata) {
        return new OrmCloseableIterable<T>() {
            @Override
            public void close() throws IOException {
                scanner.close();
            }

            @Override
            public OrmCloseableIterator<T> iterator() {
                final RowIterator rowIterator = new RowIterator(scanner);
                return new OrmCloseableIterator<T>() {

                    @Override
                    public void close() throws IOException {
                        scanner.close();
                    }

                    @Override
                    public boolean hasNext() {
                        return rowIterator.hasNext();
                    }

                    @Override
                    public T next() {
                        Iterator<Map.Entry<Key, Value>> row = rowIterator.next();
                        return createObjectFromRow(modelMetadata, row);
                    }

                    @Override
                    public void remove() {
                        rowIterator.remove();
                    }
                };
            }
        };
    }

    private <T> T findByIdInRange (Class<T> rowClass, String id, SimpleOrmContext context, Range r) {
        try {
            ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(rowClass);
            Scanner scanner = createScanner(getTableName(modelMetadata), (AccumuloSimpleOrmContext) context);
            try {
                scanner.setRange(new Range(id));
                Iterator<T> rows = scannerToRows(scanner, modelMetadata).iterator();
                if (!rows.hasNext()) {
                    return null;
                }
                T result = rows.next();
                if (rows.hasNext()) {
                    throw new SimpleOrmException("Too many rows returned for a single row query (rowKey: " + id + ")");
                }
                return result;
            } finally {
                scanner.close();
            }
        } catch (TableNotFoundException e) {
            throw new SimpleOrmException("Could not find by id", e);
        }
    }

    @Override
    public <T> T findById(Class<T> rowClass, String id, SimpleOrmContext context) {
        return findByIdInRange(rowClass, id, context, new Range(id));
    }

    public <T> T findByExactId(Class<T> rowClass, String id, SimpleOrmContext context) {
        return findByIdInRange(rowClass, id, context, Range.exact(id));
    }

    @Override
    public <T> Iterable<T> findByIdStartsWith(Class<T> rowClass, String idPrefix, SimpleOrmContext context) {
        try {
            ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(rowClass);
            final Scanner scanner = createScanner(getTableName(modelMetadata), (AccumuloSimpleOrmContext) context);
            scanner.setRange(Range.prefix(idPrefix));
            return scannerToRows(scanner, modelMetadata);
        } catch (TableNotFoundException e) {
            throw new SimpleOrmException("Could not find by id starts with", e);
        }
    }

    public <T> void alterVisibility(T obj, String currentVisibility, String newVisibility, SimpleOrmContext context) {
        try {
            ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(obj);
            Scanner scanner = createScanner(getTableName(modelMetadata), (AccumuloSimpleOrmContext) context);
            try {
                scanner.setRange(new Range(modelMetadata.getId(obj)));
                RowIterator rowIterator = new RowIterator(scanner);
                BatchWriter writer = getBatchWriter(getTableName(modelMetadata));
                ColumnVisibility newColumnVisibility = new ColumnVisibility(newVisibility);
                while (rowIterator.hasNext()) {
                    Iterator<Map.Entry<Key, Value>> row = rowIterator.next();
                    alterVisibilityRow(writer, row, currentVisibility, newColumnVisibility);
                }
                writer.flush();
            } finally {
                scanner.close();
            }
        } catch (Throwable ex) {
            throw new SimpleOrmException("Could not alterVisibility", ex);
        }
    }

    private void alterVisibilityRow(BatchWriter writer, Iterator<Map.Entry<Key, Value>> row, String currentVisibility, ColumnVisibility newVisibility) throws MutationsRejectedException {
        Mutation mAdd = null;
        Mutation mDelete = null;
        boolean hasItems = false;
        while (row.hasNext()) {
            Map.Entry<Key, Value> column = row.next();
            if (mAdd == null) {
                mAdd = new Mutation(column.getKey().getRow());
                mDelete = new Mutation(column.getKey().getRow());
            }
            if (column.getKey().getColumnVisibility().toString().equals(currentVisibility)) {
                hasItems = true;
                mAdd.put(
                        column.getKey().getColumnFamily(),
                        column.getKey().getColumnQualifier(),
                        newVisibility,
                        column.getKey().getTimestamp(),
                        column.getValue()
                );
                mDelete.putDelete(
                        column.getKey().getColumnFamily(),
                        column.getKey().getColumnQualifier()
                );
            }
        }
        if (hasItems) {
            writer.addMutation(mAdd);
            writer.addMutation(mDelete);
        }
    }

    @Override
    public <T> void save(T obj, String visibility, SimpleOrmContext context) {
        try {
            ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(obj);
            ModelMetadata.Type modelMetadataType = modelMetadata.getTypeFromObject(obj);
            ensureTableIsInitialized(getTableName(modelMetadata));
            BatchWriter writer = getBatchWriter(getTableName(modelMetadata));
            ColumnVisibility columnVisibility = new ColumnVisibility(visibility);
            writer.addMutation(getMutationForObject(modelMetadata, modelMetadataType, obj, columnVisibility));
            writer.flush();
        } catch (MutationsRejectedException e) {
            throw new SimpleOrmException("Error occurred when writing mutation", e);
        }
    }

    @Override
    public <T> void saveMany(Collection<T> objs, String visibility, SimpleOrmContext context) {
        try {
            if (objs.size() == 0) {
                return;
            }
            BatchWriter writer = null;
            ModelMetadata<T> modelMetadata = null;
            ColumnVisibility columnVisibility = new ColumnVisibility(visibility);
            for (T obj : objs) {
                if (modelMetadata == null) {
                    modelMetadata = ModelMetadata.getModelMetadata(obj);
                    ensureTableIsInitialized(getTableName(modelMetadata));
                }
                if (writer == null) {
                    writer = getBatchWriter(getTableName(modelMetadata));
                }
                writer.addMutation(getMutationForObject(modelMetadata, modelMetadata.getTypeFromObject(obj), obj, columnVisibility));
            }
            if (writer != null) {
                writer.flush();
            }
        } catch (MutationsRejectedException e) {
            throw new SimpleOrmException("Error occurred when writing mutation", e);
        }
    }


    public <T> String getTableName(Class<T> rowClass) {
        ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(rowClass);
        return getTableName(modelMetadata);
    }

    private <T> String getTableName(ModelMetadata<T> modelMetadata) {
        return tablePrefix + modelMetadata.getTableName();
    }

    public <T> Mutation getMutationForObject(T obj, String visibility) {
        ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(obj);
        ModelMetadata.Type type = modelMetadata.getTypeFromObject(obj);
        return getMutationForObject(modelMetadata, type, obj, new ColumnVisibility(visibility));
    }

    private <T> Mutation getMutationForObject(ModelMetadata modelMetadata, ModelMetadata.Type type, T obj, ColumnVisibility columnVisibility) {
        String rowKey = modelMetadata.getId(obj);
        Mutation mutation = new Mutation(rowKey);
        for (Map.Entry<String, Map<String, ModelMetadata.Field>> columnFamilyFields : type.getFields().entrySet()) {
            Text columnFamilyName = new Text(columnFamilyFields.getKey());
            for (Map.Entry<String, ModelMetadata.Field> columnField : columnFamilyFields.getValue().entrySet()) {
                Text columnName = new Text(columnField.getKey());
                byte[] value = columnField.getValue().get(obj);
                if (value == null) {
                    continue;
                }
                mutation.put(columnFamilyName, columnName, columnVisibility, new Value(value));
            }
        }
        return mutation;
    }

    private BatchWriter getBatchWriter(String tableName) {
        try {
            synchronized (batchWriters) {
                BatchWriter writer = batchWriters.get(tableName);
                if (writer == null) {
                    writer = connector.createBatchWriter(tableName, batchWriterConfig);
                    batchWriters.put(tableName, writer);
                }
                return writer;
            }
        } catch (TableNotFoundException e) {
            throw new SimpleOrmException("Could not find table: " + tableName, e);
        }
    }

    private <T> T createObjectFromRow(ModelMetadata<T> modelMetadata, Iterator<Map.Entry<Key, Value>> row) {
        try {
            boolean first = true;
            String discriminatorValue;
            Iterator<ColumnData> columns;
            if (modelMetadata.getDiscriminatorColumnName() == null) {
                discriminatorValue = ModelMetadata.DEFAULT_DISCRIMINATOR;
                columns = ColumnData.createIterator(row);
            } else {
                RowData rowData = new RowData(row);
                byte[] discriminatorValueBytes = rowData.getColumnValue(modelMetadata.getDiscriminatorColumnFamily(), modelMetadata.getDiscriminatorColumnName());
                checkNotNull(discriminatorValueBytes, "Could not find discriminatorValue " + modelMetadata.getDiscriminatorColumnFamily() + "." + modelMetadata.getDiscriminatorColumnName());
                discriminatorValue = new String(discriminatorValueBytes);
                columns = rowData.createIterator();
            }
            ModelMetadata.Type type = modelMetadata.getType(discriminatorValue);
            T result = type.newInstance();
            while (columns.hasNext()) {
                ColumnData column = columns.next();
                if (first) {
                    modelMetadata.setIdField(result, column.getRowKey());
                }

                String columnFamily = column.getColumnFamily();
                String columnName = column.getColumnName();
                ModelMetadata.Field field = type.getFieldForColumn(columnFamily, columnName);
                if (field != null) {
                    field.set(result, column.getValue());
                }
                first = false;
            }
            return result;
        } catch (Exception e) {
            throw new SimpleOrmException("Could not create class: " + modelMetadata.toString(), e);
        }
    }

    private static class RowData {
        private String rowKey;
        private final Map<String, Map<String, byte[]>> data = new HashMap<>();

        public RowData(Iterator<Map.Entry<Key, Value>> row) {
            while (row.hasNext()) {
                Map.Entry<Key, Value> column = row.next();
                if (rowKey == null) {
                    rowKey = column.getKey().getRow().toString();
                }
                addColumn(column);
            }
        }

        private void addColumn(Map.Entry<Key, Value> column) {
            Map<String, byte[]> columnFamilyData = getColumnFamilyData(column.getKey().getColumnFamily().toString());
            columnFamilyData.put(column.getKey().getColumnQualifier().toString(), column.getValue().get());
        }

        private Map<String, byte[]> getColumnFamilyData(String columnFamilyName) {
            Map<String, byte[]> columnFamilyData = data.get(columnFamilyName);
            if (columnFamilyData == null) {
                columnFamilyData = new HashMap<>();
                data.put(columnFamilyName, columnFamilyData);
            }
            return columnFamilyData;
        }

        public byte[] getColumnValue(String columnFamily, String columnName) {
            return getColumnFamilyData(columnFamily).get(columnName);
        }

        public Iterator<ColumnData> createIterator() {
            List<ColumnData> results = new ArrayList<>();
            for (Map.Entry<String, Map<String, byte[]>> columnFamily : data.entrySet()) {
                String columnFamilyName = columnFamily.getKey();
                for (Map.Entry<String, byte[]> column : columnFamily.getValue().entrySet()) {
                    String columnName = column.getKey();
                    byte[] value = column.getValue();
                    results.add(new ColumnData(rowKey, columnFamilyName, columnName, value));
                }
            }
            return results.iterator();
        }
    }

    private static class ColumnData {
        private final String rowKey;
        private final String columnFamily;
        private final String columnName;
        private final byte[] value;

        private ColumnData(String rowKey, String columnFamily, String columnName, byte[] value) {
            this.rowKey = rowKey;
            this.columnFamily = columnFamily;
            this.columnName = columnName;
            this.value = value;
        }

        public String getRowKey() {
            return rowKey;
        }

        public String getColumnFamily() {
            return columnFamily;
        }

        public String getColumnName() {
            return columnName;
        }

        public byte[] getValue() {
            return value;
        }

        public static Iterator<ColumnData> createIterator(final Iterator<Map.Entry<Key, Value>> row) {
            return new Iterator<ColumnData>() {
                @Override
                public boolean hasNext() {
                    return row.hasNext();
                }

                @Override
                public ColumnData next() {
                    Map.Entry<Key, Value> column = row.next();
                    String rowKey = column.getKey().getRow().toString();
                    String columnFamily = column.getKey().getColumnFamily().toString();
                    String columnName = column.getKey().getColumnQualifier().toString();
                    byte[] value = column.getValue().get();
                    return new ColumnData(rowKey, columnFamily, columnName, value);
                }

                @Override
                public void remove() {
                    throw new SimpleOrmException("Not supported");
                }
            };
        }
    }

    private Scanner createScanner(String tableName, AccumuloSimpleOrmContext context) throws TableNotFoundException {
        ensureTableIsInitialized(tableName);

        Scanner scanner = connector.createScanner(tableName, context.getAccumuloAuthorizations());
        IteratorSetting iteratorSetting = new IteratorSetting(
                100,
                RowDeletingIterator.class.getSimpleName(),
                RowDeletingIterator.class
        );
        scanner.addScanIterator(iteratorSetting);
        return scanner;
    }

    private void ensureTableIsInitialized(String tableName) {
        try {
            if (initializedTables.contains(tableName)) {
                return;
            }

            if (!connector.tableOperations().list().contains(tableName)) {
                LOGGER.info("creating table: " + tableName);
                try {
                    connector.tableOperations().create(tableName);
                } catch (TableExistsException e) {
                    // This sometimes happens. Just ignore since the table is present.
                }
            }

            IteratorSetting is = new IteratorSetting(ROW_DELETING_ITERATOR_PRIORITY, ROW_DELETING_ITERATOR_NAME, RowDeletingIterator.class);
            if (!connector.tableOperations().listIterators(tableName).containsKey(ROW_DELETING_ITERATOR_NAME)) {
                try {
                    connector.tableOperations().attachIterator(tableName, is);
                } catch (AccumuloException e) {
                    absorbIteratorNameConflictException(e);
                }
            }
            initializedTables.add(tableName);
        } catch (Exception e) {
            throw new SimpleOrmException("Could not initialize table", e);
        }
    }

    private void absorbIteratorNameConflictException(AccumuloException ex) throws AccumuloException {
        // Like the TableExistsException, this sometimes happens.
        // The exception inspection here depends on the implementation of
        // org.apache.accumulo.core.client.impl.TableOperationsHelper#checkIteratorConflicts(), and is therefore
        // fragile.
        boolean isErrorOk = false;
        Throwable cause = ex.getCause();
        if (cause instanceof IllegalArgumentException) {
            if (cause.getMessage().contains("iterator name conflict")) {
                isErrorOk = true;
            }
        }
        if (!isErrorOk) {
            throw ex;
        }
    }
}
