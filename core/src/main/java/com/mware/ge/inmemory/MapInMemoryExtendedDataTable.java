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
package com.mware.ge.inmemory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.mware.ge.*;
import com.mware.ge.security.SecurityAuthorizations;
import com.mware.ge.security.VisibilityEvaluator;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.StreamUtils;
import com.mware.ge.values.storable.Value;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.collect.Streams.stream;

public class MapInMemoryExtendedDataTable extends InMemoryExtendedDataTable {
    private Map<ElementType, ElementTypeData> elementTypeData = new HashMap<>();

    @Override
    public ImmutableSet<String> getTableNames(
            ElementType elementType,
            String elementId,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        ElementTypeData data = elementTypeData.get(elementType);
        if (data == null) {
            return ImmutableSet.of();
        }
        return data.getTableNames(elementId, fetchHints, authorizations);
    }

    @Override
    public Iterable<ExtendedDataRow> getTable(
            ElementType elementType,
            String elementId,
            String tableName,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        ElementTypeData data = elementTypeData.get(elementType);
        if (data == null) {
            return ImmutableList.of();
        }
        return data.getTable(elementId, tableName, fetchHints, authorizations);
    }

    @Override
    public synchronized void addData(
            ExtendedDataRowId rowId,
            String column,
            String key,
            Value value,
            long timestamp,
            Visibility visibility
    ) {
        ElementTypeData data = elementTypeData.computeIfAbsent(rowId.getElementType(), k -> new ElementTypeData());
        data.addData(rowId, column, key, value, timestamp, visibility);
    }

    @Override
    public void remove(ExtendedDataRowId rowId) {
        ElementTypeData data = elementTypeData.get(rowId.getElementType());
        if (data != null) {
            data.removeData(rowId);
        }
    }

    @Override
    public void removeColumn(ExtendedDataRowId rowId, String columnName, String key, Visibility visibility) {
        ElementTypeData data = elementTypeData.get(rowId.getElementType());
        if (data != null) {
            data.removeColumn(rowId, columnName, key, visibility);
        }
    }

    private static class ElementTypeData {
        Map<String, ElementData> elementData = new HashMap<>();

        public ImmutableSet<String> getTableNames(String elementId, FetchHints fetchHints, Authorizations authorizations) {
            ElementData data = elementData.get(elementId);
            if (data == null) {
                return ImmutableSet.of();
            }
            return data.getTableNames(fetchHints, authorizations);
        }

        public Iterable<ExtendedDataRow> getTable(
                String elementId,
                String tableName,
                FetchHints fetchHints,
                Authorizations authorizations
        ) {
            ElementData data = elementData.get(elementId);
            if (data == null) {
                return ImmutableList.of();
            }
            return data.getTable(tableName, fetchHints, authorizations);
        }

        public synchronized void addData(
                ExtendedDataRowId rowId,
                String column,
                String key,
                Value value,
                long timestamp,
                Visibility visibility
        ) {
            ElementData data = elementData.computeIfAbsent(rowId.getElementId(), k -> new ElementData());
            data.addData(rowId, column, key, value, timestamp, visibility);
        }

        public void removeData(ExtendedDataRowId rowId) {
            ElementData data = elementData.get(rowId.getElementId());
            if (data != null) {
                data.removeData(rowId);
            }
        }

        public void removeColumn(ExtendedDataRowId rowId, String columnName, String key, Visibility visibility) {
            ElementData data = elementData.get(rowId.getElementId());
            if (data != null) {
                data.removeColumn(rowId, columnName, key, visibility);
            }
        }
    }

    private static class ElementData {
        private final Map<String, Table> tables = new HashMap<>();

        public ImmutableSet<String> getTableNames(FetchHints fetchHints, Authorizations authorizations) {
            VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(new SecurityAuthorizations(authorizations.getAuthorizations()));
            return tables.entrySet().stream()
                    .filter(entry -> entry.getValue().canRead(visibilityEvaluator, fetchHints))
                    .map(Map.Entry::getKey)
                    .collect(StreamUtils.toImmutableSet());
        }

        public Iterable<ExtendedDataRow> getTable(String tableName, FetchHints fetchHints, Authorizations authorizations) {
            if (tableName == null) {
                return getTableNames(fetchHints, authorizations).stream()
                        .flatMap(t -> stream(getTable(t, fetchHints, authorizations)))
                        .collect(Collectors.toList());
            }
            VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(new SecurityAuthorizations(authorizations.getAuthorizations()));
            Table table = tables.get(tableName);
            if (table == null) {
                throw new GeException("Invalid table '" + tableName + "'");
            }
            Iterable<ExtendedDataRow> rows = table.getRows(visibilityEvaluator, fetchHints);
            if (!rows.iterator().hasNext()) {
                return Collections.emptyList();
            }
            return rows;
        }

        public synchronized void addData(
                ExtendedDataRowId rowId,
                String column,
                String key,
                Value value,
                long timestamp,
                Visibility visibility
        ) {
            Table table = tables.computeIfAbsent(rowId.getTableName(), k -> new Table());
            table.addData(rowId, column, key, value, timestamp, visibility);
        }

        public void removeData(ExtendedDataRowId rowId) {
            Table table = tables.get(rowId.getTableName());
            if (table != null) {
                table.removeData(rowId);
            }
        }

        public void removeColumn(ExtendedDataRowId rowId, String columnName, String key, Visibility visibility) {
            Table table = tables.get(rowId.getTableName());
            if (table != null) {
                table.removeColumn(rowId, columnName, key, visibility);
            }
        }

        private class Table {
            private final TreeSet<InMemoryExtendedDataRow> rows = new TreeSet<>();

            public Iterable<ExtendedDataRow> getRows(VisibilityEvaluator visibilityEvaluator, FetchHints fetchHints) {
                return rows.stream()
                        .map(row -> row.toReadable(visibilityEvaluator, fetchHints))
                        .filter(Objects::nonNull)
                        .filter(row -> IterableUtils.count(row.getProperties()) > 0)
                        .collect(Collectors.toList());
            }

            public boolean canRead(VisibilityEvaluator visibilityEvaluator, FetchHints fetchHints) {
                return rows.stream().anyMatch(r -> r.canRead(visibilityEvaluator, fetchHints));
            }

            public void addData(
                    ExtendedDataRowId rowId,
                    String column,
                    String key,
                    Value value,
                    long timestamp,
                    Visibility visibility
            ) {
                InMemoryExtendedDataRow row = findOrAddRow(rowId);
                row.addColumn(column, key, value, timestamp, visibility);
            }

            private InMemoryExtendedDataRow findOrAddRow(ExtendedDataRowId rowId) {
                InMemoryExtendedDataRow row = findRow(rowId);
                if (row != null) {
                    return row;
                }
                row = new InMemoryExtendedDataRow(rowId, FetchHints.ALL);
                rows.add(row);
                return row;
            }

            private InMemoryExtendedDataRow findRow(ExtendedDataRowId rowId) {
                for (InMemoryExtendedDataRow row : rows) {
                    if (row.getId().equals(rowId)) {
                        return row;
                    }
                }
                return null;
            }

            public void removeData(ExtendedDataRowId rowId) {
                rows.removeIf(row -> row.getId().equals(rowId));
            }

            public void removeColumn(ExtendedDataRowId rowId, String columnName, String key, Visibility visibility) {
                InMemoryExtendedDataRow row = findRow(rowId);
                if (row == null) {
                    return;
                }
                row.removeColumn(columnName, key, visibility);
            }
        }
    }

}
