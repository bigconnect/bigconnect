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
package com.mware.ge.store.kv;

import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.google.common.collect.ImmutableSet;
import com.mware.ge.*;
import com.mware.ge.collection.CombiningIterable;
import com.mware.ge.collection.Iterators;
import com.mware.ge.collection.Pair;
import com.mware.ge.collection.PrefetchingIterator;

import com.mware.ge.store.*;
import com.mware.ge.store.decoder.*;
import com.mware.ge.store.mutations.ElementMutationBuilder;
import com.mware.ge.store.mutations.StoreColumnUpdate;
import com.mware.ge.store.mutations.StoreMutation;
import com.mware.ge.util.IncreasingTime;
import com.mware.ge.util.LookAheadIterable;
import org.apache.commons.io.IOUtils;
import org.apache.curator.shaded.com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public abstract class KVStoreGraph extends AbstractStorableGraph<StorableVertex, StorableEdge> {
    protected KVStore kvStore;

    public KVStoreGraph(StorableGraphConfiguration config) {
        super(config);

        kvStore = createStore();
        kvStore.open();
    }

    @Override
    public Iterable<Vertex> getVerticesInRange(IdRange idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return new LookAheadIterable<VertexElementData, Vertex>() {
            ScanIterator iter;

            @Override
            protected boolean isIncluded(VertexElementData src, Vertex vertex) {
                return vertex != null;
            }

            @Override
            protected Vertex convert(VertexElementData vertexElementData) {
                if (vertexElementData == null) {
                    return null;
                }

                ImmutableSet<String> extendedDataTableNames = vertexElementData.extendedTableNames.size() > 0
                        ? ImmutableSet.copyOf(vertexElementData.extendedTableNames)
                        : ImmutableSet.of();

                boolean outEdgeLabelsOnly =
                        fetchHints.isIncludeEdgeLabelsAndCounts() && !(fetchHints.isIncludeAllEdgeRefs() || fetchHints.isIncludeOutEdgeRefs());

                boolean inEdgeLabelsOnly =
                        fetchHints.isIncludeEdgeLabelsAndCounts() && !(fetchHints.isIncludeAllEdgeRefs() || fetchHints.isIncludeInEdgeRefs());

                return new StorableVertex(
                        KVStoreGraph.this,
                        vertexElementData.id,
                        vertexElementData.conceptType,
                        null,
                        new Visibility(vertexElementData.visibility),
                        vertexElementData.getProperties(fetchHints),
                        null,
                        null,
                        vertexElementData.hiddenVisibilities.stream().map(Visibility::new).collect(Collectors.toList()),
                        extendedDataTableNames,
                        inEdgeLabelsOnly ? vertexElementData.inEdges.getEdgesWithCount() : vertexElementData.inEdges,
                        outEdgeLabelsOnly ? vertexElementData.outEdges.getEdgesWithCount() : vertexElementData.outEdges,
                        vertexElementData.timestamp,
                        fetchHints,
                        authorizations
                );
            }

            @Override
            protected Iterator<VertexElementData> createIterator() {
                iter = getKvStore().scan(getVerticesTableName(), idRange);

                Iterator<Pair<StoreKey, StoreValue>> mappingIterator =
                        Iterators.map(o -> Pair.of(KVKeyUtils.storeKey(o.first()), StoreValue.deserialize(o.other())), iter);
                return new VertexDecoder(Iterators.prefetching(mappingIterator), KVStoreGraph.this, fetchHints, authorizations).iterator();
            }

            @Override
            public void close() {
                IOUtils.closeQuietly(iter);
            }
        };
    }

    @Override
    public Iterable<Edge> getEdgesInRange(IdRange idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return new LookAheadIterable<EdgeElementData, Edge>() {
            ScanIterator iter;

            @Override
            protected boolean isIncluded(EdgeElementData src, Edge vertex) {
                return vertex != null;
            }

            @Override
            protected Edge convert(EdgeElementData edgeElementData) {
                if (edgeElementData == null) {
                    return null;
                }

                ImmutableSet<String> extendedDataTableNames = edgeElementData.extendedTableNames.size() > 0
                        ? ImmutableSet.copyOf(edgeElementData.extendedTableNames)
                        : ImmutableSet.of();

                return new StorableEdge(
                        KVStoreGraph.this,
                        edgeElementData.id,
                        edgeElementData.outVertexId,
                        edgeElementData.inVertexId,
                        edgeElementData.label,
                        null,
                        new Visibility(edgeElementData.visibility),
                        edgeElementData.getProperties(fetchHints),
                        null,
                        null,
                        edgeElementData.hiddenVisibilities.stream().map(Visibility::new).collect(Collectors.toList()),
                        extendedDataTableNames,
                        edgeElementData.timestamp,
                        fetchHints,
                        authorizations
                );
            }

            @Override
            protected Iterator<EdgeElementData> createIterator() {
                iter = kvStore.scan(getEdgesTableName(), idRange);
                Iterator<Pair<StoreKey, StoreValue>> mappingIterator =
                        Iterators.map(o -> Pair.of(KVKeyUtils.storeKey(o.first()), StoreValue.deserialize(o.other())), iter);
                return new EdgeDecoder(Iterators.prefetching(mappingIterator), KVStoreGraph.this, fetchHints, authorizations)
                        .iterator();
            }

            @Override
            public void close() {
                IOUtils.closeQuietly(iter);
            }
        };
    }

    @Override
    protected Iterable<ExtendedDataRow> getExtendedDataRowsInRange(
            List ranges,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        List<Iterable<ExtendedDataRow>> iterables = new ArrayList<>();

        for (IdRange range : (List<IdRange>) ranges) {
            iterables.add(new LookAheadIterable<Pair<StoreKey, StoreValue>, ExtendedDataRow>() {
                ScanIterator iter;
                PrefetchingIterator<Pair<StoreKey, StoreValue>> storeIterable;

                @Override
                protected boolean isIncluded(Pair<StoreKey, StoreValue> src, ExtendedDataRow extendedDataRow) {
                    return extendedDataRow != null;
                }

                @Override
                protected ExtendedDataRow convert(Pair<StoreKey, StoreValue> source) {
                    List<Pair<StoreKey, StoreValue>> mutations = new ArrayList<>();
                    mutations.add(source);

                    while (storeIterable.hasNext()) {
                        Pair<StoreKey, StoreValue> next = storeIterable.peek();
                        if (next != null) {
                            String id = source.first().id();
                            String nextId = next.first().id();
                            if (id.equals(nextId)) {
                                mutations.add(storeIterable.next());
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }

                    boolean deleted = false;
                    for (int i = 0; i < mutations.size(); i++) {
                        Pair<StoreKey, StoreValue> pair = mutations.get(i);
                        if (StorableElement.DELETE_ROW_COLUMN_FAMILY.equals(pair.first().cf())
                                && StorableElement.DELETE_ROW_COLUMN_QUALIFIER.equals(pair.first().cq())
                                && Arrays.equals(ElementMutationBuilder.DELETE_ROW_VALUE, pair.other().value())) {
                            deleted = true;
                        }
                    }

                    if (deleted)
                        return null;
                    else
                        return new ExtendedDataDecoder(KVStoreGraph.this, fetchHints, authorizations)
                                .decode(mutations);
                }

                @Override
                protected Iterator<Pair<StoreKey, StoreValue>> createIterator() {
                    iter = kvStore.scan(getExtendedDataTableName(), range);
                    Iterator<Pair<StoreKey, StoreValue>> mappingIterator =
                            Iterators.map(o -> Pair.of(KVKeyUtils.storeKey(o.first()), StoreValue.deserialize(o.other())), iter);

                    storeIterable = Iterators.prefetching(mappingIterator);
                    return storeIterable;
                }

                @Override
                public void close() {
                    IOUtils.closeQuietly(iter);
                }
            });
        }

        return new CombiningIterable<>(iterables);
    }


    @Override
    protected void addMutations(GeObjectType objectType, StoreMutation... mutations) {
        _addMutations(getTableFromElementType(objectType), mutations);
    }

    private String getTableFromElementType(GeObjectType objectType) {
        switch (objectType) {
            case VERTEX:
                return getVerticesTableName();
            case EDGE:
                return getEdgesTableName();
            case EXTENDED_DATA:
                return getExtendedDataTableName();
            default:
                throw new GeException("Unexpected object type: " + objectType);
        }
    }


    private void _addMutations(String tableName, StoreMutation... mutations) {
        for (StoreMutation m : mutations) {
            List<StoreColumnUpdate> updates = m.getUpdates();
            for (int i = 0; i < updates.size(); i++) {
                StoreColumnUpdate u = updates.get(i);
                ByteBuffer key = KVKeyUtils.keyFromMutation(m, u.getColumnFamily(), u.getColumnQualifier(), u.getColumnVisibility());
                if (u.isDeleted()) {
                    kvStore.delete(tableName, key.array());
                } else {
                    long ts = u.getTimestamp() == 0L ? IncreasingTime.currentTimeMillis() : u.getTimestamp();
                    byte[] storeValue = new StoreValue(ts, u.getValue()).serialize();
                    kvStore.put(tableName, key.array(), storeValue);

                }
            }
        }
    }

    @Override
    public void dumpGraph() {
        dumpTable(getVerticesTableName(), "VERTICES");
        dumpTable(getEdgesTableName(), "EDGES");

        try (ScanIterator iter = kvStore.scan(getExtendedDataTableName())) {
            Iterator<Pair<StoreKey, StoreValue>> iter2 =
                    Iterators.map(o -> Pair.of(KVKeyUtils.storeKey(o.first()), StoreValue.deserialize(o.other())), iter);

             List<DumpTableRow> data = new ArrayList<>();

            while (iter2.hasNext()) {
                Pair<StoreKey, StoreValue> pair = iter2.next();
                StoreKey key = pair.first();
                StoreValue v = pair.other();

                data.add(new DumpTableRow(key.id(), key.cf(), key.cq(), key.visibilityString(), v.ts(), ""));
            }

            File tempFile = File.createTempFile(getExtendedDataTableName(), "");
            String content = AsciiTable.getTable(data, Arrays.asList(
                    new Column().header("ID").with(row -> row.id),
                    new Column().header("CF").with(row -> row.cf),
                    new Column().header("CQ").with(row -> row.cq),
                    new Column().header("VIS").with(row -> row.vis),
                    new Column().header("TS").with(row -> String.valueOf(row.ts)),
                    new Column().header("VAL").with(row -> row.val)
            ));
            Files.write(content, tempFile, StandardCharsets.UTF_8);

        } catch (IOException ex) {
            ex.printStackTrace();
        }


        try (ScanIterator iter = kvStore.scan(getMetadataTableName())) {
            List<GraphMetadataEntry> data = new ArrayList<>();
            Iterator<GraphMetadataEntry> iter2 = Iterators.map(pair ->
                            new GraphMetadataEntry(
                                    new String(pair.first()),
                                    pair.other()
                            )
                    , iter);

            while (iter2.hasNext()) {
                data.add(iter2.next());
            }

            File tempFile = File.createTempFile(getMetadataTableName(), "");
            String content = AsciiTable.getTable(data, Arrays.asList(
                    new Column().header("Key").with(row -> row.getKey()),
                    new Column().header("Value").with(row -> row.getValue() == null ? null : row.getValue().toString())
            ));
            Files.write(content, tempFile, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void dumpTable(String table, String name) {
        try (ScanIterator iter = kvStore.scan(table)) {
            Iterator<Pair<StoreKey, StoreValue>> iter2 =
                    Iterators.map(o -> Pair.of(KVKeyUtils.storeKey(o.first()), StoreValue.deserialize(o.other())), iter);

            List<DumpTableRow> data = new ArrayList<>();

            while (iter2.hasNext()) {
                Pair<StoreKey, StoreValue> pair = iter2.next();
                StoreKey key = pair.first();
                StoreValue v = pair.other();
                Object obj;
                try {
                    obj = geSerializer.bytesToObject(pair.other().value());
                } catch (Throwable t) {
                    obj = new String(pair.other().value());
                }
                data.add(new DumpTableRow(key.id(), key.cf(), key.cq(), key.visibilityString(), v.ts(), obj != null ? obj.toString() : null));
            }

            File tempFile = File.createTempFile(table, "");
            String content = AsciiTable.getTable(data, Arrays.asList(
                    new Column().header("ID").with(row -> row.id),
                    new Column().header("CF").with(row -> row.cf),
                    new Column().header("CQ").with(row -> row.cq),
                    new Column().header("VIS").with(row -> row.vis),
                    new Column().header("TS").with(row -> String.valueOf(row.ts)),
                    new Column().header("VAL").with(row -> row.val)
            ));
            Files.write(content, tempFile, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static class DumpTableRow {
        public String id;
        public String cf;
        public String cq;
        public String vis;
        public long ts;
        public String val;

        public DumpTableRow(String id, String cf, String cq, String vis, long ts, String val) {
            this.id = id;
            this.cf = cf;
            this.cq = cq;
            this.vis = vis;
            this.ts = ts;
            this.val = val;
        }
    }

    protected abstract KVStore createStore();

    public KVStore getKvStore() {
        return kvStore;
    }
}
