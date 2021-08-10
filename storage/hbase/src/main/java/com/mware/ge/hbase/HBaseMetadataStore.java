package com.mware.ge.hbase;

import com.mware.ge.Authorizations;
import com.mware.ge.GeException;
import com.mware.ge.GraphMetadataEntry;
import com.mware.ge.store.DistributedMetadataStore;
import com.mware.ge.store.StorableElement;
import com.mware.ge.store.mutations.StoreColumnUpdate;
import com.mware.ge.store.mutations.StoreMutation;
import com.mware.ge.util.LookAheadIterable;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HBaseMetadataStore extends DistributedMetadataStore {
    private static final Authorizations METADATA_AUTHORIZATIONS = new Authorizations();
    protected static final byte[] METADATA_COLUMN_FAMILY = "m".getBytes();

    public HBaseMetadataStore(HBaseGraph graph) {
        super(graph);
    }

    @Override
    protected void write(StoreMutation m) throws IOException {
        ((HBaseGraph) graph).getMetadataWriter()
                .mutate(toHBaseMutations(m));
    }

    @Override
    protected void delete(StoreMutation m) throws IOException {
        ((HBaseGraph) graph).getMetadataWriter()
                .mutate(toHBaseMutations(m));
    }

    @Override
    protected Iterable<GraphMetadataEntry> getAllMetadata() {
        final HBaseGraph hBaseGraph = (HBaseGraph) graph;

        return new LookAheadIterable<Result, GraphMetadataEntry>() {
            ResultScanner scanner;

            @Override
            protected boolean isIncluded(Result src, GraphMetadataEntry graphMetadataEntry) {
                return true;
            }

            @Override
            protected GraphMetadataEntry convert(Result entry) {
                String key = new String(entry.getRow());
                byte[] value = entry.getValue(StorableElement.METADATA_COLUMN_FAMILY.getBytes(), StorableElement.METADATA_COLUMN_QUALIFIER.getBytes());
                return new GraphMetadataEntry(key, value);
            }

            @Override
            protected Iterator<Result> createIterator() {
                try {
                    scanner = hBaseGraph.createScanner(graph.getMetadataTableName(), METADATA_AUTHORIZATIONS);
                    return scanner.iterator();
                } catch (IOException ex) {
                    throw new GeException("Could not create metadata scanner", ex);
                }
            }
        };
    }

    public static List<Mutation> toHBaseMutations(StoreMutation sm) {
        List<StoreColumnUpdate> updates = sm.getUpdates();
        List<Mutation> mutations = new ArrayList<>();
        for (int i = 0; i < updates.size(); i++) {
            StoreColumnUpdate update = updates.get(i);
            if (update.isDeleted()) {
                if (update.hasTimestamp()) {
                    mutations.add(new Delete(sm.getRow(), update.getTimestamp())
                            .addColumn(transformCF(update.getColumnFamily()), transformCQ(update.getColumnQualifier()))
                            .setCellVisibility(new CellVisibility(new String(update.getColumnVisibility())))
                    );
                } else {
                    mutations.add(new Delete(sm.getRow())
                            .addColumn(transformCF(update.getColumnFamily()), transformCQ(update.getColumnQualifier()))
                            .setCellVisibility(new CellVisibility(new String(update.getColumnVisibility())))
                    );
                }
            } else {
                if (update.hasTimestamp()) {
                    mutations.add(new Put(sm.getRow(), update.getTimestamp())
                            .addColumn(transformCF(update.getColumnFamily()), transformCQ(update.getColumnQualifier()), update.getValue())
                            .setCellVisibility(new CellVisibility(new String(update.getColumnVisibility())))
                    );
                } else {
                    mutations.add(new Put(sm.getRow())
                            .addColumn(transformCF(update.getColumnFamily()), transformCQ(update.getColumnQualifier()), update.getValue())
                            .setCellVisibility(new CellVisibility(new String(update.getColumnVisibility())))
                    );
                }
            }
        }
        return mutations;
    }

    private static byte[] transformCQ(byte[] columnQualifier) {
        return columnQualifier.length == 0 ? "key".getBytes() : columnQualifier;
    }

    private static byte[] transformCF(byte[] columnFamily) {
        return columnFamily.length == 0 ? "m".getBytes() : columnFamily;
    }
}
