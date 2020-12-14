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

import com.mware.ge.Authorizations;
import com.mware.ge.GeException;
import com.mware.ge.GraphMetadataEntry;
import com.mware.ge.store.DistributedMetadataStore;
import com.mware.ge.store.mutations.StoreMutation;
import com.mware.ge.util.LookAheadIterable;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.VersioningIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class AccumuloMetadataStore extends DistributedMetadataStore {
    private static final Authorizations METADATA_AUTHORIZATIONS = new Authorizations();

    public AccumuloMetadataStore(AccumuloGraph graph) {
        super(graph);
    }

    @Override
    protected void write(StoreMutation m) throws IOException {
        BatchWriter writer = ((AccumuloGraph) graph).getMetadataWriter();
        try {
            writer.addMutation(AccumuloGraph.toAccumuloMutation(m));
        } catch (MutationsRejectedException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void delete(StoreMutation m) throws IOException {
        BatchWriter writer = ((AccumuloGraph) graph).getMetadataWriter();
        try {
            writer.addMutation(AccumuloGraph.toAccumuloMutation(m));
        } catch (MutationsRejectedException e) {
            throw new IOException(e);
        }
    }

    public Iterable<GraphMetadataEntry> getAllMetadata() {
        final AccumuloGraph accumuloGraph = (AccumuloGraph) graph;

        return new LookAheadIterable<Map.Entry<Key, Value>, GraphMetadataEntry>() {
            public Scanner scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, GraphMetadataEntry graphMetadataEntry) {
                return true;
            }

            @Override
            protected GraphMetadataEntry convert(Map.Entry<Key, Value> entry) {
                String key = entry.getKey().getRow().toString();
                return new GraphMetadataEntry(key, entry.getValue().get());
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                try {
                    scanner = accumuloGraph.createScanner(graph.getMetadataTableName(), null, METADATA_AUTHORIZATIONS);

                    IteratorSetting versioningIteratorSettings = new IteratorSetting(
                            90,
                            VersioningIterator.class.getSimpleName(),
                            VersioningIterator.class
                    );
                    VersioningIterator.setMaxVersions(versioningIteratorSettings, 1);
                    scanner.addScanIterator(versioningIteratorSettings);

                    return scanner.iterator();
                } catch (TableNotFoundException ex) {
                    throw new GeException("Could not create metadata scanner", ex);
                }
            }

            @Override
            public void close() {
                super.close();
                this.scanner.close();
            }
        };
    }
}
