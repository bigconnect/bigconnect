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

import com.mware.ge.GeException;
import com.mware.ge.GraphMetadataEntry;
import com.mware.ge.GraphMetadataStore;
import com.mware.ge.IdRange;
import com.mware.ge.collection.Pair;
import com.mware.ge.util.JavaSerializableUtils;
import com.mware.ge.util.LookAheadIterable;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class KVMetadataStore extends GraphMetadataStore {
    private KVStoreGraph graph;

    public KVMetadataStore(KVStoreGraph graph) {
        this.graph = graph;
    }

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        return new LookAheadIterable<Pair<byte[], byte[]>, GraphMetadataEntry>() {
            ScanIterator iter;

            @Override
            protected boolean isIncluded(Pair<byte[], byte[]> src, GraphMetadataEntry graphMetadataEntry) {
                return graphMetadataEntry != null;
            }

            @Override
            protected GraphMetadataEntry convert(Pair<byte[], byte[]> pair) {
                return new GraphMetadataEntry(
                        new String(KVKeyUtils.decodeId(pair.first())),
                        pair.other()
                );
            }

            @Override
            protected Iterator<Pair<byte[], byte[]>> createIterator() {
                iter = graph.getKvStore().scan(graph.getMetadataTableName());
                return iter;
            }

            @Override
            public void close() {
                IOUtils.closeQuietly(iter);
            }
        };
    }

    @Override
    public Object getMetadata(String key) {
        try (ScanIterator iter = graph.getKvStore().scan(graph.getMetadataTableName(), new IdRange(key))) {
            if (iter.hasNext()) {
                Pair<byte[], byte[]> pair = iter.next();
                return new GraphMetadataEntry(
                        new String(KVKeyUtils.decodeId(pair.first())),
                        pair.other()
                ).getValue();
            }

            return null;
        } catch (IOException ex) {
            throw new GeException(ex);
        }
    }

    @Override
    public void setMetadata(String key, Object value) {
        byte[] bvalue = JavaSerializableUtils.objectToBytes(value);
        byte[] bkey = KVKeyUtils.encodeId(key.getBytes(StandardCharsets.UTF_8));
        graph.kvStore.put(graph.getMetadataTableName(), bkey, bvalue);
    }

    @Override
    public void reloadMetadata() {

    }

    @Override
    public void removeMetadata(String key) {
        byte[] bkey = KVKeyUtils.encodeId(key.getBytes());
        graph.kvStore.delete(graph.getMetadataTableName(), bkey);
    }

    @Override
    public void close() {

    }

    @Override
    public void drop() {

    }
}
