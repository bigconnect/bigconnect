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
package com.mware.ge.accumulo.iterator;

import com.mware.ge.accumulo.iterator.util.ByteArrayWrapper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowEncodingIterator;
import org.apache.hadoop.io.Text;
import com.mware.ge.accumulo.iterator.model.GeAccumuloIteratorException;
import com.mware.ge.accumulo.iterator.util.DataInputStreamUtils;
import com.mware.ge.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

public class VertexEdgeIdIterator extends RowEncodingIterator {
    @Override
    public SortedMap<Key, Value> rowDecoder(Key rowKey, Value rowValue) throws IOException {
        throw new GeAccumuloIteratorException("Not Implemented");
    }

    @Override
    public Value rowEncoder(List<Key> keys, List<Value> values) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        for (Key key : keys) {
            if (!key.getColumnFamily().equals(VertexIterator.CF_OUT_EDGE)
                    && !key.getColumnFamily().equals(VertexIterator.CF_IN_EDGE)) {
                continue;
            }
            Text edgeId = key.getColumnQualifier();
            DataOutputStreamUtils.encodeByteArray(out, edgeId.getBytes());
        }
        return new Value(baos.toByteArray());
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new VertexEdgeIdIterator();
    }

    public static Iterable<ByteArrayWrapper> decodeValue(final Value value) {
        return new Iterable<ByteArrayWrapper>() {
            @Override
            public Iterator<ByteArrayWrapper> iterator() {
                ByteArrayInputStream bais = new ByteArrayInputStream(value.get());
                final DataInputStream in = new DataInputStream(bais);
                return new Iterator<ByteArrayWrapper>() {
                    @Override
                    public boolean hasNext() {
                        try {
                            return in.available() > 0;
                        } catch (IOException e) {
                            throw new GeAccumuloIteratorException("Could not get available", e);
                        }
                    }

                    @Override
                    public ByteArrayWrapper next() {
                        try {
                            return DataInputStreamUtils.decodeByteArrayWrapper(in);
                        } catch (IOException e) {
                            throw new GeAccumuloIteratorException("Could not read text", e);
                        }
                    }

                    @Override
                    public void remove() {
                        throw new GeAccumuloIteratorException("not implemented");
                    }
                };
            }
        };
    }
}
