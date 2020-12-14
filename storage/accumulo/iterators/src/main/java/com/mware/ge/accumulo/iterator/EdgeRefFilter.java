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

import com.mware.ge.accumulo.iterator.util.SetOfStringsEncoder;
import com.mware.ge.store.StorableEdgeInfo;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

public class EdgeRefFilter extends Filter {
    private static final String SETTING_VERTEX_IDS = "vertexId";
    private Set<String> vertexIdsSet;
    private List<Text> nonVisibleEdges = Arrays.asList(
            VertexIterator.CF_IN_EDGE_HIDDEN,
            VertexIterator.CF_IN_EDGE_SOFT_DELETE,
            VertexIterator.CF_OUT_EDGE_HIDDEN,
            VertexIterator.CF_OUT_EDGE_SOFT_DELETE);

    public static void setVertexIds(IteratorSetting settings, Set<String> vertexIdsSet) {
        settings.addOption(SETTING_VERTEX_IDS, SetOfStringsEncoder.encodeToString(vertexIdsSet));
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        this.vertexIdsSet = SetOfStringsEncoder.decodeFromString(options.get(SETTING_VERTEX_IDS));
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        EdgeRefFilter edgeRefFilter = (EdgeRefFilter) super.deepCopy(env);
        edgeRefFilter.vertexIdsSet = new HashSet<>(this.vertexIdsSet);
        return edgeRefFilter;
    }

    @Override
    public boolean accept(Key k, Value v) {
        Text columnFamily = k.getColumnFamily();
        if (columnFamily.equals(VertexIterator.CF_IN_EDGE) || columnFamily.equals(VertexIterator.CF_OUT_EDGE)) {
            StorableEdgeInfo edgeInfo = new StorableEdgeInfo(v.get(), k.getTimestamp());
            return vertexIdsSet.contains(edgeInfo.getVertexId());
        }
        return nonVisibleEdges.contains(columnFamily);
    }
}
