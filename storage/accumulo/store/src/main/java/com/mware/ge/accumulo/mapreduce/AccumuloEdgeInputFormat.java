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
package com.mware.ge.accumulo.mapreduce;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.mware.ge.*;
import com.mware.ge.store.StorableEdge;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import com.mware.ge.accumulo.AccumuloGraph;
import com.mware.ge.accumulo.iterator.EdgeIterator;
import com.mware.ge.accumulo.iterator.model.EdgeElementData;
import com.mware.ge.mutation.PropertyDeleteMutation;
import com.mware.ge.mutation.PropertySoftDeleteMutation;

import javax.annotation.Nullable;
import java.util.Map;

public class AccumuloEdgeInputFormat extends AccumuloElementInputFormatBase<Edge> {
    private static EdgeIterator edgeIterator;

    public static void setInputInfo(Job job, AccumuloGraph graph, String instanceName, String zooKeepers, String principal, AuthenticationToken token, String[] authorizations) throws AccumuloSecurityException {
        String tableName = graph.getEdgesTableName();
        setInputInfo(job, instanceName, zooKeepers, principal, token, authorizations, tableName);
    }

    @Override
    protected Edge createElementFromRow(
            AccumuloGraph graph,
            PeekingIterator<Map.Entry<Key, Value>> row,
            Authorizations authorizations
    ) {
        try {
            FetchHints fetchHints = graph.getDefaultFetchHints();
            EdgeElementData edgeElementData = getEdgeIterator(graph).createElementDataFromRows(row);
            if (edgeElementData == null) {
                return null;
            }
            Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(edgeElementData.visibility.toString()));
            Iterable<Property> properties = makePropertiesFromElementData(graph, edgeElementData, graph.toIteratorFetchHints(fetchHints));
            Iterable<PropertyDeleteMutation> propertyDeleteMutations = null;
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = null;
            Iterable<Visibility> hiddenVisibilities = Iterables.transform(edgeElementData.hiddenVisibilities, new Function<Text, Visibility>() {
                @Nullable
                @Override
                public Visibility apply(Text visibilityText) {
                    return AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(visibilityText.toString()));
                }
            });
            ImmutableSet<String> extendedDataTableNames = edgeElementData.extendedTableNames.size() > 0
                    ? ImmutableSet.copyOf(edgeElementData.extendedTableNames)
                    : null;
            return new StorableEdge(
                    graph,
                    edgeElementData.id.toString(),
                    edgeElementData.outVertexId.toString(),
                    edgeElementData.inVertexId.toString(),
                    edgeElementData.label.toString(),
                    null,
                    visibility,
                    properties,
                    propertyDeleteMutations,
                    propertySoftDeleteMutations,
                    hiddenVisibilities,
                    extendedDataTableNames,
                    edgeElementData.timestamp,
                    fetchHints,
                    authorizations
            );
        } catch (Throwable ex) {
            throw new GeException("Failed to create vertex", ex);
        }
    }

    private EdgeIterator getEdgeIterator(AccumuloGraph graph) {
        if (edgeIterator == null) {
            edgeIterator = new EdgeIterator(graph.toIteratorFetchHints(graph.getDefaultFetchHints()), true);
        }
        return edgeIterator;
    }
}

