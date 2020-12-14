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
import com.mware.ge.store.StorableVertex;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import com.mware.ge.accumulo.AccumuloGraph;
import com.mware.ge.accumulo.iterator.VertexIterator;
import com.mware.ge.accumulo.iterator.model.VertexElementData;
import com.mware.ge.mutation.PropertyDeleteMutation;
import com.mware.ge.mutation.PropertySoftDeleteMutation;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;

public class AccumuloVertexInputFormat extends AccumuloElementInputFormatBase<Vertex> {
    private static VertexIterator vertexIterator;

    public static void setInputInfo(Job job, AccumuloGraph graph, String instanceName, String zooKeepers, String principal, AuthenticationToken token, String[] authorizations) throws AccumuloSecurityException {
        String tableName = graph.getVerticesTableName();
        setInputInfo(job, instanceName, zooKeepers, principal, token, authorizations, tableName);
    }

    @Override
    protected Vertex createElementFromRow(AccumuloGraph graph, PeekingIterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        return createVertex(graph, row, authorizations);
    }

    public static Vertex createVertex(AccumuloGraph graph, Iterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        try {
            FetchHints fetchHints = graph.getDefaultFetchHints();
            VertexElementData vertexElementData = getVertexIterator(graph).createElementDataFromRows(row);
            if (vertexElementData == null) {
                return null;
            }
            Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(vertexElementData.visibility.toString()));
            Iterable<Property> properties = makePropertiesFromElementData(graph, vertexElementData, graph.toIteratorFetchHints(fetchHints));
            Iterable<PropertyDeleteMutation> propertyDeleteMutations = null;
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = null;
            Iterable<Visibility> hiddenVisibilities = Iterables.transform(vertexElementData.hiddenVisibilities, new Function<Text, Visibility>() {
                @Nullable
                @Override
                public Visibility apply(Text visibilityText) {
                    return AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(visibilityText.toString()));
                }
            });
            ImmutableSet<String> extendedDataTableNames = vertexElementData.extendedTableNames.size() > 0
                    ? ImmutableSet.copyOf(vertexElementData.extendedTableNames)
                    : null;

            return new StorableVertex(
                    graph,
                    vertexElementData.id.toString(),
                    vertexElementData.conceptType.toString(),
                    null,
                    visibility,
                    properties,
                    propertyDeleteMutations,
                    propertySoftDeleteMutations,
                    hiddenVisibilities,
                    extendedDataTableNames,
                    vertexElementData.inEdges,
                    vertexElementData.outEdges,
                    vertexElementData.timestamp,
                    fetchHints,
                    authorizations
            );
        } catch (Throwable ex) {
            throw new GeException("Failed to create vertex", ex);
        }
    }

    private static VertexIterator getVertexIterator(AccumuloGraph graph) {
        if (vertexIterator == null) {
            vertexIterator = new VertexIterator(graph.toIteratorFetchHints(graph.getDefaultFetchHints()), true);
        }
        return vertexIterator;
    }
}

