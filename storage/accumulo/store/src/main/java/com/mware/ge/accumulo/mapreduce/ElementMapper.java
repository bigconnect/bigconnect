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

import com.mware.ge.*;
import com.mware.ge.accumulo.*;
import com.mware.ge.id.IdGenerator;
import com.mware.ge.id.NameSubstitutionStrategy;
import com.mware.ge.serializer.GeSerializer;
import com.mware.ge.store.StorableEdge;
import com.mware.ge.store.StorableGraph;
import com.mware.ge.store.StorableVertex;
import com.mware.ge.store.mutations.ElementMutationBuilder;
import com.mware.ge.store.mutations.StoreMutation;
import com.mware.ge.store.util.StreamingPropertyValueStorageStrategy;
import com.mware.ge.util.IncreasingTime;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class ElementMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements StorableGraph {
    public static final String GRAPH_CONFIG_PREFIX = "graphConfigPrefix";
    private ElementMutationBuilder elementMutationBuilder;
    private ElementMapperGraph graph;
    private NameSubstitutionStrategy nameSubstitutionStrategy;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        String configPrefix = context.getConfiguration().get(GRAPH_CONFIG_PREFIX, "");
        AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(context.getConfiguration(), configPrefix);
        String tableNamePrefix = accumuloGraphConfiguration.getTableNamePrefix();
        final Text edgesTableName = new Text(AccumuloGraph.getEdgesTableName(tableNamePrefix));
        final Text dataTableName = new Text(AccumuloGraph.getDataTableName(tableNamePrefix));
        final Text verticesTableName = new Text(AccumuloGraph.getVerticesTableName(tableNamePrefix));
        final Text extendedDataTableName = new Text(AccumuloGraph.getExtendedDataTableName(tableNamePrefix));

        this.graph = new ElementMapperGraph(this, accumuloGraphConfiguration);
        GeSerializer geSerializer = accumuloGraphConfiguration.createSerializer(this.graph);
        nameSubstitutionStrategy = accumuloGraphConfiguration.createSubstitutionStrategy(this.graph);
        StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy = accumuloGraphConfiguration.createStreamingPropertyValueStorageStrategy(this.graph);
        this.elementMutationBuilder = new ElementMutationBuilder(streamingPropertyValueStorageStrategy, this, geSerializer) {
            @Override
            protected void saveVertexMutation(StoreMutation m) {
                try {
                    ElementMapper.this.saveVertexMutation(context, verticesTableName, m);
                } catch (Exception e) {
                    throw new RuntimeException("Could not save vertex", e);
                }
            }

            @Override
            protected void saveEdgeMutation(StoreMutation m) {
                try {
                    ElementMapper.this.saveEdgeMutation(context, edgesTableName, m);
                } catch (Exception e) {
                    throw new RuntimeException("Could not save edge", e);
                }
            }

            @Override
            protected void saveExtendedDataMutation(ElementType elementType, StoreMutation m) {
                try {
                    ElementMapper.this.saveExtendedDataMutation(context, extendedDataTableName, m);
                } catch (Exception e) {
                    throw new RuntimeException("Could not save edge", e);
                }
            }

            @Override
            protected NameSubstitutionStrategy getNameSubstitutionStrategy() {
                return nameSubstitutionStrategy;
            }

            @Override
            public void saveDataMutation(StoreMutation m) {
                try {
                    ElementMapper.this.saveDataMutation(context, dataTableName, m);
                } catch (Exception e) {
                    throw new RuntimeException("Could not save data", e);
                }
            }
        };
    }

    protected abstract void saveDataMutation(Context context, Text dataTableName, StoreMutation m) throws IOException, InterruptedException;

    protected abstract void saveEdgeMutation(Context context, Text edgesTableName, StoreMutation m) throws IOException, InterruptedException;

    protected abstract void saveVertexMutation(Context context, Text verticesTableName, StoreMutation m) throws IOException, InterruptedException;

    protected abstract void saveExtendedDataMutation(Context context, Text tableName, StoreMutation m) throws IOException, InterruptedException;

    public VertexBuilder prepareVertex(Vertex vertex) {
        return prepareVertex(vertex.getId(),  null, vertex.getVisibility(), vertex.getConceptType());
    }

    public VertexBuilder prepareVertex(String vertexId, Visibility visibility, String conceptType) {
        return prepareVertex(vertexId, null, visibility, conceptType);
    }

    public VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility, String conceptType) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        return new VertexBuilder(vertexId, conceptType, visibility) {
            @Override
            public Vertex save(Authorizations authorizations) {
                AccumuloGraph graph = null;
                Iterable<Visibility> hiddenVisibilities = null;

                // This has to occur before createVertex since it will mutate the properties
                elementMutationBuilder.saveVertexBuilder(graph, this, timestampLong);

                return new StorableVertex(
                        graph,
                        getId(),
                        getConceptType(),
                        getNewConceptType(),
                        getVisibility(),
                        getProperties(),
                        getPropertyDeletes(),
                        getPropertySoftDeletes(),
                        hiddenVisibilities,
                        getExtendedDataTableNames(),
                        timestampLong,
                        FetchHints.ALL_INCLUDING_HIDDEN,
                        authorizations
                );
            }
        };
    }

    public Edge addEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertex, inVertex, label, null, visibility).save(authorizations);
    }

    public EdgeBuilderByVertexId prepareEdge(Edge edge) {
        return prepareEdge(
                edge.getId(),
                edge.getVertexId(Direction.OUT),
                edge.getVertexId(Direction.IN),
                edge.getLabel(),
                edge.getTimestamp(),
                edge.getVisibility()
        );
    }

    public EdgeBuilderByVertexId prepareEdge(
            String edgeId,
            String outVertexId,
            String inVertexId,
            String label,
            Visibility visibility
    ) {
        return prepareEdge(edgeId, outVertexId, inVertexId, label, null, visibility);
    }

    public EdgeBuilderByVertexId prepareEdge(
            String edgeId,
            String outVertexId,
            String inVertexId,
            String label,
            Long timestamp,
            Visibility visibility
    ) {
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        return new EdgeBuilderByVertexId(edgeId, outVertexId, inVertexId, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                // This has to occur before createEdge since it will mutate the properties
                elementMutationBuilder.saveEdgeBuilder(null, this, timestampLong);

                StorableEdge edge = new StorableEdge(
                        null,
                        getId(),
                        getVertexId(Direction.OUT),
                        getVertexId(Direction.IN),
                        getEdgeLabel(),
                        getNewEdgeLabel(),
                        getVisibility(),
                        getProperties(),
                        getPropertyDeletes(),
                        getPropertySoftDeletes(),
                        null,
                        getExtendedDataTableNames(),
                        timestampLong,
                        FetchHints.ALL_INCLUDING_HIDDEN,
                        authorizations
                );
                return edge;
            }
        };
    }

    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        return prepareEdge(edgeId, outVertex, inVertex, label, null, visibility);
    }

    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility) {
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                // This has to occur before createEdge since it will mutate the properties
                elementMutationBuilder.saveEdgeBuilder(null, this, timestampLong);

                StorableEdge edge = new StorableEdge(
                        null,
                        getId(),
                        getOutVertex().getId(),
                        getInVertex().getId(),
                        getEdgeLabel(),
                        getNewEdgeLabel(),
                        getVisibility(),
                        getProperties(),
                        getPropertyDeletes(),
                        getPropertySoftDeletes(),
                        null,
                        getExtendedDataTableNames(),
                        timestampLong,
                        FetchHints.ALL_INCLUDING_HIDDEN,
                        authorizations
                );
                return edge;
            }
        };
    }

    public IdGenerator getIdGenerator() {
        throw new GeException("not implemented");
    }

    public Graph getGraph() {
        return graph;
    }
}
