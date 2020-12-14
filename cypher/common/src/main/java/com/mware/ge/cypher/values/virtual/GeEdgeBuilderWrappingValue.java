package com.mware.ge.cypher.values.virtual;

import com.mware.ge.Direction;
import com.mware.ge.EdgeBuilderBase;
import com.mware.ge.ElementType;
import com.mware.ge.cypher.ge.GeCypherQueryContext;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Values;
import com.mware.ge.values.virtual.MapValue;
import com.mware.ge.values.virtual.MapValueBuilder;
import com.mware.ge.values.virtual.NodeValue;
import com.mware.ge.values.virtual.RelationshipValue;
import com.mware.ge.mutation.EdgeMutation;

public class GeEdgeBuilderWrappingValue extends RelationshipValue {
    private EdgeMutation edge;
    private GeCypherQueryContext queryContext;
    private volatile TextValue type;
    private volatile MapValue properties;

    public GeEdgeBuilderWrappingValue(EdgeMutation edge, GeCypherQueryContext queryContext) {
        super(edge.getId());
        this.edge = edge;
        this.queryContext = queryContext;
    }

    @Override
    public NodeValue startNode() {
        if (queryContext.getElementBuilders().containsKey(id())) {
            EdgeBuilderBase ebb = (EdgeBuilderBase) queryContext.getElementBuilders().get(id());
            String endNodeId = ebb.getVertexId(Direction.OUT);
            return queryContext.getVertexById(endNodeId, true);
        } else {
            throw new IllegalStateException("I don't know how to handle this yet");
        }
    }

    @Override
    public NodeValue endNode() {
        if (queryContext.getElementBuilders().containsKey(id())) {
            EdgeBuilderBase ebb = (EdgeBuilderBase) queryContext.getElementBuilders().get(id());
            String endNodeId = ebb.getVertexId(Direction.IN);
            return queryContext.getVertexById(endNodeId, true);
        } else {
            throw new IllegalStateException("I don't know how to handle this yet");
        }
    }

    @Override
    public TextValue type() {
        TextValue t = type;
        if (t == null) {
            synchronized (this) {
                t = type;
                if (t == null) {
                    t = type = Values.stringValue(edge.hasChanges() ? edge.getNewEdgeLabel() : edge.getEdgeLabel());
                }
            }
        }
        return t;
    }

    @Override
    public MapValue properties() {
        MapValue m = properties;
        if (m == null) {
            synchronized (this) {
                m = properties;
                if (m == null) {
                    MapValueBuilder builder = new MapValueBuilder();
                    queryContext.getElementProperties(edge.getId(), ElementType.EDGE)
                            .forEach((k, v) -> builder.add(k, v));
                    m = properties = builder.build();
                }
            }
        }
        return m;
    }
}
