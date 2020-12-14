package com.mware.ge.cypher.values.virtual;

import com.mware.ge.Direction;
import com.mware.ge.Edge;
import com.mware.ge.Element;
import com.mware.ge.ElementType;
import com.mware.ge.cypher.ge.GeCypherQueryContext;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import com.mware.ge.values.virtual.MapValue;
import com.mware.ge.values.virtual.MapValueBuilder;
import com.mware.ge.values.virtual.NodeValue;
import com.mware.ge.values.virtual.RelationshipValue;

import java.util.HashMap;
import java.util.Map;

public class GeEdgeWrappingValue extends RelationshipValue implements GraphLoadableValue {
    private Edge edge;
    private volatile NodeValue startNode;
    private volatile NodeValue endNode;
    private volatile MapValue properties;
    private volatile TextValue type;
    private volatile GeCypherQueryContext queryContext;
    private volatile boolean loaded;

    public GeEdgeWrappingValue(Edge edge) {
        this(edge, null);
    }

    public GeEdgeWrappingValue(String id, GeCypherQueryContext queryContext) {
        super(id);
        Preconditions.checkNotNull(queryContext);
        this.queryContext = queryContext;
    }

    public GeEdgeWrappingValue(Edge edge, GeCypherQueryContext queryContext) {
        super(edge.getId());
        Preconditions.checkNotNull(queryContext);
        this.edge = edge;
        this.queryContext = queryContext;
        this.loaded = true;
    }

    public Edge getEdge() {
        return edge;
    }

    @Override
    public NodeValue startNode() {
        NodeValue start = startNode;
        if (start == null) {
            synchronized (this) {
                start = startNode;
                if (start == null) {
                    String startVid = edge.getVertexId(Direction.OUT);
                    start = startNode = new GeVertexWrappingNodeValue(startVid, queryContext.getGraph(), queryContext.getAuthorizations());
                }
            }
        }
        return start;
    }

    @Override
    public NodeValue endNode() {
        NodeValue end = endNode;
        if (end == null) {
            synchronized (this) {
                end = endNode;
                if (end == null) {
                    String endVid = edge.getVertexId(Direction.IN);
                    end = endNode = new GeVertexWrappingNodeValue(endVid, queryContext.getGraph(), queryContext.getAuthorizations());
                }
            }
        }
        return end;
    }

    @Override
    public TextValue type() {
        TextValue t = type;
        if (t == null) {
            synchronized (this) {
                t = type;
                if (t == null) {
                    t = type = Values.stringValue(edge.getLabel());
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
                    edge.getProperties().forEach(p -> builder.add(p.getName(), p.getValue()));
                    m = properties = builder.build();
                }
            }
        }
        return m;
    }

    public Map<String, Object> getAllProperties() {
        Map<String, Object> allProps = new HashMap<>();
        properties().foreach((k, v) -> {
            if (v instanceof Value)
                allProps.put(k, ((Value)v).asObjectCopy());
            else
                allProps.put(k, v);
        });
        return allProps;
    }
    @Override
    public void setGraphElement(Element element) {
        this.edge = (Edge) element;
        this.loaded = true;
    }

    @Override
    public ElementType getType() {
        return ElementType.EDGE;
    }

    @Override
    public boolean isLoaded() {
        return loaded;
    }

    @Override
    public boolean equals(Object other) {
        return this == other ||
                (other instanceof GeEdgeWrappingValue &&
                        ((GeEdgeWrappingValue) other).id().equals(id())
                );
    }
}
