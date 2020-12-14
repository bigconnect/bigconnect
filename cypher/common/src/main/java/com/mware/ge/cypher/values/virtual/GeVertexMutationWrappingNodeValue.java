package com.mware.ge.cypher.values.virtual;

import com.mware.ge.ElementType;
import com.mware.ge.cypher.ge.GeCypherQueryContext;
import com.mware.ge.values.storable.TextArray;
import com.mware.ge.values.storable.Values;
import com.mware.ge.values.virtual.MapValue;
import com.mware.ge.values.virtual.MapValueBuilder;
import com.mware.ge.values.virtual.NodeValue;
import com.mware.ge.mutation.VertexMutation;

public class GeVertexMutationWrappingNodeValue extends NodeValue implements GeVirtualNodeValue {
    private volatile VertexMutation vertex;
    private GeCypherQueryContext queryContext;
    private volatile TextArray labels;
    private volatile MapValue properties;

    public GeVertexMutationWrappingNodeValue(VertexMutation vertex, GeCypherQueryContext queryContext) {
        super(vertex.getId());
        this.vertex = vertex;
        this.queryContext = queryContext;
    }

    @Override
    public TextArray labels() {
        TextArray l = labels;
        if (l == null) {
            synchronized (this) {
                l = labels;
                if (l == null) {
                    l = labels = Values.stringArray(getConceptType());
                }
            }
        }
        return l;
    }

    private String getConceptType() {
        return vertex.hasChanges() ? vertex.getNewConceptType() : vertex.getConceptType();
    }

    @Override
    public MapValue properties() {
        MapValue m = properties;
        if (m == null) {
            synchronized (this) {
                m = properties;
                if (m == null) {
                    MapValueBuilder builder = new MapValueBuilder();
                    queryContext.getElementProperties(vertex.getId(), ElementType.EDGE)
                            .forEach(builder::add);
                    m = properties = builder.build();
                }
            }
        }
        return m;
    }
}
