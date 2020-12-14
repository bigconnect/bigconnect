package com.mware.ge.cypher.values.virtual;

import com.mware.ge.*;
import com.mware.ge.cypher.ge.GeCypherQueryContext;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.TextArray;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import com.mware.ge.values.virtual.MapValue;
import com.mware.ge.values.virtual.MapValueBuilder;
import com.mware.ge.values.virtual.NodeValue;

import java.util.HashMap;
import java.util.Map;

public class GeVertexWrappingNodeValue extends NodeValue implements GraphLoadableValue, GeVirtualNodeValue {
    private volatile Vertex vertex;
    private volatile Graph graph;
    private volatile Authorizations authorizations;
    private volatile TextArray labels;
    private volatile MapValue properties;
    private volatile boolean loaded;

    public GeVertexWrappingNodeValue(Vertex vertex) {
        super(vertex.getId());
        this.vertex = vertex;
        this.graph = vertex.getGraph();
        this.authorizations = this.vertex.getAuthorizations();
        this.labels = Values.stringArray(vertex.getConceptType());
        this.properties = properties();
        this.loaded = true;
    }

    public GeVertexWrappingNodeValue(String id, Authorizations authorizations, Graph graph) {
        super(id);
        this.authorizations = authorizations;
        this.graph = graph;
        this.loaded = false;
    }

    public GeVertexWrappingNodeValue(String id, String label, GeCypherQueryContext queryContext) {
        super(id);
        this.labels = Values.stringArray(label);
        Preconditions.checkNotNull(queryContext);
        this.graph = queryContext.getGraph();
        this.authorizations = queryContext.getAuthorizations();
    }

    public GeVertexWrappingNodeValue(String id, Graph graph, Authorizations authorizations) {
        super(id);
        Preconditions.checkNotNull(graph);
        Preconditions.checkNotNull(authorizations);
        this.graph = graph;
        this.authorizations = authorizations;
    }

    public Vertex getVertex() {
        return getOrLoadVertex(FetchHints.ALL);
    }

    @Override
    public TextArray labels() {
        TextArray l = labels;
        if ( l == null ) {
            synchronized ( this ) {
                l = labels;
                if ( l == null ) {
                    l = labels = Values.stringArray(getOrLoadVertex(FetchHints.PROPERTIES).getConceptType());
                }
            }
        }
        return l;
    }

    @Override
    public MapValue properties() {
        MapValue m = properties;
        if ( m == null ) {
            synchronized ( this ) {
                m = properties;
                if ( m == null ) {
                    MapValueBuilder builder = new MapValueBuilder();

                    getOrLoadVertex(FetchHints.PROPERTIES).getProperties().forEach(p -> {
                        builder.add(p.getName(), p.getValue());
                    });


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

    private Vertex getOrLoadVertex(FetchHints fetchHints) {
        if(vertex == null) {
            vertex = graph.getVertex(id(), fetchHints, authorizations);
            this.loaded = true;
        }

        return vertex;
    }

    @Override
    public void setGraphElement(Element element) {
        this.vertex = (Vertex) element;
        this.loaded = true;
    }

    @Override
    public ElementType getType() {
        return ElementType.VERTEX;
    }

    @Override
    public boolean isLoaded() {
        return loaded;
    }

    @Override
    public boolean equals( Object other ) {
        return this == other ||
                (other instanceof GeVertexWrappingNodeValue &&
                        ((GeVertexWrappingNodeValue) other).id().equals(id())
                );
    }

    @Override
    public String toString() {
        return id()+": "+labels();
    }
}
