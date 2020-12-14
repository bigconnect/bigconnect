package com.mware.ge.cypher.values.virtual;

import com.mware.ge.Authorizations;
import com.mware.ge.Edge;
import com.mware.ge.Graph;
import com.mware.ge.Vertex;
import com.mware.ge.values.virtual.NodeValue;
import com.mware.ge.values.virtual.PathValue;
import com.mware.ge.values.virtual.RelationshipValue;

import java.util.ArrayList;
import java.util.List;

public class GeWrappingPath {
    private Graph graph;
    private PathValue inner;
    private Authorizations authorizations;

    public GeWrappingPath(Graph graph, PathValue pathValue, Authorizations authorizations) {
        this.graph = graph;
        this.inner = pathValue;
        this.authorizations = authorizations;
    }

    public Vertex startNode() {
        return toGeNodeValue(inner.startNode());
    }

    public Vertex endNode() {
        return toGeNodeValue(inner.endNode());
    }

    public Edge lastRelationship() {
        return toGeRelValue(inner.lastRelationship());
    }

    public Vertex[] nodes() {
        List<Vertex> r = new ArrayList<>();
        for(NodeValue nv : inner.nodes())
            r.add(toGeNodeValue(nv));
        return r.toArray(new Vertex[0]);
    }

    public Edge[] relationships() {
        List<Edge> r = new ArrayList<>();
        for(RelationshipValue nv : inner.relationships())
            r.add(toGeRelValue(nv));
        return r.toArray(new Edge[0]);
    }

    Vertex toGeNodeValue(NodeValue nodeValue) {
        return graph.getVertex(String.valueOf(nodeValue.id()), authorizations);
    }

    Edge toGeRelValue(RelationshipValue relValue) {
        return graph.getEdge(String.valueOf(relValue.id()), authorizations);
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }
}
