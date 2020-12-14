package com.mware.ge.cypher.values.virtual;

import com.mware.ge.cypher.Path;
import com.mware.ge.values.virtual.NodeValue;
import com.mware.ge.values.virtual.PathValue;
import com.mware.ge.values.virtual.RelationshipValue;

import java.util.ArrayList;
import java.util.List;

public class GeWrappingPathValue extends PathValue {
    private Path inner;

    public GeWrappingPathValue(Path path) {
        this.inner = path;
    }

    @Override
    public NodeValue startNode() {
        return inner.startNode();
    }

    @Override
    public NodeValue endNode() {
        return inner.endNode();
    }

    @Override
    public RelationshipValue lastRelationship() {
        return inner.lastRelationship();
    }

    public NodeValue[] nodes() {
        List<NodeValue> r = new ArrayList<>();
        for (NodeValue nv : inner.nodes())
            r.add(nv);
        return r.toArray(new NodeValue[0]);
    }

    public RelationshipValue[] relationships() {
        List<RelationshipValue> r = new ArrayList<>();
        for (RelationshipValue nv : inner.relationships())
            r.add(nv);
        return r.toArray(new RelationshipValue[0]);
    }
}
