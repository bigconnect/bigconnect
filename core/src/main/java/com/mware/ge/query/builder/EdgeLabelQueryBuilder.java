package com.mware.ge.query.builder;

import com.mware.ge.Authorizations;
import com.mware.ge.Edge;
import com.mware.ge.GeObject;
import com.mware.ge.Vertex;
import com.mware.ge.helpers.ArrayUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class EdgeLabelQueryBuilder extends GeQueryBuilder {
    private final String[] edgeLabels;

    protected EdgeLabelQueryBuilder(String ...edgeLabels) {
        this.edgeLabels = edgeLabels;
    }

    public String[] getEdgeLabels() {
        return edgeLabels;
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations authorizations) {
        if (geObject instanceof Edge) {
            return ArrayUtil.contains(edgeLabels, ((Edge) geObject).getLabel());
        }
        if (geObject instanceof Vertex) {
            Set<String> edgeLabels = ((Vertex) geObject).getEdgesSummary(authorizations).getEdgeLabels();
            return CollectionUtils.intersection(edgeLabels, List.of(this.edgeLabels)).size() > 0;
        }
        return false;
    }

    @Override
    public GeQueryBuilder clone() {
        return new EdgeLabelQueryBuilder(edgeLabels);
    }
}
