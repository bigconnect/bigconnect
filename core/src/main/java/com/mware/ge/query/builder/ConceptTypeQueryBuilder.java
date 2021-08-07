package com.mware.ge.query.builder;

import com.mware.ge.Authorizations;
import com.mware.ge.GeObject;
import com.mware.ge.Vertex;
import com.mware.ge.helpers.ArrayUtil;

public class ConceptTypeQueryBuilder extends GeQueryBuilder {
    private final String[] conceptTypes;

    protected ConceptTypeQueryBuilder(String... conceptTypes) {
        this.conceptTypes = conceptTypes;
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations authorizations) {
        if (geObject instanceof Vertex) {
            return ArrayUtil.contains(conceptTypes, ((Vertex) geObject).getConceptType());
        }
        return false;
    }
}
