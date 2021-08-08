package com.mware.ge.query.builder;

import com.mware.ge.Authorizations;
import com.mware.ge.GeObject;

public class SearchAllQueryBuilder extends GeQueryBuilder {
    protected SearchAllQueryBuilder() {
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations authorizations) {
        return true;
    }

    @Override
    public GeQueryBuilder clone() {
        return new SearchAllQueryBuilder();
    }
}
