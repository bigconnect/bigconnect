package com.mware.ge.query.builder;

import com.mware.ge.Authorizations;
import com.mware.ge.GeObject;
import com.mware.ge.Property;

public class ExistsQueryBuilder extends GeQueryBuilder {
    private final String field;

    protected ExistsQueryBuilder(String field) {
        this.field = field;
    }

    public String getField() {
        return field;
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations authorizations) {
        for (Property prop : geObject.getProperties()) {
            if (this.field.equals(prop.getName())) {
                return true;
            }
        }
        return false;
    }
}
