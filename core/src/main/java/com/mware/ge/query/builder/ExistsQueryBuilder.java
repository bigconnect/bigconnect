package com.mware.ge.query.builder;

import com.mware.ge.Authorizations;
import com.mware.ge.GeObject;
import com.mware.ge.Property;

public class ExistsQueryBuilder extends GeQueryBuilder {
    private final String propertyName;

    protected ExistsQueryBuilder(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations authorizations) {
        for (Property prop : geObject.getProperties()) {
            if (this.propertyName.equals(prop.getName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public GeQueryBuilder clone() {
        return new ExistsQueryBuilder(propertyName);
    }
}
