package com.mware.ge.query.builder;

import com.mware.ge.Authorizations;
import com.mware.ge.GeObject;
import com.mware.ge.query.Predicate;
import com.mware.ge.values.storable.Value;

import java.util.Collections;

public class PropertyQueryBuilder extends GeQueryBuilder {
    private final String propertyName;
    private final Predicate predicate;
    private final Value value;

    protected PropertyQueryBuilder(String propertyName, Predicate predicate, Value value) {
        this.propertyName = propertyName;
        this.predicate = predicate;
        this.value = value;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public Value getValue() {
        return value;
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations authorizations) {
        return this.predicate.evaluate(geObject.getProperties(propertyName), value);
    }

    @Override
    public GeQueryBuilder clone() {
        return new PropertyQueryBuilder(propertyName, predicate, value);
    }
}
