package com.mware.ge.query.builder;

import com.mware.ge.Authorizations;
import com.mware.ge.GeObject;
import com.mware.ge.query.Compare;
import com.mware.ge.query.Predicate;
import com.mware.ge.values.storable.Value;

public class RangeQueryBuilder extends GeQueryBuilder {
    private final String propertyName;
    private final Value startValue;
    private final boolean inclusiveStartValue;
    private final Value endValue;
    private final boolean inclusiveEndValue;

    public RangeQueryBuilder(String propertyName, Value startValue, boolean inclusiveStartValue, Value endValue, boolean inclusiveEndValue) {
        this.propertyName = propertyName;
        this.startValue = startValue;
        this.inclusiveStartValue = inclusiveStartValue;
        this.endValue = endValue;
        this.inclusiveEndValue = inclusiveEndValue;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Value getStartValue() {
        return startValue;
    }

    public boolean isInclusiveStartValue() {
        return inclusiveStartValue;
    }

    public Value getEndValue() {
        return endValue;
    }

    public boolean isInclusiveEndValue() {
        return inclusiveEndValue;
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations authorizations) {
        BoolQueryBuilder boolQuery = GeQueryBuilders.boolQuery();
        if (startValue != null) {
            Predicate predicate = inclusiveStartValue ? Compare.GREATER_THAN_EQUAL : Compare.GREATER_THAN;
            boolQuery.and(GeQueryBuilders.hasFilter(propertyName, predicate, startValue));
        }
        if (endValue != null) {
            Predicate predicate = inclusiveEndValue ? Compare.LESS_THAN_EQUAL : Compare.LESS_THAN;
            boolQuery.and(GeQueryBuilders.hasFilter(propertyName, predicate, endValue));
        }
        return boolQuery.matches(geObject, authorizations);
    }
}
