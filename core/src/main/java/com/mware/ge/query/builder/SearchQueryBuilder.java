package com.mware.ge.query.builder;

import com.mware.ge.*;
import com.mware.ge.values.storable.StreamingPropertyValue;

public class SearchQueryBuilder extends GeQueryBuilder {
    private final String queryString;

    public SearchQueryBuilder(String query) {
        this.queryString = query;
    }

    public String getQuery() {
        return queryString;
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations authorizations) {
        if (geObject instanceof Element || geObject instanceof ExtendedDataRow) {
            for (Property property : geObject.getProperties()) {
                if (evaluateQueryStringOnValue(property.getValue())) {
                    return true;
                }
            }
            return false;
        } else {
            throw new GeException("Unhandled GeObject type: " + geObject.getClass().getName());
        }
    }

    private boolean evaluateQueryStringOnValue(Object value) {
        if (value == null) {
            return false;
        }
        if (queryString.equals("*")) {
            return true;
        }
        if (value instanceof StreamingPropertyValue) {
            value = ((StreamingPropertyValue) value).readToString();
        }
        String valueString = value.toString().toLowerCase();
        return valueString.contains(queryString.toLowerCase());
    }
}
