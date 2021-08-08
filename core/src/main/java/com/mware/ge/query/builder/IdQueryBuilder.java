package com.mware.ge.query.builder;

import com.mware.ge.*;
import com.mware.ge.helpers.ArrayUtil;

public class IdQueryBuilder extends GeQueryBuilder {
    private final String[] ids;

    protected IdQueryBuilder(String ...ids) {
        this.ids = ids;
    }

    public String[] getIds() {
        return ids;
    }

    @Override
    public boolean matches(GeObject geElem, Authorizations authorizations) {
        if (geElem instanceof Element) {
            return ArrayUtil.contains(ids, ((Element)geElem).getId());
        } else if (geElem instanceof ExtendedDataRow) {
            return ArrayUtil.contains(ids, ((ExtendedDataRow)geElem).getId().getElementId());
        } else {
            throw new GeException("Unhandled element type: " + geElem.getClass().getName());
        }
    }

    @Override
    public GeQueryBuilder clone() {
        return new IdQueryBuilder(ids);
    }
}
