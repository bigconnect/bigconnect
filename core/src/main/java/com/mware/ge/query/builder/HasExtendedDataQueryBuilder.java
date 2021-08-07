package com.mware.ge.query.builder;

import com.mware.ge.*;

public class HasExtendedDataQueryBuilder extends GeQueryBuilder {
    private final ElementType elementType;
    private final String elementId;
    private final String tableName;

    protected HasExtendedDataQueryBuilder(ElementType elementType, String elementId, String tableName) {
        this.elementType = elementType;
        this.elementId = elementId;
        this.tableName = tableName;
    }

    public ElementType getElementType() {
        return elementType;
    }

    public String getElementId() {
        return elementId;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations authorizations) {
        if (!(geObject instanceof ExtendedDataRow)) {
            return false;
        }

        ExtendedDataRow row = (ExtendedDataRow) geObject;
        ExtendedDataRowId rowId = row.getId();
        if (elementType == null || rowId.getElementType().equals(elementType)
                && (elementId == null || rowId.getElementId().equals(elementId))
                && (tableName == null || rowId.getTableName().equals(tableName))) {
            return true;
        }
        return false;
    }
}
