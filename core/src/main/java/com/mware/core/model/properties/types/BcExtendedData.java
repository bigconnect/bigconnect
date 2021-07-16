/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
package com.mware.core.model.properties.types;

import com.mware.ge.Element;
import com.mware.ge.ExtendedDataRow;
import com.mware.ge.mutation.ElementMutation;
import com.mware.core.model.graph.ElementUpdateContext;
import com.mware.ge.values.storable.NoValue;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;

import java.text.SimpleDateFormat;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class BcExtendedData<TRaw> {
    private static final String ROW_ID_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private final String tableName;
    private final String columnName;

    protected BcExtendedData(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public abstract Value rawToGraph(TRaw value);

    public abstract TRaw graphToRaw(Value value);

    public <T extends Element> void addExtendedData(
            ElementUpdateContext<T> elemCtx,
            String row,
            TRaw newValue,
            PropertyMetadata propertyMetadata
    ) {
        addExtendedData(elemCtx.getMutation(), row, newValue, propertyMetadata);
    }

    public <T extends Element> void addExtendedData(
            ElementUpdateContext<T> elemCtx,
            String row,
            TRaw newValue,
            PropertyMetadata propertyMetadata,
            Long timestamp
    ) {
        addExtendedData(elemCtx.getMutation(), row, newValue, propertyMetadata, timestamp);
    }

    public <T extends Element> void addExtendedData(
            ElementMutation<T> m,
            String row,
            TRaw newValue,
            PropertyMetadata propertyMetadata
    ) {
        addExtendedData(m, row, newValue, propertyMetadata, null);
    }

    public <T extends Element> void addExtendedData(
            ElementMutation<T> m,
            String row,
            TRaw newValue,
            PropertyMetadata propertyMetadata,
            Long timestamp
    ) {
        checkNotNull(newValue, "null values are not allowed");
        m.addExtendedData(tableName, row, columnName, rawToGraph(newValue), timestamp, propertyMetadata.getPropertyVisibility());
    }

    public static String rowIdFromDate(Date timestamp) {
        return new SimpleDateFormat(ROW_ID_DATE_FORMAT).format(timestamp);
    }

    public TRaw getValue(ExtendedDataRow row) {
        Value value = row.getPropertyValue(columnName);
        if (value == null || value instanceof NoValue)
            return null;
        else
            return graphToRaw(value);
    }
}
