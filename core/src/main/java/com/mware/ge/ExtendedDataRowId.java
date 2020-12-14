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
package com.mware.ge;

import java.io.Serializable;

public class ExtendedDataRowId implements Serializable, Comparable<ExtendedDataRowId>, GeObjectId {
    private static final long serialVersionUID = 6419674145598605844L;
    private final ElementType elementType;
    private final String elementId;
    private final String tableName;
    private final String rowId;

    public ExtendedDataRowId(ElementType elementType, String elementId, String tableName, String rowId) {
        this.elementType = elementType;
        this.elementId = elementId;
        this.tableName = tableName;
        this.rowId = rowId;
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

    public String getRowId() {
        return rowId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ExtendedDataRowId that = (ExtendedDataRowId) o;

        if (elementType != that.elementType) {
            return false;
        }
        if (!elementId.equals(that.elementId)) {
            return false;
        }
        if (!tableName.equals(that.tableName)) {
            return false;
        }
        return rowId.equals(that.rowId);
    }

    @Override
    public int hashCode() {
        int result = elementType.hashCode();
        result = 31 * result + elementId.hashCode();
        result = 31 * result + tableName.hashCode();
        result = 31 * result + rowId.hashCode();
        return result;
    }

    @Override
    public int compareTo(ExtendedDataRowId other) {
        int i = this.getElementType().compareTo(other.getElementType());
        if (i != 0) {
            return i;
        }

        i = this.getElementId().compareTo(other.getElementId());
        if (i != 0) {
            return i;
        }

        i = this.getTableName().compareTo(other.getTableName());
        if (i != 0) {
            return i;
        }

        i = this.getRowId().compareTo(other.getRowId());
        if (i != 0) {
            return i;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "ExtendedDataRowId{" +
                "elementType=" + elementType +
                ", elementId='" + elementId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", rowId='" + rowId + '\'' +
                '}';
    }
}
