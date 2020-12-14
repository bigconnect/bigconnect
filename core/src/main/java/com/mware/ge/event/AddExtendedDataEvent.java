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
package com.mware.ge.event;

import com.mware.ge.Element;
import com.mware.ge.Graph;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.Value;

import java.util.Objects;

public class AddExtendedDataEvent extends GraphEvent {
    private final Element element;
    private final String tableName;
    private final String row;
    private final String columnName;
    private final String key;
    private final Value value;
    private final Visibility visibility;

    public AddExtendedDataEvent(
            Graph graph,
            Element element,
            String tableName,
            String row,
            String columnName,
            String key,
            Value value,
            Visibility visibility
    ) {
        super(graph);
        this.element = element;
        this.tableName = tableName;
        this.row = row;
        this.columnName = columnName;
        this.key = key;
        this.value = value;
        this.visibility = visibility;
    }

    public Element getElement() {
        return element;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRow() {
        return row;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return "AddExtendedDataEvent{" +
                "element=" + element +
                ", tableName='" + tableName + '\'' +
                ", row='" + row + '\'' +
                ", columnName='" + columnName + '\'' +
                ", key='" + key + '\'' +
                ", value=" + value +
                ", visibility=" + visibility +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AddExtendedDataEvent that = (AddExtendedDataEvent) o;
        return Objects.equals(element, that.element) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(row, that.row) &&
                Objects.equals(columnName, that.columnName) &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value) &&
                Objects.equals(visibility, that.visibility);
    }

    @Override
    public int hashCode() {
        return Objects.hash(element, tableName, row, columnName, key, value, visibility);
    }
}
