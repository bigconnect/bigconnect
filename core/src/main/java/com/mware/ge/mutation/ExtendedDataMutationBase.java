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
package com.mware.ge.mutation;


import com.google.common.collect.Ordering;
import com.mware.ge.Visibility;

public class ExtendedDataMutationBase<T extends ExtendedDataMutationBase> implements Comparable<T> {
    private final String tableName;
    private final String row;
    private final String columnName;
    private final String key;
    private final Visibility visibility;

    public ExtendedDataMutationBase(String tableName, String row, String columnName, String key, Visibility visibility) {
        this.tableName = tableName;
        this.row = row;
        this.columnName = columnName;
        this.key = key;
        this.visibility = visibility;
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

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "tableName='" + tableName + '\'' +
                ", row='" + row + '\'' +
                ", columnName='" + columnName + '\'' +
                ", key='" + key + '\'' +
                ", visibility=" + visibility +
                '}';
    }

    @Override
    public int compareTo(T other) {
        int i = tableName.compareTo(other.getTableName());
        if (i != 0) {
            return i;
        }

        i = row.compareTo(other.getRow());
        if (i != 0) {
            return i;
        }

        i = columnName.compareTo(other.getColumnName());
        if (i != 0) {
            return i;
        }

        i = Ordering.natural().nullsFirst().compare(key, other.getKey());
        if (i != 0) {
            return i;
        }

        return 0;
    }
}