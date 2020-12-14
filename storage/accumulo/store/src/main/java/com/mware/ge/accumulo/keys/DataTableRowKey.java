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
package com.mware.ge.accumulo.keys;

import com.mware.ge.Property;
import com.mware.ge.accumulo.iterator.model.KeyBase;
import org.apache.commons.lang.StringUtils;

public class DataTableRowKey extends KeyBase {
    private static final int LEGACY_VALUE_SEPARATOR_COUNT = 3;
    private static final int PARTS_INDEX_ELEMENT_ROW_KEY = 0;
    private static final int PARTS_INDEX_PROPERTY_NAME = 1;
    private static final int PARTS_INDEX_PROPERTY_KEY = 2;
    private final String[] parts;

    public DataTableRowKey(String elementRowKey, Property property) {
        this(elementRowKey, property.getKey(), property.getName());
    }

    public DataTableRowKey(String elementRowKey, String propertyKey, String propertyName) {
        this.parts = new String[]{
                elementRowKey,
                propertyName,
                propertyKey
        };
    }

    public String getRowKey() {
        assertNoValueSeparator(getElementRowKey());
        assertNoValueSeparator(getPropertyName());
        assertNoValueSeparator(getPropertyKey());
        return getElementRowKey()
                + VALUE_SEPARATOR + getPropertyName()
                + VALUE_SEPARATOR + getPropertyKey();
    }

    public String getElementRowKey() {
        return parts[PARTS_INDEX_ELEMENT_ROW_KEY];
    }

    public String getPropertyName() {
        return parts[PARTS_INDEX_PROPERTY_NAME];
    }

    public String getPropertyKey() {
        return parts[PARTS_INDEX_PROPERTY_KEY];
    }

    public static boolean isLegacy(String dataRowKey) {
        if (StringUtils.countMatches(dataRowKey, "" + VALUE_SEPARATOR) == LEGACY_VALUE_SEPARATOR_COUNT) {
            return true;
        }
        return false;
    }
}
