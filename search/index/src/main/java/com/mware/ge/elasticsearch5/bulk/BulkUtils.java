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
package com.mware.ge.elasticsearch5.bulk;

import com.mware.ge.ElementId;
import com.mware.ge.ExtendedDataRowId;
import com.mware.ge.GeException;
import com.mware.ge.GeObjectId;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class BulkUtils {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(BulkUtils.class);

    public static int calculateSizeOfId(GeObjectId geObjectId) {
        if (geObjectId instanceof ElementId) {
            return ((ElementId) geObjectId).getId().length();
        } else if (geObjectId instanceof ExtendedDataRowId) {
            ExtendedDataRowId extendedDataRowId = (ExtendedDataRowId) geObjectId;
            return extendedDataRowId.getElementId().length()
                    + extendedDataRowId.getTableName().length()
                    + extendedDataRowId.getRowId().length();
        } else {
            throw new GeException("Unhandled GeObjectId: " + geObjectId.getClass().getName());
        }
    }

    public static int calculateSizeOfMap(Map<?, ?> map) {
        int size = 0;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            size += calculateSizeOfValue(entry.getKey()) + calculateSizeOfValue(entry.getValue());
        }
        return size;
    }

    public static int calculateSizeOfCollection(Collection<?> list) {
        int size = 0;
        for (Object o : list) {
            size += calculateSizeOfValue(o);
        }
        return size;
    }

    public static int calculateSizeOfValue(Object value) {
        if (value instanceof String) {
            return ((String) value).length();
        } else if (value instanceof Boolean) {
            return 4;
        } else if (value instanceof Number || value instanceof Date) {
            return 8;
        } else if (value instanceof Collection) {
            return calculateSizeOfCollection((Collection<?>) value);
        } else if (value instanceof Map) {
            return calculateSizeOfMap((Map<?, ?>) value);
        } else {
            LOGGER.warn("unhandled object to calculate size for: " + value.getClass().getName() + ", defaulting to 100");
            return 100;
        }
    }
}
