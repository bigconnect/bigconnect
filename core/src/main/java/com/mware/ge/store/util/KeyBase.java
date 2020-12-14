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
package com.mware.ge.store.util;

import com.mware.ge.GeException;

import java.util.ArrayList;
import java.util.List;

public abstract class KeyBase {
    public static final char VALUE_SEPARATOR = '\u001f';

    public static void assertNoValueSeparator(String str) {
        if (str.indexOf(VALUE_SEPARATOR) >= 0) {
            throw new GeException("String cannot contain '" + VALUE_SEPARATOR + "' (0x1f): " + str);
        }
    }

    public static String[] splitOnValueSeparator(String s) {
        List<String> results = new ArrayList<>();
        int last = 0;
        int i = s.indexOf(VALUE_SEPARATOR);
        while (true) {
            if (i > 0) {
                results.add(s.substring(last, i));
                last = i + 1;
                i = s.indexOf(VALUE_SEPARATOR, last);
            } else {
                results.add(s.substring(last));
                break;
            }
        }
        return results.toArray(new String[results.size()]);
    }
}
