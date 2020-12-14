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
package com.mware.ge.util;

import com.mware.ge.GeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;

import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;

public class StringUtil {
    public static boolean containsOnWordBoundaryCaseInsensitive(String str, String contains) {
        Pattern regex = Pattern.compile(".*\\b" + Pattern.quote(contains) + "\\b.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        return regex.matcher(str).matches();
    }

    public static String toCamelCase(String text) {
        text = text.trim().replaceAll("\\s+", "_").toLowerCase();
        return StringUtils.uncapitalize(StringUtils.remove(WordUtils.capitalizeFully(text, '_'), "_"));
    }

    public static String removeAll(final String text, final String regex) {
        return replaceAll(text, regex, StringUtils.EMPTY);
    }

    public static String replaceAll(final String text, final String regex, final String replacement) {
        if (text == null || regex == null|| replacement == null ) {
            return text;
        }
        return text.replaceAll(regex, replacement);
    }

    public static byte[] encode(String value) {
        try {
            return value.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new GeException("Failed to encode string", e);
        }
    }

    public static String decode(byte[] bytes) {
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new GeException("Failed to decode string", e);
        }
    }
}
