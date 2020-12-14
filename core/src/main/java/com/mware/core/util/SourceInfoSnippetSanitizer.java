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
package com.mware.core.util;

import org.apache.commons.lang3.StringEscapeUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SourceInfoSnippetSanitizer {
    private static Pattern snippetPattern = Pattern.compile(
            "^(.*?)<span class=\"selection\">(.+?)</span>(.*)$",
            Pattern.DOTALL
    );

    /**
     * Sanitize Snippet HTML parts:
     * 1. Before selection span
     * 2. Inside selection
     * 3. After selection
     *
     * If pattern doesn't match expected escape whole string
     *
     * @param snippet
     * @return sanitized snippet
     */
    public static String sanitizeSnippet(String snippet) {
        if (snippet == null) {
            return null;
        }

        Matcher matcher = snippetPattern.matcher(snippet);
        if (matcher.matches() && matcher.groupCount() == 3) {
            String prefix = matcher.group(1);
            String selection = matcher.group(2);
            String suffix = matcher.group(3);

            return escapeParts(prefix, selection, suffix);
        } else {
            return StringEscapeUtils.escapeXml11(snippet);
        }
    }

    private static String escapeParts(String prefix, String selection, String suffix) {
        StringBuilder str = new StringBuilder();

        str.append(StringEscapeUtils.escapeXml11(prefix));
        str.append("<span class=\"selection\">");
        str.append(StringEscapeUtils.escapeXml11(selection));
        str.append("</span>");
        str.append(StringEscapeUtils.escapeXml11(suffix));

        return str.toString();
    }
}
