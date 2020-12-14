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

import java.util.*;

public enum TextIndexHint {
    /**
     * Tokenize this property and index it for full text search.
     */
    FULL_TEXT((byte) 0x01),
    /**
     * Index this property for exact match searches.
     */
    EXACT_MATCH((byte) 0x02);

    private final byte value;

    TextIndexHint(byte value) {
        this.value = value;
    }

    public static byte toBits(TextIndexHint... indexHints) {
        return toBits(EnumSet.copyOf(Arrays.asList(indexHints)));
    }

    public static byte toBits(Collection<TextIndexHint> hints) {
        byte b = 0;
        for (TextIndexHint hint : hints) {
            b |= hint.value;
        }
        return b;
    }

    public static Set<TextIndexHint> toSet(byte indexHint) {
        Set<TextIndexHint> hints = new HashSet<TextIndexHint>();
        if ((indexHint & FULL_TEXT.value) == FULL_TEXT.value) {
            hints.add(FULL_TEXT);
        }
        if ((indexHint & EXACT_MATCH.value) == EXACT_MATCH.value) {
            hints.add(EXACT_MATCH);
        }
        return hints;
    }

    public static Set<TextIndexHint> parse(String str) {
        if (str == null || str.equalsIgnoreCase("NONE")) {
            return NONE;
        }

        if (str.equalsIgnoreCase("ALL")) {
            return ALL;
        }

        String[] textIndexHintParts = str.split(",");
        Set<TextIndexHint> textIndexHints = new HashSet<TextIndexHint>();
        for (String textIndexHintPart : textIndexHintParts) {
            if (textIndexHintPart.trim().length() == 0) {
                continue;
            }
            textIndexHints.add(parsePart(textIndexHintPart));
        }
        return textIndexHints;
    }

    private static TextIndexHint parsePart(String part) {
        part = part.trim();
        for (TextIndexHint textIndexHint : TextIndexHint.values()) {
            if (textIndexHint.name().equalsIgnoreCase(part)) {
                return textIndexHint;
            }
        }
        return TextIndexHint.valueOf(part);
    }

    /**
     * Use this to prevent indexing of this Text property.  The property
     * will not be searchable.
     */
    public static final Set<TextIndexHint> NONE = EnumSet.noneOf(TextIndexHint.class);

    /**
     * The set of indexing hints that trigger all available indexes for
     * a Text property.
     */
    public static final Set<TextIndexHint> ALL = EnumSet.of(FULL_TEXT, EXACT_MATCH);
}
