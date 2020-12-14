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
package com.mware.ge.csv;

/**
 * Configuration options around reading CSV data, or similar.
 */
public interface Configuration {
    /**
     * TODO: Our intention is to flip this to false (which means to comply with RFC4180) at some point
     * because of how it better complies with common expectancy of behavior. It may be least disruptive
     * to do this when changing major version of the product.
     */
    boolean DEFAULT_LEGACY_STYLE_QUOTING = true;

    /**
     * Character to regard as quotes. Quoted values can contain newline characters and even delimiters.
     */
    char quotationCharacter();

    /**
     * Data buffer size.
     */
    int bufferSize();

    /**
     * Whether or not fields are allowed to have newline characters in them, i.e. span multiple lines.
     */
    boolean multilineFields();

    /**
     * Whether or not strings should be trimmed for whitespaces.
     */
    boolean trimStrings();

    /**
     * @return {@code true} for treating empty strings, i.e. {@code ""} as null, instead of an empty string.
     */
    boolean emptyQuotedStringsAsNull();

    /**
     * Adds a default implementation returning {@link #DEFAULT_LEGACY_STYLE_QUOTING}, this to not requiring
     * any change to other classes using this interface.
     *
     * @return whether or not the parsing will interpret <code>\"</code> (see {@link #quotationCharacter()})
     * as an inner quote. Reason why this is configurable is that this interpretation conflicts with
     * "standard" RFC for CSV parsing, see https://tools.ietf.org/html/rfc4180. This also makes it impossible
     * to enter some combinations of characters, e.g. <code>"""abc\"""</code>, when expecting <code>"abc\"</code>.
     */
    default boolean legacyStyleQuoting() {
        return DEFAULT_LEGACY_STYLE_QUOTING;
    }

    int KB = 1024;
    int MB = KB * KB;
    int DEFAULT_BUFFER_SIZE_4MB = 4 * MB;

    class Default implements Configuration {
        @Override
        public char quotationCharacter() {
            return '"';
        }

        @Override
        public int bufferSize() {
            return DEFAULT_BUFFER_SIZE_4MB;
        }

        @Override
        public boolean multilineFields() {
            return false;
        }

        @Override
        public boolean emptyQuotedStringsAsNull() {
            return false;
        }

        @Override
        public boolean trimStrings() {
            return false;
        }
    }

    Configuration DEFAULT = new Default();

    class Overridden implements Configuration {
        private final Configuration defaults;

        public Overridden(Configuration defaults) {
            this.defaults = defaults;
        }

        @Override
        public char quotationCharacter() {
            return defaults.quotationCharacter();
        }

        @Override
        public int bufferSize() {
            return defaults.bufferSize();
        }

        @Override
        public boolean multilineFields() {
            return defaults.multilineFields();
        }

        @Override
        public boolean emptyQuotedStringsAsNull() {
            return defaults.emptyQuotedStringsAsNull();
        }

        @Override
        public boolean trimStrings() {
            return defaults.trimStrings();
        }

        @Override
        public boolean legacyStyleQuoting() {
            return defaults.legacyStyleQuoting();
        }
    }
}
