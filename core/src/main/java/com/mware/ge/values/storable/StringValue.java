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
package com.mware.ge.values.storable;

import com.mware.ge.hashing.HashFunction;
import com.mware.ge.values.AnyValue;
import com.mware.ge.values.ValueMapper;
import com.mware.ge.values.virtual.ListValue;
import com.mware.ge.values.virtual.VirtualValues;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

public abstract class StringValue extends TextValue {
    abstract String value();

    @Override
    public boolean equals(Value value) {
        return value.equals(value());
    }

    @Override
    public boolean equals(char x) {
        return value().length() == 1 && value().charAt(0) == x;
    }

    @Override
    public boolean equals(String x) {
        return value().equals(x);
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writer.writeString(value());
    }

    @Override
    public TextValue toLower() {
        return new StringWrappingStringValue(value().toLowerCase());
    }

    @Override
    public TextValue toUpper() {
        return new StringWrappingStringValue(value().toUpperCase());
    }

    @Override
    public ListValue split(String separator) {
        assert separator != null;
        String asString = value();
        //Cypher has different semantics for the case where the separator
        //is exactly the value, in cypher we expect two empty arrays
        //where as java returns an empty array
        if (separator.equals(asString)) {
            return EMPTY_SPLIT;
        } else if (separator.isEmpty()) {
            return VirtualValues.fromArray(Values.charArray(asString.toCharArray()));
        }

        List<AnyValue> split = splitNonRegex(asString, separator);
        return VirtualValues.fromList(split);
    }

    /**
     * Splits a string.
     *
     * @param input String to be split
     * @param delim delimiter, must not be not empty
     * @return the split string as a List of TextValues
     */
    private static List<AnyValue> splitNonRegex(String input, String delim) {
        List<AnyValue> l = new ArrayList<>();
        int offset = 0;

        while (true) {
            int index = input.indexOf(delim, offset);
            if (index == -1) {
                String substring = input.substring(offset);
                l.add(Values.stringValue(substring));
                return l;
            } else {
                String substring = input.substring(offset, index);
                l.add(Values.stringValue(substring));
                offset = index + delim.length();
            }
        }
    }

    @Override
    public TextValue replace(String find, String replace) {
        assert find != null;
        assert replace != null;

        return Values.stringValue(value().replace(find, replace));
    }

    @Override
    public Object asObjectCopy() {
        return value();
    }

    @Override
    public String toString() {
        return format("%s(\"%s\")", getTypeName(), value());
    }

    @Override
    public String getTypeName() {
        return "String";
    }

    @Override
    public String stringValue() {
        return value();
    }

    @Override
    public String prettyPrint() {
        return format("'%s'", value());
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapString(this);
    }

    //NOTE: this doesn't respect code point order for code points that doesn't fit 16bits
    @Override
    public int compareTo(TextValue other) {
        String thisString = value();
        String thatString = other.stringValue();
        return thisString.compareTo(thatString);
    }

    static StringValue EMPTY = new StringValue() {
        @Override
        protected int computeHash() {
            return 0;
        }

        @Override
        public long updateHash(HashFunction hashFunction, long hash) {
            return hashFunction.update(hash, 0); // Mix in our length; a single zero.
        }

        @Override
        public int length() {
            return 0;
        }

        @Override
        public TextValue substring(int start, int end) {
            return this;
        }

        @Override
        public TextValue trim() {
            return this;
        }

        @Override
        public TextValue ltrim() {
            return this;
        }

        @Override
        public TextValue rtrim() {
            return this;
        }

        @Override
        public TextValue reverse() {
            return this;
        }

        @Override
        public TextValue plus(TextValue other) {
            return other;
        }

        @Override
        public boolean startsWith(TextValue other) {
            return other.length() == 0;
        }

        @Override
        public boolean endsWith(TextValue other) {
            return other.length() == 0;
        }

        @Override
        public boolean contains(TextValue other) {
            return other.length() == 0;
        }

        @Override
        public TextValue toLower() {
            return this;
        }

        @Override
        public TextValue toUpper() {
            return this;
        }

        @Override
        public TextValue replace(String find, String replace) {
            if (find.isEmpty()) {
                return Values.stringValue(replace);
            } else {
                return this;
            }
        }

        @Override
        public int compareTo(TextValue other) {
            return -other.length();
        }

        @Override
        Matcher matcher(Pattern pattern) {
            return pattern.matcher("");
        }

        @Override
        String value() {
            return "";
        }
    };
}

