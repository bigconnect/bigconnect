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

import com.mware.ge.values.ValueMapper;
import com.mware.ge.values.virtual.ListValue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mware.ge.values.storable.Values.stringArray;
import static com.mware.ge.values.virtual.VirtualValues.fromArray;

public abstract class TextValue extends ScalarValue {
    static final ListValue EMPTY_SPLIT = fromArray(stringArray("", ""));

    TextValue() {
    }

    public abstract String stringValue();

    /**
     * The length of a TextValue is the number of Unicode code points in the text.
     *
     * @return The number of Unicode code points.
     */
    public abstract int length();

    public abstract TextValue substring(int start, int length);

    public TextValue substring(int start) {
        return substring(start, Math.max(length() - start, start));
    }

    public abstract TextValue trim();

    public abstract TextValue ltrim();

    public abstract TextValue rtrim();

    public abstract TextValue toLower();

    public abstract TextValue toUpper();

    public abstract ListValue split(String separator);

    public abstract TextValue replace(String find, String replace);

    public abstract TextValue reverse();

    public abstract TextValue plus(TextValue other);

    public abstract boolean startsWith(TextValue other);

    public abstract boolean endsWith(TextValue other);

    public abstract boolean contains(TextValue other);

    public abstract int compareTo(TextValue other);

    @Override
    int unsafeCompareTo(Value otherValue) {
        return compareTo((TextValue) otherValue);
    }

    @Override
    public final boolean equals(boolean x) {
        return false;
    }

    @Override
    public final boolean equals(long x) {
        return false;
    }

    @Override
    public final boolean equals(double x) {
        return false;
    }

    @Override
    public ValueGroup valueGroup() {
        return ValueGroup.TEXT;
    }

    @Override
    public NumberType numberType() {
        return NumberType.NO_NUMBER;
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapText(this);
    }

    abstract Matcher matcher(Pattern pattern);
}
