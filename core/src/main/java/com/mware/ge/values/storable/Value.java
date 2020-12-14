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

import com.mware.ge.Authorizations;
import com.mware.ge.csv.CSVHeaderInformation;
import com.mware.ge.hashing.HashFunction;
import com.mware.ge.values.*;

import java.time.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mware.ge.values.storable.Values.NO_VALUE;
import static java.lang.String.format;

public abstract class Value extends AnyValue {
    static final Pattern mapPattern = Pattern.compile("\\{(.*)\\}");

    static final Pattern keyValuePattern =
            Pattern.compile("(?:\\A|,)\\s*+(?<k>[a-z_A-Z]\\w*+)\\s*:\\s*(?<v>[^\\s,]+)");

    static final Pattern quotesPattern = Pattern.compile("^[\"']|[\"']$");

    @Override
    public boolean eq(Object other) {
        return other instanceof Value && equals((Value) other);
    }

    public abstract boolean equals(Value other);

    public boolean equals(byte[] x) {
        return false;
    }

    public boolean equals(short[] x) {
        return false;
    }

    public boolean equals(int[] x) {
        return false;
    }

    public boolean equals(long[] x) {
        return false;
    }

    public boolean equals(float[] x) {
        return false;
    }

    public boolean equals(double[] x) {
        return false;
    }

    public boolean equals(boolean x) {
        return false;
    }

    public boolean equals(boolean[] x) {
        return false;
    }

    public boolean equals(long x) {
        return false;
    }

    public boolean equals(double x) {
        return false;
    }

    public boolean equals(char x) {
        return false;
    }

    public boolean equals(String x) {
        return false;
    }

    public boolean equals(char[] x) {
        return false;
    }

    public boolean equals(String[] x) {
        return false;
    }

    public boolean equals(ZonedDateTime[] x) {
        return false;
    }

    public boolean equals(LocalDate[] x) {
        return false;
    }

    public boolean equals(DurationValue[] x) {
        return false;
    }

    public boolean equals(LocalDateTime[] x) {
        return false;
    }

    public boolean equals(LocalTime[] x) {
        return false;
    }

    public boolean equals(OffsetTime[] x) {
        return false;
    }

    @Override
    public Boolean ternaryEquals(AnyValue other) {
        if (other == null || other == NO_VALUE) {
            return null;
        }
        if (other.isSequenceValue() && this.isSequenceValue()) {
            return ((SequenceValue) this).ternaryEquality((SequenceValue) other);
        }
        if (other instanceof Value && ((Value) other).valueGroup() == valueGroup()) {
            Value otherValue = (Value) other;
            if (this.isNaN() || otherValue.isNaN()) {
                return null;
            }
            return equals(otherValue);
        }
        return Boolean.FALSE;
    }

    abstract int unsafeCompareTo(Value other);

    /**
     * Should return {@code null} for values that cannot be compared
     * under Comparability semantics.
     */
    Comparison unsafeTernaryCompareTo(Value other) {
        return Comparison.from(unsafeCompareTo(other));
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer, Authorizations authorizations) throws E {
        writeTo(writer);
    }

    public abstract <E extends Exception> void writeTo(ValueWriter<E> writer) throws E;

    /**
     * Return this value as a regular java boxed primitive, String or primitive array. This method performs defensive
     * copying when needed, so the returned value is safe to modify.
     *
     * @return the object version of the current value
     */
    public abstract Object asObjectCopy();

    /**
     * Return this value as a regular java boxed primitive, String or primitive array. This method does not clone
     * primitive arrays.
     *
     * @return the object version of the current value
     */
    public Object asObject() {
        return asObjectCopy();
    }

    /**
     * Returns a json-like string representation of the current value.
     */
    public abstract String prettyPrint();

    public abstract ValueGroup valueGroup();

    public abstract NumberType numberType();

    public final long hashCode64() {
        HashFunction xxh64 = HashFunction.incrementalXXH64();
        long seed = 1; // Arbitrary seed, but it must always be the same or hash values will change.
        return xxh64.finalise(updateHash(xxh64, xxh64.initialise(seed)));
    }

    public abstract long updateHash(HashFunction hashFunction, long hash);

    public boolean isNaN() {
        return false;
    }

    public static void parseHeaderInformation(CharSequence text, String type, CSVHeaderInformation info) {
        Matcher mapMatcher = mapPattern.matcher(text);
        String errorMessage = format("Failed to parse %s value: '%s'", type, text);
        if (!(mapMatcher.find() && mapMatcher.groupCount() == 1)) {
            throw new InvalidValuesArgumentException(errorMessage);
        }

        String mapContents = mapMatcher.group(1);
        if (mapContents.isEmpty()) {
            throw new InvalidValuesArgumentException(errorMessage);
        }

        Matcher matcher = keyValuePattern.matcher(mapContents);
        if (!(matcher.find())) {
            throw new InvalidValuesArgumentException(errorMessage);
        }

        do {
            String key = matcher.group("k");
            if (key != null) {
                String value = matcher.group("v");
                if (value != null) {
                    info.assign(key, value);
                }
            }
        }
        while (matcher.find());
    }
}
