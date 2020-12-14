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

import com.mware.ge.values.Comparison;
import com.mware.ge.values.TernaryComparator;

import java.util.Comparator;

/**
 * Comparator for values. Usable for sorting values, for example during index range scans.
 */
public final class ValueComparator implements Comparator<Value>, TernaryComparator<Value> {
    private final Comparator<ValueGroup> valueGroupComparator;

    ValueComparator(
            Comparator<ValueGroup> valueGroupComparator) {
        this.valueGroupComparator = valueGroupComparator;
    }

    @Override
    public int compare(Value v1, Value v2) {
        assert v1 != null && v2 != null : "null values are not supported, use NoValue.NO_VALUE instead";

        ValueGroup id1 = v1.valueGroup();
        ValueGroup id2 = v2.valueGroup();

        int x = valueGroupComparator.compare(id1, id2);

        if (x == 0) {
            return v1.unsafeCompareTo(v2);
        }
        return x;
    }

    @Override
    public Comparison ternaryCompare(Value v1, Value v2) {
        assert v1 != null && v2 != null : "null values are not supported, use NoValue.NO_VALUE instead";

        ValueGroup id1 = v1.valueGroup();
        ValueGroup id2 = v2.valueGroup();

        int x = valueGroupComparator.compare(id1, id2);

        if (x == 0) {
            return v1.unsafeTernaryCompareTo(v2);
        }
        return Comparison.from(x);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ValueComparator;
    }

    @Override
    public int hashCode() {
        return 1;
    }
}
