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
package com.mware.ge.values;

import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.ValueComparator;
import com.mware.ge.values.storable.Values;
import com.mware.ge.values.virtual.VirtualValueGroup;

import java.util.Comparator;

/**
 * Comparator for any values.
 */
class AnyValueComparator implements Comparator<AnyValue>, TernaryComparator<AnyValue> {
    private final Comparator<VirtualValueGroup> virtualValueGroupComparator;
    private final ValueComparator valueComparator;

    AnyValueComparator(ValueComparator valueComparator, Comparator<VirtualValueGroup> virtualValueGroupComparator) {
        this.virtualValueGroupComparator = virtualValueGroupComparator;
        this.valueComparator = valueComparator;
    }

    private Comparison cmp(AnyValue v1, AnyValue v2, boolean ternary) {
        assert v1 != null && v2 != null : "null values are not supported, use NoValue.NO_VALUE instead";

        // NO_VALUE is bigger than all other values, need to check for that up
        // front
        if (v1 == v2) {
            return Comparison.EQUAL;
        }
        if (v1 == Values.NO_VALUE) {
            return Comparison.GREATER_THAN;
        }
        if (v2 == Values.NO_VALUE) {
            return Comparison.SMALLER_THAN;
        }

        // We must handle sequences as a special case, as they can be both storable and virtual
        boolean isSequence1 = v1.isSequenceValue();
        boolean isSequence2 = v2.isSequenceValue();

        if (isSequence1 && isSequence2) {
            return Comparison.from(compareSequences((SequenceValue) v1, (SequenceValue) v2));
        } else if (isSequence1) {
            return Comparison.from(compareSequenceAndNonSequence((SequenceValue) v1, v2));
        } else if (isSequence2) {
            return Comparison.from(-compareSequenceAndNonSequence((SequenceValue) v2, v1));
        }

        // Handle remaining AnyValues
        boolean isValue1 = v1 instanceof Value;
        boolean isValue2 = v2 instanceof Value;

        int x = Boolean.compare(isValue1, isValue2);

        if (x == 0) {
            //noinspection ConstantConditions
            // Do not turn this into ?-operator
            if (isValue1) {
                if (ternary) {
                    return valueComparator.ternaryCompare((Value) v1, (Value) v2);
                } else {
                    return Comparison.from(valueComparator.compare((Value) v1, (Value) v2));
                }
            } else {
                // This returns int
                return Comparison.from(compareVirtualValues((VirtualValue) v1, (VirtualValue) v2));
            }

        }
        return Comparison.from(x);
    }

    @Override
    public int compare(AnyValue v1, AnyValue v2) {
        return cmp(v1, v2, false).value();
    }

    @Override
    public Comparison ternaryCompare(AnyValue v1, AnyValue v2) {
        return cmp(v1, v2, true);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof AnyValueComparator;
    }

    @Override
    public int hashCode() {
        return 1;
    }

    private int compareVirtualValues(VirtualValue v1, VirtualValue v2) {
        VirtualValueGroup id1 = v1.valueGroup();
        VirtualValueGroup id2 = v2.valueGroup();

        int x = virtualValueGroupComparator.compare(id1, id2);

        if (x == 0) {
            return v1.compareTo(v2, this);
        }
        return x;
    }

    private int compareSequenceAndNonSequence(SequenceValue v1, AnyValue v2) {
        boolean isValue2 = v2 instanceof Value;
        if (isValue2) {
            return -1;
        } else {
            return virtualValueGroupComparator.compare(VirtualValueGroup.LIST, ((VirtualValue) v2).valueGroup());
        }
    }

    private int compareSequences(SequenceValue v1, SequenceValue v2) {
        return v1.compareToSequence(v2, this);
    }
}
