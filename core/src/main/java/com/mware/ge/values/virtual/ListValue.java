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
package com.mware.ge.values.virtual;

import com.mware.ge.Authorizations;
import com.mware.ge.collection.Iterators;
import com.mware.ge.collection.PrefetchingIterator;
import com.mware.ge.values.*;
import com.mware.ge.values.storable.ArrayValue;
import com.mware.ge.values.storable.Values;

import java.util.*;

import static com.mware.ge.values.SequenceValue.IterationPreference.RANDOM_ACCESS;
import static com.mware.ge.values.storable.Values.NO_VALUE;
import static com.mware.ge.values.virtual.ArrayHelpers.containsNull;
import static com.mware.ge.values.virtual.VirtualValues.EMPTY_LIST;

public abstract class ListValue extends VirtualValue implements SequenceValue, Iterable<AnyValue> {
    public abstract int size();

    @Override
    public abstract AnyValue value(int offset);

    @Override
    public String getTypeName() {
        return "List";
    }

    static final class ArrayValueListValue extends ListValue {
        private final ArrayValue array;

        ArrayValueListValue(ArrayValue array) {
            this.array = array;
        }

        @Override
        public IterationPreference iterationPreference() {
            return RANDOM_ACCESS;
        }

        @Override
        public boolean storable() {
            return true;
        }

        @Override
        public ArrayValue toStorableArray() {
            return array;
        }

        @Override
        public int size() {
            return array.length();
        }

        @Override
        public AnyValue value(int offset) {
            return array.value(offset);
        }

        @Override
        public int computeHash() {
            return array.hashCode();
        }
    }

    static final class ArrayListValue extends ListValue {
        private final AnyValue[] values;

        ArrayListValue(AnyValue[] values) {
            assert values != null;
            assert !containsNull(values);

            this.values = values;
        }

        @Override
        public IterationPreference iterationPreference() {
            return RANDOM_ACCESS;
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public AnyValue value(int offset) {
            return values[offset];
        }

        @Override
        public AnyValue[] asArray() {
            return values;
        }

        @Override
        public int computeHash() {
            return Arrays.hashCode(values);
        }
    }

    static final class JavaListListValue extends ListValue {
        private final List<AnyValue> values;

        JavaListListValue(List<AnyValue> values) {
            assert values != null;
            assert !containsNull(values);

            this.values = values;
        }

        @Override
        public IterationPreference iterationPreference() {
            return IterationPreference.ITERATION;
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public AnyValue value(int offset) {
            return values.get(offset);
        }

        @Override
        public AnyValue[] asArray() {
            return values.toArray(new AnyValue[0]);
        }

        @Override
        public int computeHash() {
            return values.hashCode();
        }

        @Override
        public Iterator<AnyValue> iterator() {
            return values.iterator();
        }
    }

    static final class ListSlice extends ListValue {
        private final ListValue inner;
        private final int from;
        private final int to;

        ListSlice(ListValue inner, int from, int to) {
            assert from >= 0;
            assert to <= inner.size();
            assert from <= to;
            this.inner = inner;
            this.from = from;
            this.to = to;
        }

        @Override
        public IterationPreference iterationPreference() {
            return inner.iterationPreference();
        }

        @Override
        public int size() {
            return to - from;
        }

        @Override
        public AnyValue value(int offset) {
            return inner.value(offset + from);
        }

        @Override
        public Iterator<AnyValue> iterator() {
            switch (inner.iterationPreference()) {
                case RANDOM_ACCESS:
                    return super.iterator();
                case ITERATION:
                    return new PrefetchingIterator<AnyValue>() {
                        private int count;
                        private Iterator<AnyValue> innerIterator = inner.iterator();

                        @Override
                        protected AnyValue fetchNextOrNull() {
                            //make sure we are at least at first element
                            while (count < from && innerIterator.hasNext()) {
                                innerIterator.next();
                                count++;
                            }
                            //check if we are done
                            if (count < from || count >= to || !innerIterator.hasNext()) {
                                return null;
                            }
                            //take the next step
                            count++;
                            return innerIterator.next();
                        }
                    };

                default:
                    throw new IllegalStateException("unknown iteration preference");
            }
        }
    }

    static final class ReversedList extends ListValue {
        private final ListValue inner;

        ReversedList(ListValue inner) {
            this.inner = inner;
        }

        @Override
        public IterationPreference iterationPreference() {
            return inner.iterationPreference();
        }

        @Override
        public int size() {
            return inner.size();
        }

        @Override
        public AnyValue value(int offset) {
            return inner.value(size() - 1 - offset);
        }
    }

    static final class DropNoValuesListValue extends ListValue {
        private final ListValue inner;
        private int size = -1;

        DropNoValuesListValue(ListValue inner) {
            this.inner = inner;
        }

        @Override
        public int size() {
            if (size < 0) {
                int s = 0;
                for (int i = 0; i < inner.size(); i++) {
                    if (inner.value(i) != NO_VALUE) {
                        s++;
                    }
                }
                size = s;
            }

            return size;
        }

        @Override
        public AnyValue value(int offset) {
            int actualOffset = 0;
            int size = inner.size();
            for (int i = 0; i < size; i++) {
                AnyValue value = inner.value(i);
                if (value != NO_VALUE) {
                    if (actualOffset == offset) {
                        return value;
                    }
                    actualOffset++;
                }
            }

            throw new IndexOutOfBoundsException();
        }

        @Override
        public Iterator<AnyValue> iterator() {
            return new FilteredIterator();
        }

        @Override
        public IterationPreference iterationPreference() {
            return IterationPreference.ITERATION;
        }

        private class FilteredIterator implements Iterator<AnyValue> {
            private AnyValue next;
            private int index;

            FilteredIterator() {
                computeNext();
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public AnyValue next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                AnyValue current = next;
                computeNext();
                return current;
            }

            private void computeNext() {
                if (index >= inner.size()) {
                    next = null;
                } else {
                    while (true) {
                        if (index >= inner.size()) {
                            next = null;
                            return;
                        }
                        AnyValue candidate = inner.value(index++);
                        if (candidate != NO_VALUE) {
                            next = candidate;
                            return;
                        }
                    }
                }
            }
        }
    }

    static final class IntegralRangeListValue extends ListValue {
        private final long start;
        private final long end;
        private final long step;
        private int length = -1;

        IntegralRangeListValue(long start, long end, long step) {
            this.start = start;
            this.end = end;
            this.step = step;
        }

        @Override
        public IterationPreference iterationPreference() {
            return RANDOM_ACCESS;
        }

        @Override
        public String toString() {
            return "Range(" + start + "..." + end + ", step = " + step + ")";
        }

        @Override
        public int size() {
            if (length != -1) {
                return length;
            } else {
                long l = ((end - start) / step) + 1;
                if (l > Integer.MAX_VALUE) {
                    throw new OutOfMemoryError("Cannot index an collection of size " + l);
                }
                length = (int) l;
                return length;
            }
        }

        @Override
        public AnyValue value(int offset) {
            if (offset >= size()) {
                throw new IndexOutOfBoundsException();
            } else {
                return Values.longValue(start + offset * step);
            }
        }

        @Override
        public int computeHash() {
            int hashCode = 1;
            long current = start;
            int size = size();
            for (int i = 0; i < size; i++, current += step) {
                hashCode = 31 * hashCode + Long.hashCode(current);
            }
            return hashCode;
        }

    }

    static final class ConcatList extends ListValue {
        private final ListValue[] lists;
        private int size = -1;

        ConcatList(ListValue[] lists) {
            this.lists = lists;
        }

        @Override
        public IterationPreference iterationPreference() {
            return IterationPreference.ITERATION;
        }

        @Override
        public int size() {
            if (size < 0) {
                int s = 0;
                for (ListValue list : lists) {
                    s += list.size();
                }
                size = s;
            }
            return size;
        }

        @Override
        public AnyValue value(int offset) {
            for (ListValue list : lists) {
                int size = list.size();
                if (offset < size) {
                    return list.value(offset);
                }
                offset -= size;
            }
            throw new IndexOutOfBoundsException();
        }
    }

    static final class AppendList extends ListValue {
        private final ListValue base;
        private final AnyValue[] appended;

        AppendList(ListValue base, AnyValue[] appended) {
            this.base = base;
            this.appended = appended;
        }

        @Override
        public IterationPreference iterationPreference() {
            return base.iterationPreference();
        }

        @Override
        public int size() {
            return base.size() + appended.length;
        }

        @Override
        public AnyValue value(int offset) {
            int size = base.size();
            if (offset < size) {
                return base.value(offset);
            } else if (offset < size + appended.length) {
                return appended[offset - size];
            } else {
                throw new IndexOutOfBoundsException(offset + " is outside range " + size);
            }
        }

        @Override
        public Iterator<AnyValue> iterator() {
            switch (base.iterationPreference()) {
                case RANDOM_ACCESS:
                    return super.iterator();
                case ITERATION:
                    return Iterators.appendTo(base.iterator(), appended);
                default:
                    throw new IllegalStateException("unknown iteration preference");
            }
        }
    }

    static final class PrependList extends ListValue {
        private final ListValue base;
        private final AnyValue[] prepended;

        PrependList(ListValue base, AnyValue[] prepended) {
            this.base = base;
            this.prepended = prepended;
        }

        @Override
        public IterationPreference iterationPreference() {
            return base.iterationPreference();
        }

        @Override
        public int size() {
            return prepended.length + base.size();
        }

        @Override
        public AnyValue value(int offset) {
            int size = base.size();
            if (offset < prepended.length) {
                return prepended[offset];
            } else if (offset < size + prepended.length) {
                return base.value(offset - prepended.length);
            } else {
                throw new IndexOutOfBoundsException(offset + " is outside range " + size);
            }
        }

        @Override
        public Iterator<AnyValue> iterator() {
            switch (base.iterationPreference()) {
                case RANDOM_ACCESS:
                    return super.iterator();
                case ITERATION:
                    return Iterators.prependTo(base.iterator(), prepended);
                default:
                    throw new IllegalStateException("unknown iteration preference");
            }
        }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean nonEmpty() {
        return size() != 0;
    }

    public boolean storable() {
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getTypeName() + "{");
        int i = 0;
        for (; i < size() - 1; i++) {
            sb.append(value(i));
            sb.append(", ");
        }
        if (size() > 0) {
            sb.append(value(i));
        }
        sb.append('}');
        return sb.toString();
    }

    public ArrayValue toStorableArray() {
        throw new UnsupportedOperationException("List cannot be turned into a storable array");
    }

    @Override
    public boolean isSequenceValue() {
        return true;
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapSequence(this);
    }

    @Override
    public boolean equals(VirtualValue other) {
        return other != null && other.isSequenceValue() && equals((SequenceValue) other);
    }

    public AnyValue head() {
        int size = size();
        if (size == 0) {
            throw new NoSuchElementException("head of empty list");
        }
        return value(0);
    }

    public AnyValue last() {
        int size = size();
        if (size == 0) {
            throw new NoSuchElementException("last of empty list");
        }
        return value(size - 1);
    }

    @Override
    public Iterator<AnyValue> iterator() {
        return new Iterator<AnyValue>() {
            private int count;

            @Override
            public boolean hasNext() {
                return count < size();
            }

            @Override
            public AnyValue next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return value(count++);
            }
        };
    }

    @Override
    public VirtualValueGroup valueGroup() {
        return VirtualValueGroup.LIST;
    }

    @Override
    public int length() {
        return size();
    }

    @Override
    public int compareTo(VirtualValue other, Comparator<AnyValue> comparator) {
        if (!(other instanceof ListValue)) {
            throw new IllegalArgumentException("Cannot compare different virtual values");
        }

        ListValue otherList = (ListValue) other;
        if (iterationPreference() == RANDOM_ACCESS && otherList.iterationPreference() == RANDOM_ACCESS) {
            return randomAccessCompareTo(comparator, otherList);
        } else {
            return iteratorCompareTo(comparator, otherList);
        }
    }

    public AnyValue[] asArray() {
        switch (iterationPreference()) {
            case RANDOM_ACCESS:
                return randomAccessAsArray();
            case ITERATION:
                return iterationAsArray();
            default:
                throw new IllegalStateException("not a valid iteration preference");
        }
    }

    @Override
    public int computeHash() {
        switch (iterationPreference()) {
            case RANDOM_ACCESS:
                return randomAccessComputeHash();
            case ITERATION:
                return iterationComputeHash();
            default:
                throw new IllegalStateException("not a valid iteration preference");
        }
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer, Authorizations authorizations) throws E {
        switch (iterationPreference()) {
            case RANDOM_ACCESS:
                randomAccessWriteTo(writer, authorizations);
                break;
            case ITERATION:
                iterationWriteTo(writer, authorizations);
                break;
            default:
                throw new IllegalStateException("not a valid iteration preference");
        }
    }

    public ListValue dropNoValues() {
        return new DropNoValuesListValue(this);
    }

    public ListValue slice(int from, int to) {
        int f = Math.max(from, 0);
        int t = Math.min(to, size());
        if (f > t) {
            return EMPTY_LIST;
        } else {
            return new ListSlice(this, f, t);
        }
    }

    public ListValue tail() {
        return slice(1, size());
    }

    public ListValue drop(int n) {
        int size = size();
        int start = Math.max(0, Math.min(n, size));
        return new ListSlice(this, start, size);
    }

    public ListValue take(int n) {
        int end = Math.max(0, Math.min(n, size()));
        return new ListSlice(this, 0, end);
    }

    public ListValue reverse() {
        return new ReversedList(this);
    }

    public ListValue append(AnyValue... values) {
        if (values.length == 0) {
            return this;
        }
        return new AppendList(this, values);
    }

    public ListValue prepend(AnyValue... values) {
        if (values.length == 0) {
            return this;
        }
        return new PrependList(this, values);
    }

    private AnyValue[] iterationAsArray() {
        ArrayList<AnyValue> values = new ArrayList<>();
        int size = 0;
        for (AnyValue value : this) {
            values.add(value);
            size++;
        }
        return values.toArray(new AnyValue[size]);
    }

    private AnyValue[] randomAccessAsArray() {
        int size = size();
        AnyValue[] values = new AnyValue[size];
        for (int i = 0; i < values.length; i++) {
            values[i] = value(i);
        }
        return values;
    }

    private int randomAccessComputeHash() {
        int hashCode = 1;
        int size = size();
        for (int i = 0; i < size; i++) {
            hashCode = 31 * hashCode + value(i).hashCode();
        }
        return hashCode;
    }

    private int iterationComputeHash() {
        int hashCode = 1;
        for (AnyValue value : this) {
            hashCode = 31 * hashCode + value.hashCode();
        }
        return hashCode;
    }

    private <E extends Exception> void randomAccessWriteTo(AnyValueWriter<E> writer, Authorizations authorizations) throws E {
        writer.beginList(size());
        for (int i = 0; i < size(); i++) {
            value(i).writeTo(writer, authorizations);
        }
        writer.endList();
    }

    private <E extends Exception> void iterationWriteTo(AnyValueWriter<E> writer, Authorizations authorizations) throws E {
        writer.beginList(size());
        for (AnyValue value : this) {
            value.writeTo(writer, authorizations);
        }
        writer.endList();
    }

    private int randomAccessCompareTo(Comparator<AnyValue> comparator, ListValue otherList) {
        int x = Integer.compare(this.length(), otherList.length());

        if (x == 0) {
            for (int i = 0; i < length(); i++) {
                x = comparator.compare(this.value(i), otherList.value(i));
                if (x != 0) {
                    return x;
                }
            }
        }

        return x;
    }

    private int iteratorCompareTo(Comparator<AnyValue> comparator, ListValue otherList) {
        Iterator<AnyValue> thisIterator = iterator();
        Iterator<AnyValue> thatIterator = otherList.iterator();
        while (thisIterator.hasNext()) {
            if (!thatIterator.hasNext()) {
                return 1;
            }
            int compare = comparator.compare(thisIterator.next(), thatIterator.next());
            if (compare != 0) {
                return compare;
            }
        }
        if (thatIterator.hasNext()) {
            return -1;
        } else {
            return 0;
        }
    }
}
