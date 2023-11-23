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

import com.mware.ge.Element;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.mware.ge.util.CloseableUtils.closeQuietly;
import static org.junit.Assert.assertTrue;

public class IterableUtils {
    @SuppressWarnings("unchecked")
    public static <T> List<T> toList(Iterable<? extends T> iterable) {
        if (iterable instanceof List) {
            return (List<T>) iterable;
        }
        List<T> results = new ArrayList<>();
        for (T o : iterable) {
            results.add(o);
        }
        closeQuietly(iterable);
        return results;
    }

    public static <T> List<T> toList(Iterator<T> iterator) {
        List<T> results = new ArrayList<>();
        while (iterator.hasNext()) {
            T o = iterator.next();
            results.add(o);
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    public static <T> Set<T> toSet(Iterable<? extends T> iterable) {
        if (iterable instanceof Set) {
            return (Set<T>) iterable;
        }
        Set<T> results = new HashSet<>();
        for (T o : iterable) {
            results.add(o);
        }
        closeQuietly(iterable);
        return results;
    }

    @SuppressWarnings("unchecked")
    public static <T> Set<T> toSet(T[] iterable) {
        Set<T> results = new HashSet<>();
        Collections.addAll(results, iterable);
        return results;
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] toArray(Iterable<? extends T> iterable, Class<T> type) {
        if (iterable instanceof Collection) {
            T[] array = (T[]) Array.newInstance(type, ((Collection) iterable).size());
            return ((Collection<T>) iterable).toArray(array);
        }
        List<? extends T> list = toList(iterable);
        T[] array = (T[]) Array.newInstance(type, list.size());
        return list.toArray(array);
    }

    public static <T> int count(Iterable<T> iterable) {
        if (iterable instanceof Collection) {
            return ((Collection) iterable).size();
        }

        int count = 0;
        for (T ignore : iterable) {
            count++;
        }
        closeQuietly(iterable);
        return count;
    }

    public static <T> int count(Iterator<T> iterator) {
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        closeQuietly(iterator);
        return count;
    }

    public static <T> boolean isEmpty(Iterable<T> iterable) {
        Iterator<T> iterator = iterable.iterator();
        try {
            return !iterator.hasNext();
        } finally {
            closeQuietly(iterator);
        }
    }

    public static <T> Iterable<T> toIterable(final T[] arr) {
        return () -> new Iterator<T>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < arr.length;
            }

            @Override
            public T next() {
                return arr[index++];
            }

            @Override
            public void remove() {
                throw new RuntimeException("Not supported");
            }
        };
    }

    public static <T> Iterator<T> toSingleValueIterator(final T obj) {
        return new Iterator<T>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < 1;
            }

            @Override
            public T next() {
                return obj;
            }

            @Override
            public void remove() {
                throw new RuntimeException("Not supported");
            }
        };
    }

    public static <T> T single(final Iterable<? extends T> it) {
        Iterator<? extends T> i = it.iterator();
        if (!i.hasNext()) {
            closeQuietly(i, it);
            throw new IllegalStateException("No items found.");
        }

        T result = i.next();

        if (i.hasNext()) {
            closeQuietly(i, it);
            throw new IllegalStateException("More than 1 item found.");
        }

        closeQuietly(i, it);
        return result;
    }

    public static <T> T anyOrDefault(final Iterable<? extends T> it, T defaultValue) {
        Iterator<? extends T> i = it.iterator();
        if (!i.hasNext()) {
            closeQuietly(i, it);
            return defaultValue;
        }

        try {
            return i.next();
        } finally {
            closeQuietly(i, it);
        }
    }

    public static <T> T singleOrDefault(final Iterable<? extends T> it, T defaultValue) {
        Iterator<? extends T> i = it.iterator();
        if (!i.hasNext()) {
            closeQuietly(i, it);
            return defaultValue;
        }

        T result = i.next();

//        if (i.hasNext()) {
//            T nextValue = i.next();
//            closeQuietly(i, it);
//            throw new IllegalStateException("More than 1 item found. [" + result + ", " + nextValue + "...]");
//        }

        closeQuietly(i, it);
        return result;
    }

    public static <T> T singleOrDefault(final Iterator<? extends T> it, T defaultValue) {
        if (!it.hasNext()) {
            closeQuietly(it);
            return defaultValue;
        }

        T result = it.next();

//        if (it.hasNext()) {
//            T nextValue = it.next();
//            closeQuietly(it, it);
//            throw new IllegalStateException("More than 1 item found. [" + result + ", " + nextValue + "...]");
//        }

        closeQuietly(it);
        return result;
    }

    public static String join(Iterable items, String sep) {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (Object o : items) {
            if (!first) {
                sb.append(sep);
            }
            sb.append(o);
            first = false;
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    public static Iterable<Element> toElementIterable(Iterable<? extends Element> elements) {
        return (Iterable<Element>) elements;
    }

    public static <T extends Element> Map<String, T> toMapById(Iterable<T> elements) {
        Map<String, T> result = new HashMap<>();
        for (T element : elements) {
            if (element != null) {
                result.put(element.getId(), element);
            }
        }
        return result;
    }

    public static <T> void assertContains(Object expected, Iterable<T> iterable) {
        StringBuilder found = new StringBuilder();
        boolean first = true;
        Iterator<T> iterator = iterable.iterator();
        try {
            while (iterator.hasNext()) {
                T o = iterator.next();
                if (expected.equals(o)) {
                    return;
                }
                if (!first) {
                    found.append(", ");
                }
                found.append(o);
                first = false;
            }
        } finally {
            closeQuietly(iterator);
        }

        assertTrue("Iterable does not contain [" + expected + "], found [" + found + "]", false);
    }

    public static <T> T last(Iterable<T> iterable) {
        Iterator<T> iterator = iterable.iterator();
        try {
            T last = null;
            while (iterator.hasNext()) {
                last = iterator.next();
            }
            return last;
        } finally {
            closeQuietly(iterator);
        }
    }

    public static <T> List<T> fastIterableToList(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), true)
                .parallel()
                .collect(Collectors.toList());
    }
}
