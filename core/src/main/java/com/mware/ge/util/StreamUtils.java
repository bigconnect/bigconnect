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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mware.ge.query.Query;
import com.mware.ge.Element;
import com.mware.ge.GeException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtils {
    private StreamUtils() {

    }

    /**
     * Create a {@link java.util.stream.Stream} containing the results of executing the queries, in order. The results
     * are not loaded into memory first.
     */
    public static Stream<Element> stream(Query... queries) {
        return Arrays.stream(queries)
                .map(query -> StreamSupport.stream(query.elements().spliterator(), false))
                .reduce(Stream::concat)
                .orElseGet(Stream::empty);
    }

    /**
     * Create a {@link java.util.stream.Stream} over the elements of the iterables, in order.  A list of iterators
     * is first created from the iterables, and passed to {@link #stream(Iterator[])}. The iterable elements are not
     * loaded into memory first.
     */
    @SafeVarargs
    @SuppressWarnings("unchecked")
    public static <T> Stream<T> stream(Iterable<T>... iterables) {
        List<Iterator<T>> iterators = Arrays.stream(iterables)
                .map(Iterable::iterator)
                .collect(Collectors.toList());

        return stream(iterators.toArray(new Iterator[iterables.length]));
    }

    /**
     * Create a {@link java.util.stream.Stream} over the elements of the iterators, in order.  The iterator elements
     * are not loaded into memory first.
     */
    @SafeVarargs
    public static <T> Stream<T> stream(Iterator<T>... iterators) {
        return withCloseHandler(
                Arrays.stream(iterators)
                        .map(StreamUtils::streamForIterator)
                        .reduce(Stream::concat)
                        .orElseGet(Stream::empty),
                iterators
        );
    }

    @SafeVarargs
    private static <T> Stream<T> withCloseHandler(Stream<T> stream, Iterator<T>... iterators) {
        return stream.onClose(() -> {
            for (Iterator<T> iterator : iterators) {
                if (iterator instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) iterator).close();
                    } catch (Exception ex) {
                        throw new GeException(
                                String.format("exception occurred when closing %s", iterator.getClass().getName()),
                                ex
                        );
                    }
                }
            }
        });
    }

    private static <T> Stream<T> streamForIterator(Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public static <T> Collector<T, ImmutableList.Builder<T>, ImmutableList<T>> toImmutableList() {
        return Collector.of(
                ImmutableList.Builder<T>::new,
                ImmutableList.Builder<T>::add,
                (l, r) -> l.addAll(r.build()),
                ImmutableList.Builder<T>::build
        );
    }

    public static <T> Collector<T, ImmutableSet.Builder<T>, ImmutableSet<T>> toImmutableSet() {
        return Collector.of(
                ImmutableSet.Builder::new,
                ImmutableSet.Builder::add,
                (l, r) -> l.addAll(r.build()),
                ImmutableSet.Builder<T>::build,
                Collector.Characteristics.UNORDERED
        );
    }

    public static <T, K, V> Collector<T, ImmutableMap.Builder<K, V>, ImmutableMap<K, V>> toImmutableMap(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper
    ) {
        return Collector.of(
                ImmutableMap.Builder<K, V>::new,
                (r, t) -> r.put(keyMapper.apply(t), valueMapper.apply(t)),
                (l, r) -> l.putAll(r.build()),
                ImmutableMap.Builder::build,
                Collector.Characteristics.UNORDERED
        );
    }

    public static <T> Collector<T, LinkedHashSet<T>, LinkedHashSet<T>> toLinkedHashSet() {
        return Collector.of(
                LinkedHashSet::new,
                HashSet::add,
                (a1, a2) -> {
                    LinkedHashSet<T> results = new LinkedHashSet<T>();
                    results.addAll(a1);
                    results.addAll(a2);
                    return results;
                },
                ts -> ts
        );
    }

    public static <TItem, TReturn> TReturn ifEmpty(
            Stream<TItem> stream,
            Supplier<TReturn> trueFunc,
            Function<Stream<TItem>, TReturn> falseFunc
    ) {
        Spliterator<TItem> split = stream.spliterator();
        AtomicReference<TItem> firstItem = new AtomicReference<>();
        if (split.tryAdvance(firstItem::set)) {
            Stream<TItem> newStream = Stream.concat(Stream.of(firstItem.get()), StreamSupport.stream(split, stream.isParallel()));
            return falseFunc.apply(newStream);
        } else {
            return trueFunc.get();
        }
    }

    public static <T> Predicate<T> distinctBy(Function<? super T, ?> fn) {
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        return t -> seen.add(fn.apply(t));
    }

    public static <T, R> Stream<R> mapOptional(Stream<T> stream, Function<T, R> transform) {
        // TODO is there a better way to check the size without getting all the results first
        List<T> l = stream.collect(Collectors.toList());
        if (l.size() == 0) {
            return Stream.of((T) null).map(transform);
        } else {
            return l.stream().map(transform);
        }
    }
}
