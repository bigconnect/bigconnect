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
/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.mware.ge.function;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.mware.ge.function.ThrowingPredicate.throwingPredicate;
import static com.mware.ge.function.ThrowingSupplier.throwingSupplier;

/**
 * Constructors for basic {@link Predicate} types
 */
public class Predicates {
    public static final IntPredicate ALWAYS_TRUE_INT = v -> true;
    public static final IntPredicate ALWAYS_FALSE_INT = v -> false;

    private static final int DEFAULT_POLL_INTERVAL = 20;

    private Predicates() {
    }

    public static <T> Predicate<T> alwaysTrue() {
        return x -> true;
    }

    public static <T> Predicate<T> alwaysFalse() {
        return x -> false;
    }

    public static <T> Predicate<T> notNull() {
        return Objects::nonNull;
    }

    @SafeVarargs
    public static <T> Predicate<T> all(final Predicate<T>... predicates) {
        return all(Arrays.asList(predicates));
    }

    public static <T> Predicate<T> all(final Iterable<Predicate<T>> predicates) {
        return item ->
        {
            for (Predicate<T> predicate : predicates) {
                if (!predicate.test(item)) {
                    return false;
                }
            }
            return true;
        };
    }

    @SafeVarargs
    public static <T> Predicate<T> any(final Predicate<T>... predicates) {
        return any(Arrays.asList(predicates));
    }

    public static <T> Predicate<T> any(final Iterable<Predicate<T>> predicates) {
        return item ->
        {
            for (Predicate<T> predicate : predicates) {
                if (predicate.test(item)) {
                    return true;
                }
            }
            return false;
        };
    }

    public static <T> Predicate<T> instanceOf(@Nonnull final Class<?> clazz) {
        return clazz::isInstance;
    }

    public static <T> Predicate<T> instanceOfAny(final Class<?>... classes) {
        return item ->
        {
            if (item != null) {
                for (Class<?> clazz : classes) {
                    if (clazz.isInstance(item)) {
                        return true;
                    }
                }
            }
            return false;
        };
    }

    public static <T> Predicate<T> noDuplicates() {
        return new Predicate<T>() {
            private final Set<T> visitedItems = new HashSet<>();

            @Override
            public boolean test(T item) {
                return visitedItems.add(item);
            }
        };
    }

    public static <TYPE> TYPE await(Supplier<TYPE> supplier, Predicate<TYPE> predicate, long timeout,
                                    TimeUnit timeoutUnit, long pollInterval, TimeUnit pollUnit) throws TimeoutException {
        return awaitEx(supplier::get, predicate::test, timeout, timeoutUnit, pollInterval, pollUnit);
    }

    public static <TYPE> TYPE await(Supplier<TYPE> supplier, Predicate<TYPE> predicate, long timeout,
                                    TimeUnit timeoutUnit) throws TimeoutException {
        return awaitEx(throwingSupplier(supplier), throwingPredicate(predicate), timeout, timeoutUnit);
    }

    public static <TYPE, EXCEPTION extends Exception> TYPE awaitEx(ThrowingSupplier<TYPE, EXCEPTION> supplier,
                                                                   ThrowingPredicate<TYPE, EXCEPTION> predicate, long timeout, TimeUnit timeoutUnit, long pollInterval,
                                                                   TimeUnit pollUnit) throws TimeoutException, EXCEPTION {
        Suppliers.ThrowingCapturingSupplier<TYPE, EXCEPTION> composed = Suppliers.compose(supplier, predicate);
        awaitEx(composed, timeout, timeoutUnit, pollInterval, pollUnit);
        return composed.lastInput();
    }

    public static <TYPE, EXCEPTION extends Exception> TYPE awaitEx(ThrowingSupplier<TYPE, ? extends EXCEPTION> supplier,
                                                                   ThrowingPredicate<TYPE, ? extends EXCEPTION> predicate, long timeout, TimeUnit timeoutUnit)
            throws TimeoutException, EXCEPTION {
        Suppliers.ThrowingCapturingSupplier<TYPE, EXCEPTION> composed = Suppliers.compose(supplier, predicate);
        awaitEx(composed, timeout, timeoutUnit);
        return composed.lastInput();
    }

    public static void await(BooleanSupplier condition, long timeout, TimeUnit unit) throws TimeoutException {
        awaitEx(condition::getAsBoolean, timeout, unit);
    }

    public static <EXCEPTION extends Exception> void awaitEx(ThrowingSupplier<Boolean, EXCEPTION> condition,
                                                             long timeout, TimeUnit unit) throws TimeoutException, EXCEPTION {
        awaitEx(condition, timeout, unit, DEFAULT_POLL_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public static void await(BooleanSupplier condition, long timeout, TimeUnit timeoutUnit, long pollInterval,
                             TimeUnit pollUnit) throws TimeoutException {
        awaitEx(condition::getAsBoolean, timeout, timeoutUnit, pollInterval, pollUnit);
    }

    public static <EXCEPTION extends Exception> void awaitEx(ThrowingSupplier<Boolean, EXCEPTION> condition,
                                                             long timeout, TimeUnit unit, long pollInterval, TimeUnit pollUnit) throws TimeoutException, EXCEPTION {
        if (!tryAwaitEx(condition, timeout, unit, pollInterval, pollUnit)) {
            throw new TimeoutException(
                    "Waited for " + timeout + " " + unit + ", but " + condition + " was not accepted.");
        }
    }

    public static <EXCEPTION extends Exception> boolean tryAwaitEx(ThrowingSupplier<Boolean, EXCEPTION> condition,
                                                                   long timeout, TimeUnit timeoutUnit, long pollInterval, TimeUnit pollUnit) throws EXCEPTION {
        return tryAwaitEx(condition, timeout, timeoutUnit, pollInterval, pollUnit, Clock.systemUTC());
    }

    public static <EXCEPTION extends Exception> boolean tryAwaitEx(ThrowingSupplier<Boolean, EXCEPTION> condition,
                                                                   long timeout, TimeUnit timeoutUnit, long pollInterval, TimeUnit pollUnit, Clock clock) throws EXCEPTION {
        long deadlineMillis = clock.millis() + timeoutUnit.toMillis(timeout);
        long pollIntervalNanos = pollUnit.toNanos(pollInterval);

        do {
            if (condition.get()) {
                return true;
            }
            LockSupport.parkNanos(pollIntervalNanos);
        }
        while (clock.millis() < deadlineMillis);
        return false;
    }

    public static void awaitForever(BooleanSupplier condition, long checkInterval, TimeUnit unit) {
        long sleep = unit.toNanos(checkInterval);
        do {
            if (condition.getAsBoolean()) {
                return;
            }
            LockSupport.parkNanos(sleep);
        }
        while (true);
    }

    @SafeVarargs
    public static <T> Predicate<T> in(final T... allowed) {
        return in(Arrays.asList(allowed));
    }

    public static <T> Predicate<T> not(Predicate<T> predicate) {
        return t -> !predicate.test(t);
    }

    public static <T> Predicate<T> in(final Iterable<T> allowed) {
        return item ->
        {
            for (T allow : allowed) {
                if (allow.equals(item)) {
                    return true;
                }
            }
            return false;
        };
    }

    public static IntPredicate any(int[] values) {
        return v ->
        {
            for (int value : values) {
                if (v == value) {
                    return true;
                }
            }
            return false;
        };
    }
}
