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

import java.util.Optional;
import java.util.function.Function;

/**
 * Represents a function that accepts one argument and produces a result, or throws an exception.
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 * @param <E> the type of exception that may be thrown from the function
 */
public interface ThrowingFunction<T, R, E extends Exception> {
    /**
     * Apply a value to this function
     *
     * @param t the function argument
     * @return the function result
     * @throws E an exception if the function fails
     */
    R apply(T t) throws E;

    /**
     * Construct a regular function that calls a throwing function and catches all checked exceptions
     * declared and thrown by the throwing function and rethrows them as {@link UncaughtCheckedException}
     * for handling further up the stack.
     *
     * @param throwing the throwing function to wtap
     * @param <T>      type of arguments
     * @param <R>      type of results
     * @param <E>      type of checked exceptions thrown by the throwing function
     * @return a new, non-throwing function
     * @throws IllegalStateException if an unexpected exception is caught (ie. neither of type E or a runtime exception)
     * @see UncaughtCheckedException
     */
    static <T, R, E extends Exception> Function<T, R> catchThrown(Class<E> clazz, ThrowingFunction<T, R, E> throwing) {
        return input ->
        {
            try {
                return throwing.apply(input);
            } catch (Exception e) {
                if (clazz.isInstance(e)) {
                    throw new UncaughtCheckedException(throwing, clazz.cast(e));
                } else if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new IllegalStateException("Unexpected exception", e);
                }
            }
        };
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    static <E extends Exception> void throwIfPresent(Optional<E> exception) throws E {
        if (exception.isPresent()) {
            throw exception.get();
        }
    }
}
