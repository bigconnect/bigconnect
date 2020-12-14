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
package com.mware.ge.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.function.BiFunction;

/**
 * IO helper methods.
 */
public final class IOUtils {
    private IOUtils() {
    }

    /**
     * Closes given {@link Collection collection} of {@link AutoCloseable closeables}.
     *
     * @param closeables the closeables to close
     * @param <T>        the type of closeable
     * @throws IOException if an exception was thrown by one of the close methods.
     * @see #closeAll(AutoCloseable[])
     */
    public static <T extends AutoCloseable> void closeAll(Collection<T> closeables) throws IOException {
        close(IOException::new, closeables.toArray(new AutoCloseable[0]));
    }

    /**
     * Close all the provided {@link AutoCloseable closeables}, chaining exceptions, if any, into a single {@link UncheckedIOException}.
     *
     * @param closeables to call close on.
     * @param <T>        the type of closeable.
     * @throws UncheckedIOException if any exception is thrown from any of the {@code closeables}.
     */
    public static <T extends AutoCloseable> void closeAllUnchecked(Collection<T> closeables) throws UncheckedIOException {
        closeAllUnchecked(closeables.toArray(new AutoCloseable[0]));
    }

    /**
     * Close all the provided {@link AutoCloseable closeables}, chaining exceptions, if any, into a single {@link UncheckedIOException}.
     *
     * @param closeables to call close on.
     * @param <T>        the type of closeable.
     * @throws UncheckedIOException if any exception is thrown from any of the {@code closeables}.
     */
    @SafeVarargs
    public static <T extends AutoCloseable> void closeAllUnchecked(T... closeables) throws UncheckedIOException {
        try {
            closeAll(closeables);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Closes given {@link Collection collection} of {@link AutoCloseable closeables} ignoring all exceptions.
     *
     * @param closeables the closeables to close
     * @param <T>        the type of closeable
     * @see #closeAll(AutoCloseable[])
     */
    public static <T extends AutoCloseable> void closeAllSilently(Collection<T> closeables) {
        close((msg, cause) -> null, closeables.toArray(new AutoCloseable[0]));
    }

    /**
     * Closes given array of {@link AutoCloseable closeables}. If any {@link AutoCloseable#close()} call throws
     * {@link IOException} than it will be rethrown to the caller after calling {@link AutoCloseable#close()}
     * on other given resources. If more than one {@link AutoCloseable#close()} throw than resulting exception will
     * have suppressed exceptions. See {@link Exception#addSuppressed(Throwable)}
     *
     * @param closeables the closeables to close
     * @param <T>        the type of closeable
     * @throws IOException if an exception was thrown by one of the close methods.
     */
    @SafeVarargs
    public static <T extends AutoCloseable> void closeAll(T... closeables) throws IOException {
        close(IOException::new, closeables);
    }

    /**
     * Closes given array of {@link AutoCloseable closeables} ignoring all exceptions.
     *
     * @param closeables the closeables to close
     * @param <T>        the type of closeable
     */
    @SafeVarargs
    public static <T extends AutoCloseable> void closeAllSilently(T... closeables) {
        close((msg, cause) -> null, closeables);
    }

    /**
     * Close all ofthe given closeables, and if something goes wrong, use the given constructor to create a {@link Throwable} instance with the specific cause
     * attached. The remaining closeables will still be closed, in that case, and if they in turn throw any exceptions then these will be attached as
     * suppressed exceptions.
     *
     * @param constructor The function used to construct the parent throwable that will have the first thrown exception attached as a cause, and any
     *                    remaining exceptions attached as suppressed exceptions. If this function returns {@code null}, then the exception is ignored.
     * @param closeables  an iterator of all the things to close, in order.
     * @param <T>         the type of things to close.
     * @param <E>         the type of the parent exception.
     * @throws E when any {@link AutoCloseable#close()} throws exception
     */
    public static <T extends AutoCloseable, E extends Throwable> void close(BiFunction<String, Throwable, E> constructor, Collection<T> closeables) throws E {
        close(constructor, closeables.toArray(new AutoCloseable[0]));
    }

    /**
     * Close all ofthe given closeables, and if something goes wrong, use the given constructor to create a {@link Throwable} instance with the specific cause
     * attached. The remaining closeables will still be closed, in that case, and if they in turn throw any exceptions then these will be attached as
     * suppressed exceptions.
     *
     * @param constructor The function used to construct the parent throwable that will have the first thrown exception attached as a cause, and any
     *                    remaining exceptions attached as suppressed exceptions. If this function returns {@code null}, then the exception is ignored.
     * @param closeables  all the things to close, in order.
     * @param <T>         the type of things to close.
     * @param <E>         the type of the parent exception.
     * @throws E when any {@link AutoCloseable#close()} throws exception
     */
    @SafeVarargs
    public static <T extends AutoCloseable, E extends Throwable> void close(BiFunction<String, Throwable, E> constructor, T... closeables) throws E {
        E closeThrowable = null;
        for (T closeable : closeables) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            } catch (Exception e) {
                if (closeThrowable == null) {
                    closeThrowable = constructor.apply("Exception closing multiple resources.", e);
                } else {
                    closeThrowable.addSuppressed(e);
                }
            }
        }
        if (closeThrowable != null) {
            throw closeThrowable;
        }
    }
}
