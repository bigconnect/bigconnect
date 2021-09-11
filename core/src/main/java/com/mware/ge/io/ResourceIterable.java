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

import java.util.stream.Stream;

/**
 * {@link Iterable} whose {@link ResourceIterator iterators} have associated resources
 * that need to be released.
 * <p>
 * {@link ResourceIterator ResourceIterators} are always automatically released when their owning
 * transaction is committed or rolled back.
 * <p>
 * Inside a long running transaction, it is possible to release associated resources early. To do so
 * you must ensure that all returned ResourceIterators are either fully exhausted, or explicitly closed.
 * <p>
 * If you intend to exhaust the returned iterators, you can use conventional code as you would with a normal Iterable:
 *
 * <pre>
 * {@code
 * ResourceIterable<Object> iterable;
 * for ( Object item : iterable )
 * {
 *     ...
 * }
 * }
 * </pre>
 * <p>
 * However, if your code might not exhaust the iterator, (run until {@link java.util.Iterator#hasNext()}
 * returns {@code false}), {@link ResourceIterator} provides you with a {@link ResourceIterator#close()} method that
 * can be invoked to release its associated resources early, by using a {@code finally}-block, or try-with-resource.
 *
 * <pre>
 * {@code
 * ResourceIterable<Object> iterable;
 * ResourceIterator<Object> iterator = iterable.iterator();
 * try
 * {
 *     while ( iterator.hasNext() )
 *     {
 *         Object item = iterator.next();
 *         if ( ... )
 *         {
 *             return item; // iterator may not be exhausted.
 *         }
 *     }
 * }
 * finally
 * {
 *     iterator.close();
 * }
 * }
 * </pre>
 *
 * @param <T> the type of values returned through the iterators
 * @see ResourceIterator
 */
public interface ResourceIterable<T> extends Iterable<T> {
    /**
     * Returns an {@link ResourceIterator iterator} with associated resources that may be managed.
     */
    @Override
    ResourceIterator<T> iterator();

    /**
     * @return this iterable as a {@link Stream}
     */
    default Stream<T> stream() {
        return iterator().stream();
    }
}
