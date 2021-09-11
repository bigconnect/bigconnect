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
package com.mware.ge.collection;

import java.util.NoSuchElementException;

public abstract class PrefetchingRawIterator<T, EXCEPTION extends Exception> implements RawIterator<T, EXCEPTION>
{
    boolean hasFetchedNext;
    T nextObject;

    /**
     * @return {@code true} if there is a next item to be returned from the next
     * call to {@link #next()}.
     */
    @Override
    public boolean hasNext() throws EXCEPTION
    {
        return peek() != null;
    }

    /**
     * @return the next element that will be returned from {@link #next()} without
     * actually advancing the iterator
     */
    public T peek() throws EXCEPTION
    {
        if ( hasFetchedNext )
        {
            return nextObject;
        }

        nextObject = fetchNextOrNull();
        hasFetchedNext = true;
        return nextObject;
    }

    /**
     * Uses {@link #hasNext()} to try to fetch the next item and returns it
     * if found, otherwise it throws a {@link NoSuchElementException}.
     *
     * @return the next item in the iteration, or throws
     * {@link NoSuchElementException} if there's no more items to return.
     */
    @Override
    public T next() throws EXCEPTION
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        T result = nextObject;
        nextObject = null;
        hasFetchedNext = false;
        return result;
    }

    protected abstract T fetchNextOrNull() throws EXCEPTION;

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
