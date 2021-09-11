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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

class FilterIterable<T> implements Iterable<T> {
    private final Iterable<T> iterable;

    private final Predicate<? super T> specification;

    FilterIterable(Iterable<T> iterable, Predicate<? super T> specification) {
        this.iterable = iterable;
        this.specification = specification;
    }

    @Override
    public Iterator<T> iterator() {
        return new FilterIterator<>(iterable.iterator(), specification);
    }

    static class FilterIterator<T> implements Iterator<T> {
        private final Iterator<T> iterator;

        private final Predicate<? super T> specification;

        private T currentValue;
        boolean finished;
        boolean nextConsumed = true;

        FilterIterator(Iterator<T> iterator, Predicate<? super T> specification) {
            this.specification = specification;
            this.iterator = iterator;
        }

        boolean moveToNextValid() {
            boolean found = false;
            while (!found && iterator.hasNext()) {
                T currentValue = iterator.next();
                boolean satisfies = specification.test(currentValue);

                if (satisfies) {
                    found = true;
                    this.currentValue = currentValue;
                    nextConsumed = false;
                }
            }
            if (!found) {
                finished = true;
            }
            return found;
        }

        @Override
        public T next() {
            if (!nextConsumed) {
                nextConsumed = true;
                return currentValue;
            } else {
                if (!finished) {
                    if (moveToNextValid()) {
                        nextConsumed = true;
                        return currentValue;
                    }
                }
            }
            throw new NoSuchElementException("This iterator is exhausted.");
        }

        @Override
        public boolean hasNext() {
            return !finished && (!nextConsumed || moveToNextValid());
        }

        @Override
        public void remove() {
        }
    }
}
