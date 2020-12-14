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

import com.google.common.collect.Iterables;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class JoinIterable<T> implements Iterable<T> {
    private final Iterable<? extends T>[] iterables;

    public JoinIterable(Iterable<? extends Iterable<? extends T>> iterables) {
        //noinspection unchecked
        this(Iterables.toArray(iterables, Iterable.class));
    }

    @SafeVarargs
    public JoinIterable(Iterable<? extends T>... iterables) {
        this.iterables = iterables;
    }

    @Override
    public Iterator<T> iterator() {
        if (this.iterables.length == 0) {
            return new Iterator<T>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public T next() {
                    return null;
                }

                @Override
                public void remove() {

                }
            };
        }

        final Queue<Iterable<? extends T>> iterables = new LinkedList<>();
        Collections.addAll(iterables, this.iterables);
        final IteratorWrapper it = new IteratorWrapper();
        it.iterator = iterables.remove().iterator();

        return new Iterator<T>() {
            private T next;
            private T current;

            @Override
            public boolean hasNext() {
                loadNext();
                return next != null;
            }

            @Override
            public T next() {
                loadNext();
                if (this.next == null) {
                    throw new IllegalStateException("iterable doesn't have a next element");
                }
                this.current = this.next;
                this.next = null;
                return this.current;
            }

            @Override
            public void remove() {
                it.iterator.remove();
            }

            private void loadNext() {
                if (this.next != null) {
                    return;
                }

                while (true) {
                    if (it.iterator.hasNext()) {
                        break;
                    }
                    if (iterables.size() == 0) {
                        this.next = null;
                        return;
                    }
                    it.iterator = iterables.remove().iterator();
                }
                this.next = it.iterator.next();
            }
        };
    }

    private class IteratorWrapper {
        public Iterator<? extends T> iterator;
    }

    protected Iterable<? extends T>[] getIterables() {
        return iterables;
    }
}