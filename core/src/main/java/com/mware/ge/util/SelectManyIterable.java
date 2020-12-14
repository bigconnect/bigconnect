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

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class SelectManyIterable<TSource, TDest> implements Iterable<TDest> {
    private final Iterable<? extends TSource> source;

    public SelectManyIterable(Iterable<? extends TSource> source) {
        this.source = source;
    }

    @Override
    public Iterator<TDest> iterator() {
        final Iterator<? extends TSource> it = source.iterator();
        return new Iterator<TDest>() {
            private TDest next;
            private TDest current;
            public Iterator<? extends TDest> innerIterator = null;

            @Override
            public boolean hasNext() {
                loadNext();
                return next != null;
            }

            @Override
            public TDest next() {
                loadNext();
                if (next == null) {
                    throw new NoSuchElementException();
                }
                this.current = this.next;
                this.next = null;
                return this.current;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void loadNext() {
                if (this.next != null) {
                    return;
                }
                while (true) {
                    if (innerIterator != null) {
                        if (innerIterator.hasNext()) {
                            TDest dest = innerIterator.next();
                            if (isIncluded(dest)) {
                                this.next = dest;
                                return;
                            }
                        } else {
                            innerIterator = null;
                        }
                    } else {
                        if (it.hasNext()) {
                            TSource nextSource = it.next();
                            innerIterator = getIterable(nextSource).iterator();
                        } else {
                            return;
                        }
                    }
                }
            }
        };
    }

    protected boolean isIncluded(TDest dest) {
        return true;
    }

    protected abstract Iterable<? extends TDest> getIterable(TSource source);

    protected Iterable<? extends TSource> getSource() {
        return source;
    }
}
