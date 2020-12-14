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

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ExtendableIterator<T> extends WrappedIterator<T> {

    private final Deque<Iterator<T>> itors;
    private final List<Iterator<T>> removedItors;

    private Iterator<T> currentIterator;

    public ExtendableIterator() {
        this.itors = new ConcurrentLinkedDeque<>();
        this.removedItors = new ArrayList<>();
        this.currentIterator = null;
    }

    public ExtendableIterator(Iterator<T> itor) {
        this();
        this.extend(itor);
    }

    public ExtendableIterator(Iterator<T> itor1, Iterator<T> itor2) {
        this();
        this.extend(itor1);
        this.extend(itor2);
    }

    public ExtendableIterator<T> extend(Iterator<T> itor) {
        Preconditions.checkState(this.currentIterator == null,
                "Can't extend iterator after iterating");
        if (itor != null) {
            this.itors.addLast(itor);
        }
        return this;
    }

    @Override
    public void close() throws Exception {
        for (Iterator<T> itor : this.removedItors) {
            if (itor instanceof AutoCloseable) {
                ((AutoCloseable) itor).close();
            }
        }
        for (Iterator<T> itor : this.itors) {
            if (itor instanceof AutoCloseable) {
                ((AutoCloseable) itor).close();
            }
        }
    }

    @Override
    protected Iterator<?> originIterator() {
        return this.currentIterator;
    }

    @Override
    protected boolean fetch() {
        assert this.current == none();
        if (this.itors.isEmpty()) {
            return false;
        }

        if (this.currentIterator != null && this.currentIterator.hasNext()) {
            this.current = this.currentIterator.next();
            return true;
        }

        Iterator<T> first = null;
        while ((first = this.itors.peekFirst()) != null && !first.hasNext()) {
            if (first == this.itors.peekLast() && this.itors.size() == 1) {
                // The last one
                return false;
            }
            this.removedItors.add(this.itors.removeFirst());
        }

        assert first != null && first.hasNext();
        this.currentIterator = first;
        this.current = this.currentIterator.next();
        return true;
    }
}
