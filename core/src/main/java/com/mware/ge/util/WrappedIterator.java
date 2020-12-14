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

public abstract class WrappedIterator<R> implements Iterator<R>, AutoCloseable {

    private static final Object NONE = new Object();

    protected R current;

    public WrappedIterator() {
        this.current = none();
    }

    @Override
    public boolean hasNext() {
        if (this.current != none()) {
            return true;
        }
        return this.fetch();
    }

    @Override
    public R next() {
        if (this.current == none()) {
            this.fetch();
        }
        if (this.current == none()) {
            throw new NoSuchElementException();
        }
        R current = this.current;
        this.current = none();
        return current;
    }

    @Override
    public void remove() {
        Iterator<?> iterator = this.originIterator();
        if (iterator == null) {
            throw new NoSuchElementException(
                    "The origin iterator can't be null for removing");
        }
        iterator.remove();
    }

    @Override
    public void close() throws Exception {
        Iterator<?> iterator = this.originIterator();
        if (iterator instanceof AutoCloseable) {
            ((AutoCloseable) iterator).close();
        }
    }

    @SuppressWarnings("unchecked")
    protected static final <R> R none() {
        return (R) NONE;
    }

    protected abstract Iterator<?> originIterator();

    protected abstract boolean fetch();
}
