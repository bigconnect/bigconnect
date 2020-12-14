/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package com.mware.ge.cypher.util;

import java.util.NoSuchElementException;

public abstract class PrimitiveStringBaseIterator implements StringIterator {
    private boolean hasNextDecided;
    private boolean hasNext;
    protected String next;

    @Override
    public boolean hasNext() {
        if (!hasNextDecided) {
            hasNext = fetchNext();
            hasNextDecided = true;
        }
        return hasNext;
    }

    @Override
    public String next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements in " + this);
        }
        hasNextDecided = false;
        return next;
    }

    /**
     * Fetches the next item in this iterator. Returns whether or not a next item was found. If a next
     * item was found, that value must have been set inside the implementation of this method
     * using {@link #next(String)}.
     */
    protected abstract boolean fetchNext();

    /**
     * Called from inside an implementation of {@link #fetchNext()} if a next item was found.
     * This method returns {@code true} so that it can be used in short-hand conditionals
     * (TODO what are they called?), like:
     * <pre>
     * protected boolean fetchNext()
     * {
     *     return source.hasNext() ? next( source.next() ) : false;
     * }
     * </pre>
     *
     * @param nextItem the next item found.
     */
    protected boolean next(String nextItem) {
        next = nextItem;
        hasNext = true;
        return true;
    }

    @Override
    public void close() {
    }
}
