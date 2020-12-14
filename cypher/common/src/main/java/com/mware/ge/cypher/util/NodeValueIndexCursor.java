/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
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
package com.mware.ge.cypher.util;

import com.mware.ge.io.AutoCloseablePlus;
import com.mware.ge.values.storable.Value;

import static com.mware.ge.values.storable.Values.NO_VALUE;

public interface NodeValueIndexCursor extends AutoCloseablePlus {

    boolean hasValue();

    Value propertyValue(String propertyName);

    String nodeReference();

    boolean next();

    class Empty implements NodeValueIndexCursor {

        @Override
        public String nodeReference() {
            return null;
        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public void close() {

        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public boolean hasValue() {
            return false;
        }

        @Override
        public Value propertyValue(String propertyName) {
            return NO_VALUE;
        }
    }

    NodeValueIndexCursor EMPTY = new Empty();
}
