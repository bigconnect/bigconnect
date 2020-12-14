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
package com.mware.ge.cypher.index;

import com.mware.ge.cypher.schema.IndexDescriptor;
import com.mware.ge.cypher.index.IndexOrder;
import com.mware.ge.cypher.index.IndexQuery;

/**
 * Client which accepts nodes and some of their property values.
 */
interface NodeValueClient {
    /**
     * Setup the client for progressing using the supplied progressor. The values feed in accept map to the
     * propertyIds provided here. Called by index implementation.
     *
     * @param descriptor  The descriptor
     * @param query       The query of this progression
     * @param indexOrder  The required order the index should return nodeids in
     * @param needsValues if the index should fetch property values together with node ids for index queries
     */
    void initialize(IndexDescriptor descriptor, IndexQuery[] query, IndexOrder indexOrder, boolean needsValues);

    boolean needsValues();
}
