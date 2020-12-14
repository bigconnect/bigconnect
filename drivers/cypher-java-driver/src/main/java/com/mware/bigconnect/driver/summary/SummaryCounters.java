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
package com.mware.bigconnect.driver.summary;

import com.mware.bigconnect.driver.util.Immutable;

/**
 * Contains counters for various operations that a statement triggered.
 * @since 1.0
 */
@Immutable
public interface SummaryCounters
{
    /**
     * Whether there were any updates at all, eg. any of the counters are greater than 0.
     * @return true if the statement made any updates
     */
    boolean containsUpdates();

    /**
     * @return number of nodes created.
     */
    int nodesCreated();

    /**
     * @return number of nodes deleted.
     */
    int nodesDeleted();

    /**
     * @return number of relationships created.
     */
    int relationshipsCreated();

    /**
     * @return number of relationships deleted.
     */
    int relationshipsDeleted();

    /**
     * @return number of properties (on both nodes and relationships) set.
     */
    int propertiesSet();

    /**
     * @return number of labels added to nodes.
     */
    int labelsAdded();

    /**
     * @return number of labels removed from nodes.
     */
    int labelsRemoved();

    /**
     * @return number of indexes added to the schema.
     */
    int indexesAdded();

    /**
     * @return number of indexes removed from the schema.
     */
    int indexesRemoved();

    /**
     * @return number of constraints added to the schema.
     */
    int constraintsAdded();

    /**
     * @return number of constraints removed from the schema.
     */
    int constraintsRemoved();
}
