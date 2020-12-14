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
package com.mware.bigconnect.driver.internal.summary;

import com.mware.bigconnect.driver.summary.SummaryCounters;

public class InternalSummaryCounters implements SummaryCounters
{
    public static final InternalSummaryCounters EMPTY_STATS =
            new InternalSummaryCounters( 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 );
    private final int nodesCreated;
    private final int nodesDeleted;
    private final int relationshipsCreated;
    private final int relationshipsDeleted;
    private final int propertiesSet;
    private final int labelsAdded;
    private final int labelsRemoved;
    private final int indexesAdded;
    private final int indexesRemoved;
    private final int constraintsAdded;
    private final int constraintsRemoved;

    public InternalSummaryCounters(
            int nodesCreated, int nodesDeleted,
            int relationshipsCreated, int relationshipsDeleted,
            int propertiesSet,
            int labelsAdded, int labelsRemoved,
            int indexesAdded, int indexesRemoved,
            int constraintsAdded, int constraintsRemoved )
    {
        this.nodesCreated = nodesCreated;
        this.nodesDeleted = nodesDeleted;
        this.relationshipsCreated = relationshipsCreated;
        this.relationshipsDeleted = relationshipsDeleted;
        this.propertiesSet = propertiesSet;
        this.labelsAdded = labelsAdded;
        this.labelsRemoved = labelsRemoved;
        this.indexesAdded = indexesAdded;
        this.indexesRemoved = indexesRemoved;
        this.constraintsAdded = constraintsAdded;
        this.constraintsRemoved = constraintsRemoved;
    }

    @Override
    public boolean containsUpdates()
    {
        return
             isPositive( nodesCreated )
          || isPositive( nodesDeleted )
          || isPositive( relationshipsCreated )
          || isPositive( relationshipsDeleted )
          || isPositive( propertiesSet )
          || isPositive( labelsAdded )
          || isPositive( labelsRemoved )
          || isPositive( indexesAdded )
          || isPositive( indexesRemoved )
          || isPositive( constraintsAdded )
          || isPositive( constraintsRemoved );
    }

    @Override
    public int nodesCreated()
    {
        return nodesCreated;
    }

    @Override
    public int nodesDeleted()
    {
        return nodesDeleted;
    }

    @Override
    public int relationshipsCreated()
    {
        return relationshipsCreated;
    }

    @Override
    public int relationshipsDeleted()
    {
        return relationshipsDeleted;
    }

    @Override
    public int propertiesSet()
    {
        return propertiesSet;
    }

    @Override
    public int labelsAdded()
    {
        return labelsAdded;
    }

    @Override
    public int labelsRemoved()
    {
        return labelsRemoved;
    }

    @Override
    public int indexesAdded()
    {
        return indexesAdded;
    }

    @Override
    public int indexesRemoved()
    {
        return indexesRemoved;
    }

    @Override
    public int constraintsAdded()
    {
        return constraintsAdded;
    }

    @Override
    public int constraintsRemoved()
    {
        return constraintsRemoved;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        InternalSummaryCounters that = (InternalSummaryCounters) o;

        return nodesCreated == that.nodesCreated
            && nodesDeleted == that.nodesDeleted
            && relationshipsCreated == that.relationshipsCreated
            && relationshipsDeleted == that.relationshipsDeleted
            && propertiesSet == that.propertiesSet
            && labelsAdded == that.labelsAdded
            && labelsRemoved == that.labelsRemoved
            && indexesAdded == that.indexesAdded
            && indexesRemoved == that.indexesRemoved
            && constraintsAdded == that.constraintsAdded
            && constraintsRemoved == that.constraintsRemoved;
    }

    @Override
    public int hashCode()
    {
        int result = nodesCreated;
        result = 31 * result + nodesDeleted;
        result = 31 * result + relationshipsCreated;
        result = 31 * result + relationshipsDeleted;
        result = 31 * result + propertiesSet;
        result = 31 * result + labelsAdded;
        result = 31 * result + labelsRemoved;
        result = 31 * result + indexesAdded;
        result = 31 * result + indexesRemoved;
        result = 31 * result + constraintsAdded;
        result = 31 * result + constraintsRemoved;
        return result;
    }

    private boolean isPositive( int value )
    {
        return value > 0;
    }
}
