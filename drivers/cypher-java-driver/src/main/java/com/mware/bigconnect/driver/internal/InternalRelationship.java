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
package com.mware.bigconnect.driver.internal;

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.value.RelationshipValue;
import com.mware.bigconnect.driver.types.Relationship;

import java.util.Collections;
import java.util.Map;

/**
 * {@link Relationship} implementation that directly contains type and properties.
 */
public class InternalRelationship extends InternalEntity implements Relationship
{
    private String start;
    private String end;
    private final String type;

    public InternalRelationship( String id, String start, String end, String type )
    {
        this( id, start, end, type, Collections.<String,Value>emptyMap() );
    }

    public InternalRelationship( String id, String start, String end, String type,
                                 Map<String, Value> properties )
    {
        super( id, properties );
        this.start = start;
        this.end = end;
        this.type = type;
    }

    @Override
    public boolean hasType( String relationshipType )
    {
        return type().equals( relationshipType );
    }

    /** Modify the start/end identities of this relationship */
    public void setStartAndEnd( String start, String end )
    {
        this.start = start;
        this.end = end;
    }

    @Override
    public String startNodeId()
    {
        return start;
    }

    @Override
    public String endNodeId()
    {
        return end;
    }

    @Override
    public String type()
    {
        return type;
    }

    @Override
    public Value asValue()
    {
        return new RelationshipValue( this );
    }

    @Override
    public String toString()
    {
        return String.format( "relationship<%s>", id() );
    }
}
