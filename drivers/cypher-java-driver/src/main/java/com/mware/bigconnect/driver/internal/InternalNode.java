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
import com.mware.bigconnect.driver.internal.value.NodeValue;
import com.mware.bigconnect.driver.types.Node;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * {@link Node} implementation that directly contains labels and properties.
 */
public class InternalNode extends InternalEntity implements Node
{
    private final Collection<String> labels;

    public InternalNode( String id )
    {
        this( id, Collections.<String>emptyList(), Collections.<String,Value>emptyMap() );
    }

    public InternalNode(String id, Collection<String> labels, Map<String, Value> properties )
    {
        super( id, properties );
        this.labels = labels;
    }

    @Override
    public Collection<String> labels()
    {
        return labels;
    }

    @Override
    public boolean hasLabel( String label )
    {
        return labels.contains( label );
    }

    @Override
    public Value asValue()
    {
        return new NodeValue( this );
    }

    @Override
    public String toString()
    {
        return String.format( "node<%s>", id()  );
    }
}
