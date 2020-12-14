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
package com.mware.bigconnect.driver.internal.messaging.request;

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.Values;
import com.mware.bigconnect.driver.internal.messaging.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.mware.bigconnect.driver.internal.util.MetadataExtractor.ABSENT_QUERY_ID;

public abstract class AbstractStreamingMessage implements Message
{
    private final Map<String,Value> metadata = new HashMap<>();
    public static final long STREAM_LIMIT_UNLIMITED = -1;

    AbstractStreamingMessage( long n, long id )
    {
        this.metadata.put( "n", Values.value( n ) );
        if ( id != ABSENT_QUERY_ID )
        {
            this.metadata.put( "qid", Values.value( id ) );
        }
    }

    public Map<String,Value> metadata()
    {
        return metadata;
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
        AbstractStreamingMessage that = (AbstractStreamingMessage) o;
        return Objects.equals( metadata, that.metadata );
    }

    protected abstract String name();

    @Override
    public int hashCode()
    {
        return Objects.hash( metadata );
    }

    @Override
    public String toString()
    {
        return String.format( "%s %s", name(), metadata );
    }

}
