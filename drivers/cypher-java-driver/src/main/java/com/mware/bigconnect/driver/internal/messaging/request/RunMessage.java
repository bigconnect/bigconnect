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
import com.mware.bigconnect.driver.internal.messaging.Message;

import java.util.Collections;
import java.util.Map;

import static java.lang.String.format;

/**
 * RUN request message
 * <p>
 * Sent by clients to start a new Tank job for a given statement and
 * parameter set.
 */
public class RunMessage implements Message
{
    public final static byte SIGNATURE = 0x10;

    private final String statement;
    private final Map<String,Value> parameters;

    public RunMessage( String statement )
    {
        this( statement, Collections.emptyMap() );
    }

    public RunMessage(String statement, Map<String,Value> parameters )
    {
        this.statement = statement;
        this.parameters = parameters;
    }

    public String statement()
    {
        return statement;
    }

    public Map<String,Value> parameters()
    {
        return parameters;
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }

    @Override
    public String toString()
    {
        return format( "RUN \"%s\" %s", statement, parameters );
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

        RunMessage that = (RunMessage) o;

        return !(parameters != null ? !parameters.equals( that.parameters ) : that.parameters != null) &&
               !(statement != null ? !statement.equals( that.statement ) : that.statement != null);

    }

    @Override
    public int hashCode()
    {
        int result = statement != null ? statement.hashCode() : 0;
        result = 31 * result + (parameters != null ? parameters.hashCode() : 0);
        return result;
    }
}
