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
package com.mware.bigconnect.driver;

import com.mware.bigconnect.driver.internal.value.MapValue;
import com.mware.bigconnect.driver.summary.ResultSummary;
import com.mware.bigconnect.driver.util.Immutable;

import java.util.Map;

import static java.lang.String.format;
import static com.mware.bigconnect.driver.Values.ofValue;
import static com.mware.bigconnect.driver.Values.value;
import static com.mware.bigconnect.driver.internal.util.Iterables.newHashMapWithSize;
import static com.mware.bigconnect.driver.internal.util.Preconditions.checkArgument;

/**
 * An executable statement, i.e. the statements' text and its parameters.
 *
 * @see Session
 * @see Transaction
 * @see StatementResult
 * @see StatementResult#consume()
 * @see ResultSummary
 * @since 1.0
 */
@Immutable
public class Statement
{
    private final String text;
    private final Value parameters;

    /**
     * Create a new statement.
     * @param text the statement text
     * @param parameters the statement parameters
     */
    public Statement(String text, Value parameters )
    {
        this.text = validateQuery( text );
        if( parameters == null )
        {
            this.parameters = Values.EmptyMap;
        }
        else if ( parameters instanceof MapValue )
        {
            this.parameters = parameters;
        }
        else
        {
            throw new IllegalArgumentException( "The parameters should be provided as Map type. Unsupported parameters type: " + parameters.type().name() );
        }
    }

    /**
     * Create a new statement.
     * @param text the statement text
     * @param parameters the statement parameters
     */
    public Statement(String text, Map<String, Object> parameters )
    {
        this( text, Values.value( parameters ) );
    }

    /**
     * Create a new statement.
     * @param text the statement text
     */
    public Statement( String text )
    {
        this( text, Values.EmptyMap );
    }

    /**
     * @return the statement's text
     */
    public String text()
    {
        return text;
    }

    /**
     * @return the statement's parameters
     */
    public Value parameters()
    {
        return parameters;
    }

    /**
     * @param newText the new statement's text
     * @return a new statement with updated text
     */
    public Statement withText( String newText )
    {
        return new Statement( newText, parameters );
    }

    /**
     * @param newParameters the new statement's parameters
     * @return a new statement with updated parameters
     */
    public Statement withParameters( Value newParameters )
    {
        return new Statement( text, newParameters );
    }

    /**
     * @param newParameters the new statement's parameters
     * @return a new statement with updated parameters
     */
    public Statement withParameters( Map<String, Object> newParameters )
    {
        return new Statement( text, newParameters );
    }

    /**
     * Create a new statement with new parameters derived by updating this'
     * statement's parameters using the given updates.
     *
     * Every update key that points to a null value will be removed from
     * the new statement's parameters. All other entries will just replace
     * any existing parameter in the new statement.
     *
     * @param updates describing how to update the parameters
     * @return a new statement with updated parameters
     */
    public Statement withUpdatedParameters( Value updates )
    {
        if ( updates == null || updates.isEmpty() )
        {
            return this;
        }
        else
        {
            Map<String,Value> newParameters = newHashMapWithSize( Math.max( parameters.size(), updates.size() ) );
            newParameters.putAll( parameters.asMap( ofValue() ) );
            for ( Map.Entry<String, Value> entry : updates.asMap( ofValue() ).entrySet() )
            {
                Value value = entry.getValue();
                if ( value.isNull() )
                {
                    newParameters.remove( entry.getKey() );
                }
                else
                {
                    newParameters.put( entry.getKey(), value );
                }
            }
            return withParameters( value(newParameters) );
        }
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

        Statement statement = (Statement) o;
        return text.equals( statement.text ) && parameters.equals( statement.parameters );

    }

    @Override
    public int hashCode()
    {
        int result = text.hashCode();
        result = 31 * result + parameters.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return format( "Statement{text='%s', parameters=%s}", text, parameters );
    }

    private static String validateQuery(String query )
    {
        checkArgument( query != null, "Cypher query should not be null" );
        checkArgument( !query.isEmpty(), "Cypher query should not be an empty string" );
        return query;
    }
}
