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
package com.mware.bigconnect.driver.internal.cluster;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class RoutingContext
{
    public static final RoutingContext EMPTY = new RoutingContext();

    private final Map<String, String> context;

    private RoutingContext()
    {
        this.context = emptyMap();
    }

    public RoutingContext( URI uri )
    {
        this.context = unmodifiableMap( parseParameters( uri ) );
    }

    public boolean isDefined()
    {
        return !context.isEmpty();
    }

    public Map<String, String> asMap()
    {
        return context;
    }

    @Override
    public String toString()
    {
        return "RoutingContext" + context;
    }

    private static Map<String, String> parseParameters(URI uri )
    {
        String query = uri.getQuery();

        if ( query == null || query.isEmpty() )
        {
            return emptyMap();
        }

        Map<String, String> parameters = new HashMap<>();
        String[] pairs = query.split( "&" );
        for ( String pair : pairs )
        {
            String[] keyValue = pair.split( "=" );
            if ( keyValue.length != 2 )
            {
                throw new IllegalArgumentException(
                        "Invalid parameters: '" + pair + "' in URI '" + uri + "'" );
            }

            String key = trimAndVerify( keyValue[0], "key", uri );
            String value = trimAndVerify( keyValue[1], "value", uri );

            String previousValue = parameters.put( key, value );
            if ( previousValue != null )
            {
                throw new IllegalArgumentException(
                        "Duplicated query parameters with key '" + key + "' in URI '" + uri + "'" );
            }
        }
        return parameters;
    }

    private static String trimAndVerify(String string, String name, URI uri )
    {
        String result = string.trim();
        if ( result.isEmpty() )
        {
            throw new IllegalArgumentException( "Illegal empty " + name + " in URI query '" + uri + "'" );
        }
        return result;
    }
}
