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
package com.mware.bigconnect.driver.internal.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public abstract class Format
{
    private Format()
    {
        throw new UnsupportedOperationException();
    }

    // formats map using ':' as key-value separator instead of default '='
    public static <V> String formatPairs(Map<String,V> entries )
    {
        Iterator<Entry<String,V>> iterator = entries.entrySet().iterator();
        switch ( entries.size() ) {
            case 0:
                return "{}";

            case 1:
            {
                return String.format( "{%s}", keyValueString( iterator.next() ) );
            }

            default:
            {
                StringBuilder builder = new StringBuilder();
                builder.append( "{" );
                builder.append( keyValueString( iterator.next() ) );
                while ( iterator.hasNext() )
                {
                    builder.append( ',' );
                    builder.append( ' ' );
                    builder.append( keyValueString( iterator.next() ) );
                }
                builder.append( "}" );
                return builder.toString();
            }
        }
    }

    private static <V> String keyValueString(Entry<String,V> entry )
    {
        return String.format( "%s: %s", entry.getKey(), String.valueOf( entry.getValue() ) );
    }

    /**
     * Returns the submitted value if it is not null or an empty string if it is.
     */
    public static String valueOrEmpty(String value )
    {
        return value != null ? value : "";
    }
}
