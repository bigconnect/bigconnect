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

import java.util.function.Function;

/**
 * Static utility methods for retaining records
 *
 * @see StatementResult#list()
 * @since 1.0
 */
public abstract class Records
{
    public static Function<Record,Value> column(int index )
    {
        return column( index, Values.ofValue() );
    }

    public static Function<Record, Value> column(String key )
    {
        return column( key, Values.ofValue() );
    }

    public static <T> Function<Record, T> column(final int index, final Function<Value, T> mapFunction )
    {
        return new Function<Record, T>()
        {
            @Override
            public T apply( Record record )
            {
                return mapFunction.apply( record.get( index ) );
            }
        };
    }
    public static <T> Function<Record, T> column(final String key, final Function<Value, T> mapFunction )
    {
        return new Function<Record, T>()
        {
            @Override
            public T apply( Record recordAccessor )
            {
                return mapFunction.apply( recordAccessor.get( key ) );
            }
        };
    }
}
