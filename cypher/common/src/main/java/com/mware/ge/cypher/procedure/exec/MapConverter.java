/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package com.mware.ge.cypher.procedure.exec;

import com.mware.ge.cypher.procedure.impl.DefaultParameterValue;

import java.util.function.Function;

import static com.mware.ge.cypher.procedure.impl.DefaultParameterValue.ntMap;
import static com.mware.ge.cypher.procedure.exec.ParseUtil.parseMap;

/**
 * A naive implementation of a Cypher-map/json parser. If you find yourself using this
 * for parsing huge json-document in a place where performance matters - you probably need
 * to rethink your decision.
 */
public class MapConverter implements Function<String, DefaultParameterValue>
{
    @Override
    public DefaultParameterValue apply(String s )
    {
        String value = s.trim();
        if ( value.equalsIgnoreCase( "null" ) )
        {
            return ntMap( null );
        }
        else
        {
            return ntMap( parseMap( value ) );
        }
    }
}
