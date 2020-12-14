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

import com.mware.bigconnect.driver.*;
import com.mware.bigconnect.driver.internal.util.Extract;
import com.mware.bigconnect.driver.internal.value.MapValue;

import java.util.Map;

public abstract class AbstractStatementRunner implements StatementRunner
{
    @Override
    public final StatementResult run(String statementTemplate, Value parameters )
    {
        return run( new Statement( statementTemplate, parameters ) );
    }

    @Override
    public final StatementResult run(String statementTemplate, Map<String, Object> statementParameters )
    {
        return run( statementTemplate, parameters( statementParameters ) );
    }

    @Override
    public final StatementResult run(String statementTemplate, Record statementParameters )
    {
        return run( statementTemplate, parameters( statementParameters ) );
    }

    @Override
    public final StatementResult run( String statementText )
    {
        return run( statementText, Values.EmptyMap );
    }

    public static Value parameters( Record record )
    {
        return record == null ? Values.EmptyMap : parameters( record.asMap() );
    }

    public static Value parameters( Map<String, Object> map )
    {
        if ( map == null || map.isEmpty() )
        {
            return Values.EmptyMap;
        }
        return new MapValue( Extract.mapOfValues( map ) );
    }
}
