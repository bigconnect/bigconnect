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

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.Statement;

import java.util.List;

public class RoutingProcedureResponse
{
    private final Statement procedure;
    private final List<Record> records;
    private final Throwable error;

    public RoutingProcedureResponse( Statement procedure, List<Record> records )
    {
        this( procedure, records, null );
    }

    public RoutingProcedureResponse( Statement procedure, Throwable error )
    {
        this( procedure, null, error );
    }

    private RoutingProcedureResponse(Statement procedure, List<Record> records, Throwable error )
    {
        this.procedure = procedure;
        this.records = records;
        this.error = error;
    }

    public boolean isSuccess()
    {
        return records != null;
    }

    public Statement procedure()
    {
        return procedure;
    }

    public List<Record> records()
    {
        if ( !isSuccess() )
        {
            throw new IllegalStateException( "Can't access records of a failed result", error );
        }
        return records;
    }

    public Throwable error()
    {
        if ( isSuccess() )
        {
            throw new IllegalStateException( "Can't access error of a succeeded result " + records );
        }
        return error;
    }
}
