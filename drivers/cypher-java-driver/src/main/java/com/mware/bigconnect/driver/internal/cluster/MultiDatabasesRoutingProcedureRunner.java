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

import com.mware.bigconnect.driver.AccessMode;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.internal.BookmarkHolder;
import com.mware.bigconnect.driver.internal.InternalBookmark;
import com.mware.bigconnect.driver.internal.ReadOnlyBookmarkHolder;
import com.mware.bigconnect.driver.internal.async.connection.DirectConnection;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.util.ServerVersion;

import java.util.Objects;

import static com.mware.bigconnect.driver.Values.parameters;
import static com.mware.bigconnect.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static com.mware.bigconnect.driver.internal.messaging.request.MultiDatabaseUtil.SYSTEM_DB_NAME;

public class MultiDatabasesRoutingProcedureRunner extends RoutingProcedureRunner
{
    static final String DATABASE_NAME = "database";
    static final String MULTI_DB_GET_ROUTING_TABLE = String.format( "CALL dbms.routing.getRoutingTable($%s, $%s)", ROUTING_CONTEXT, DATABASE_NAME );

    public MultiDatabasesRoutingProcedureRunner( RoutingContext context )
    {
        super( context );
    }

    @Override
    BookmarkHolder bookmarkHolder( InternalBookmark bookmark )
    {
        return new ReadOnlyBookmarkHolder( bookmark );
    }

    @Override
    Statement procedureStatement( ServerVersion serverVersion, String databaseName )
    {
        if ( Objects.equals( ABSENT_DB_NAME, databaseName ) )
        {
            databaseName = null;
        }
        return new Statement( MULTI_DB_GET_ROUTING_TABLE, parameters( ROUTING_CONTEXT, context.asMap(), DATABASE_NAME, databaseName ) );
    }

    @Override
    DirectConnection connection( Connection connection )
    {
        return new DirectConnection( connection, SYSTEM_DB_NAME, AccessMode.READ );
    }
}
