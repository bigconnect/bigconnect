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
import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.TransactionConfig;
import com.mware.bigconnect.driver.async.StatementResultCursor;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.exceptions.FatalDiscoveryException;
import com.mware.bigconnect.driver.internal.BookmarkHolder;
import com.mware.bigconnect.driver.internal.InternalBookmark;
import com.mware.bigconnect.driver.internal.async.connection.DirectConnection;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.util.Futures;
import com.mware.bigconnect.driver.internal.util.ServerVersion;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static com.mware.bigconnect.driver.Values.parameters;
import static com.mware.bigconnect.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

public class RoutingProcedureRunner
{
    static final String ROUTING_CONTEXT = "context";
    static final String GET_ROUTING_TABLE = "CALL dbms.cluster.routing.getRoutingTable($" + ROUTING_CONTEXT + ")";

    final RoutingContext context;

    public RoutingProcedureRunner( RoutingContext context )
    {
        this.context = context;
    }

    public CompletionStage<RoutingProcedureResponse> run(Connection connection, String databaseName, InternalBookmark bookmark )
    {
        DirectConnection delegate = connection( connection );
        Statement procedure = procedureStatement( connection.serverVersion(), databaseName );
        BookmarkHolder bookmarkHolder = bookmarkHolder( bookmark );
        return runProcedure( delegate, procedure, bookmarkHolder )
                .thenCompose( records -> releaseConnection( delegate, records ) )
                .handle( ( records, error ) -> processProcedureResponse( procedure, records, error ) );
    }

    DirectConnection connection( Connection connection )
    {
        return new DirectConnection( connection, ABSENT_DB_NAME, AccessMode.WRITE );
    }

    Statement procedureStatement( ServerVersion serverVersion, String databaseName )
    {
        if ( !Objects.equals( ABSENT_DB_NAME, databaseName ) )
        {
            throw new FatalDiscoveryException( String.format(
                    "Refreshing routing table for multi-databases is not supported in server version lower than 4.0. " +
                            "Current server version: %s. Database name: `%s`", serverVersion, databaseName ) );
        }
        return new Statement( GET_ROUTING_TABLE, parameters( ROUTING_CONTEXT, context.asMap() ) );
    }

    BookmarkHolder bookmarkHolder( InternalBookmark ignored )
    {
        return BookmarkHolder.NO_OP;
    }

    CompletionStage<List<Record>> runProcedure(Connection connection, Statement procedure, BookmarkHolder bookmarkHolder )
    {
        return connection.protocol()
                .runInAutoCommitTransaction( connection, procedure, bookmarkHolder, TransactionConfig.empty(), true )
                .asyncResult().thenCompose( StatementResultCursor::listAsync );
    }

    private CompletionStage<List<Record>> releaseConnection(Connection connection, List<Record> records )
    {
        // It is not strictly required to release connection after routing procedure invocation because it'll
        // be released by the PULL_ALL response handler after result is fully fetched. Such release will happen
        // in background. However, releasing it early as part of whole chain makes it easier to reason about
        // rediscovery in stub server tests. Some of them assume connections to instances not present in new
        // routing table will be closed immediately.
        return connection.release().thenApply( ignore -> records );
    }

    private static RoutingProcedureResponse processProcedureResponse( Statement procedure, List<Record> records,
            Throwable error )
    {
        Throwable cause = Futures.completionExceptionCause( error );
        if ( cause != null )
        {
            return handleError( procedure, cause );
        }
        else
        {
            return new RoutingProcedureResponse( procedure, records );
        }
    }

    private static RoutingProcedureResponse handleError( Statement procedure, Throwable error )
    {
        if ( error instanceof ClientException )
        {
            return new RoutingProcedureResponse( procedure, error );
        }
        else
        {
            throw new CompletionException( error );
        }
    }
}
