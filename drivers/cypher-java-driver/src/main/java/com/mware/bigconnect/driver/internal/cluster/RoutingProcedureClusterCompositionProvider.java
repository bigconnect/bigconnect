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
import com.mware.bigconnect.driver.exceptions.ProtocolException;
import com.mware.bigconnect.driver.exceptions.value.ValueException;
import com.mware.bigconnect.driver.internal.InternalBookmark;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.util.Clock;
import com.mware.bigconnect.driver.internal.util.ServerVersion;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static java.lang.String.format;

public class RoutingProcedureClusterCompositionProvider implements ClusterCompositionProvider
{
    private static final String PROTOCOL_ERROR_MESSAGE = "Failed to parse '%s' result received from server due to ";

    private final Clock clock;
    private final RoutingProcedureRunner routingProcedureRunner;
    private final RoutingProcedureRunner multiDatabaseRoutingProcedureRunner;

    public RoutingProcedureClusterCompositionProvider( Clock clock, RoutingContext routingContext )
    {
        this( clock, new RoutingProcedureRunner( routingContext ), new MultiDatabasesRoutingProcedureRunner( routingContext ) );
    }

    RoutingProcedureClusterCompositionProvider( Clock clock, RoutingProcedureRunner routingProcedureRunner,
            MultiDatabasesRoutingProcedureRunner multiDatabaseRoutingProcedureRunner )
    {
        this.clock = clock;
        this.routingProcedureRunner = routingProcedureRunner;
        this.multiDatabaseRoutingProcedureRunner = multiDatabaseRoutingProcedureRunner;
    }

    @Override
    public CompletionStage<ClusterComposition> getClusterComposition(Connection connection, String databaseName, InternalBookmark bookmark )
    {
        RoutingProcedureRunner runner;
        if ( connection.serverVersion().greaterThanOrEqual( ServerVersion.v4_0_0 ) )
        {
            runner = multiDatabaseRoutingProcedureRunner;
        }
        else
        {
            runner = routingProcedureRunner;
        }

        return runner.run( connection, databaseName, bookmark )
                .thenApply( this::processRoutingResponse );
    }

    private ClusterComposition processRoutingResponse( RoutingProcedureResponse response )
    {
        if ( !response.isSuccess() )
        {
            throw new CompletionException( format(
                    "Failed to run '%s' on server. Please make sure that there is a BigConnect server or cluster up running.",
                    invokedProcedureString( response ) ), response.error() );
        }

        List<Record> records = response.records();

        long now = clock.millis();

        // the record size is wrong
        if ( records.size() != 1 )
        {
            throw new ProtocolException( format(
                    PROTOCOL_ERROR_MESSAGE + "records received '%s' is too few or too many.",
                    invokedProcedureString( response ), records.size() ) );
        }

        // failed to parse the record
        ClusterComposition cluster;
        try
        {
            cluster = ClusterComposition.parse( records.get( 0 ), now );
        }
        catch ( ValueException e )
        {
            throw new ProtocolException( format(
                    PROTOCOL_ERROR_MESSAGE + "unparsable record received.",
                    invokedProcedureString( response ) ), e );
        }

        // the cluster result is not a legal reply
        if ( !cluster.hasRoutersAndReaders() )
        {
            throw new ProtocolException( format(
                    PROTOCOL_ERROR_MESSAGE + "no router or reader found in response.",
                    invokedProcedureString( response ) ) );
        }

        // all good
        return cluster;
    }

    private static String invokedProcedureString(RoutingProcedureResponse response )
    {
        Statement statement = response.procedure();
        return statement.text() + " " + statement.parameters();
    }
}
