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

import com.mware.bigconnect.driver.internal.async.ConnectionContext;
import com.mware.bigconnect.driver.internal.async.connection.DirectConnection;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.spi.ConnectionPool;
import com.mware.bigconnect.driver.internal.spi.ConnectionProvider;

import java.util.concurrent.CompletionStage;

import static com.mware.bigconnect.driver.internal.async.ImmutableConnectionContext.simple;

/**
 * Simple {@link ConnectionProvider connection provider} that obtains connections form the given pool only for
 * the given address.
 */
public class DirectConnectionProvider implements ConnectionProvider
{
    private final BoltServerAddress address;
    private final ConnectionPool connectionPool;

    DirectConnectionProvider( BoltServerAddress address, ConnectionPool connectionPool )
    {
        this.address = address;
        this.connectionPool = connectionPool;
    }

    @Override
    public CompletionStage<Connection> acquireConnection(ConnectionContext context )
    {
        return connectionPool.acquire( address ).thenApply( connection -> new DirectConnection( connection, context.databaseName(), context.mode() ) );
    }

    @Override
    public CompletionStage<Void> verifyConnectivity()
    {
        // We verify the connection by establishing a connection with the remote server specified by the address.
        // Connection context will be ignored as no query is run in this connection and the connection is released immediately.
        return acquireConnection( simple() ).thenCompose( Connection::release );
    }

    @Override
    public CompletionStage<Void> close()
    {
        return connectionPool.close();
    }

    public BoltServerAddress getAddress()
    {
        return address;
    }
}
