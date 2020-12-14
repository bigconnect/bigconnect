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
package com.mware.bigconnect.driver.internal.async.connection;

import com.mware.bigconnect.driver.AccessMode;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.RoutingErrorHandler;
import com.mware.bigconnect.driver.internal.handlers.RoutingResponseHandler;
import com.mware.bigconnect.driver.internal.messaging.BoltProtocol;
import com.mware.bigconnect.driver.internal.messaging.Message;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.spi.ResponseHandler;
import com.mware.bigconnect.driver.internal.util.ServerVersion;

import java.util.concurrent.CompletionStage;

/**
 * A connection used by the routing driver.
 */
public class RoutingConnection implements Connection
{
    private final Connection delegate;
    private final AccessMode accessMode;
    private final RoutingErrorHandler errorHandler;
    private final String databaseName;

    public RoutingConnection(Connection delegate, String databaseName, AccessMode accessMode, RoutingErrorHandler errorHandler )
    {
        this.delegate = delegate;
        this.databaseName = databaseName;
        this.accessMode = accessMode;
        this.errorHandler = errorHandler;
    }

    @Override
    public void enableAutoRead()
    {
        delegate.enableAutoRead();
    }

    @Override
    public void disableAutoRead()
    {
        delegate.disableAutoRead();
    }

    @Override
    public void write( Message message, ResponseHandler handler )
    {
        delegate.write( message, newRoutingResponseHandler( handler ) );
    }

    @Override
    public void write( Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2 )
    {
        delegate.write( message1, newRoutingResponseHandler( handler1 ), message2, newRoutingResponseHandler( handler2 ) );
    }

    @Override
    public void writeAndFlush( Message message, ResponseHandler handler )
    {
        delegate.writeAndFlush( message, newRoutingResponseHandler( handler ) );
    }

    @Override
    public void writeAndFlush( Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2 )
    {
        delegate.writeAndFlush( message1, newRoutingResponseHandler( handler1 ), message2, newRoutingResponseHandler( handler2 ) );
    }

    @Override
    public CompletionStage<Void> reset()
    {
        return delegate.reset();
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    @Override
    public CompletionStage<Void> release()
    {
        return delegate.release();
    }

    @Override
    public void terminateAndRelease( String reason )
    {
        delegate.terminateAndRelease( reason );
    }

    @Override
    public BoltServerAddress serverAddress()
    {
        return delegate.serverAddress();
    }

    @Override
    public ServerVersion serverVersion()
    {
        return delegate.serverVersion();
    }

    @Override
    public BoltProtocol protocol()
    {
        return delegate.protocol();
    }

    @Override
    public void flush()
    {
        delegate.flush();
    }

    @Override
    public AccessMode mode()
    {
        return this.accessMode;
    }

    @Override
    public String databaseName()
    {
        return this.databaseName;
    }


    private RoutingResponseHandler newRoutingResponseHandler( ResponseHandler handler )
    {
        return new RoutingResponseHandler( handler, serverAddress(), accessMode, errorHandler );
    }
}
