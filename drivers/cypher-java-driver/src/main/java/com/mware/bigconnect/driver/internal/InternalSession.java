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
import com.mware.bigconnect.driver.async.StatementResultCursor;
import com.mware.bigconnect.driver.internal.async.ExplicitTransaction;
import com.mware.bigconnect.driver.internal.async.NetworkSession;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.util.Futures;

import java.util.Map;

import static java.util.Collections.emptyMap;

public class InternalSession extends AbstractStatementRunner implements Session
{
    private final NetworkSession session;

    public InternalSession( NetworkSession session )
    {
        this.session = session;
    }

    @Override
    public StatementResult run( Statement statement )
    {
        return run( statement, TransactionConfig.empty() );
    }

    @Override
    public StatementResult run(String statement, TransactionConfig config )
    {
        return run( statement, emptyMap(), config );
    }

    @Override
    public StatementResult run(String statement, Map<String, Object> parameters, TransactionConfig config )
    {
        return run( new Statement( statement, parameters ), config );
    }

    @Override
    public StatementResult run( Statement statement, TransactionConfig config )
    {
        StatementResultCursor cursor = Futures.blockingGet( session.runAsync( statement, config, false ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while running query in session" ) );

        // query executed, it is safe to obtain a connection in a blocking way
        Connection connection = Futures.getNow( session.connectionAsync() );
        return new InternalStatementResult( connection, cursor );
    }

    @Override
    public boolean isOpen()
    {
        return session.isOpen();
    }

    @Override
    public void close()
    {
        Futures.blockingGet( session.closeAsync(), () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while closing the session" ) );
    }

    @Override
    public Transaction beginTransaction()
    {
        return beginTransaction( TransactionConfig.empty() );
    }

    @Override
    public Transaction beginTransaction( TransactionConfig config )
    {
        ExplicitTransaction tx = Futures.blockingGet( session.beginTransactionAsync( config ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while starting a transaction" ) );
        return new InternalTransaction( tx );
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work )
    {
        return readTransaction( work, TransactionConfig.empty() );
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work, TransactionConfig config )
    {
        return transaction( AccessMode.READ, work, config );
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work )
    {
        return writeTransaction( work, TransactionConfig.empty() );
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work, TransactionConfig config )
    {
        return transaction( AccessMode.WRITE, work, config );
    }

    @Override
    public Bookmark lastBookmark()
    {
        return session.lastBookmark();
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public void reset()
    {
        Futures.blockingGet( session.resetAsync(), () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while resetting the session" ) );
    }

    private <T> T transaction( AccessMode mode, TransactionWork<T> work, TransactionConfig config )
    {
        // use different code path compared to async so that work is executed in the caller thread
        // caller thread will also be the one who sleeps between retries;
        // it is unsafe to execute retries in the event loop threads because this can cause a deadlock
        // event loop thread will bock and wait for itself to read some data
        return session.retryLogic().retry( () -> {
            try ( Transaction tx = beginTransaction( mode, config ) )
            {

                T result = work.execute( tx );
                if ( tx.isOpen() )
                {
                    // commit tx if a user has not explicitly committed or rolled back the transaction
                    tx.commit();
                }
                return result;
            }
        } );
    }

    private Transaction beginTransaction( AccessMode mode, TransactionConfig config )
    {
        ExplicitTransaction tx = Futures.blockingGet( session.beginTransactionAsync( mode, config ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while starting a transaction" ) );
        return new InternalTransaction( tx );
    }

    private void terminateConnectionOnThreadInterrupt( String reason )
    {
        // try to get current connection if it has been acquired
        Connection connection = null;
        try
        {
            connection = Futures.getNow( session.connectionAsync() );
        }
        catch ( Throwable ignore )
        {
            // ignore errors because handing interruptions is best effort
        }

        if ( connection != null )
        {
            connection.terminateAndRelease( reason );
        }
    }
}
