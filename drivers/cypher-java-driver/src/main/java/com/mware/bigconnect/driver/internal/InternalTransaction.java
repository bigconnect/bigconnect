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

import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.StatementResult;
import com.mware.bigconnect.driver.Transaction;
import com.mware.bigconnect.driver.async.StatementResultCursor;
import com.mware.bigconnect.driver.internal.async.ExplicitTransaction;
import com.mware.bigconnect.driver.internal.util.Futures;

public class InternalTransaction extends AbstractStatementRunner implements Transaction
{
    private final ExplicitTransaction tx;
    public InternalTransaction( ExplicitTransaction tx )
    {
        this.tx = tx;
    }

    @Override
    public void commit()
    {
        Futures.blockingGet( tx.commitAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while committing the transaction" ) );
    }

    @Override
    public void rollback()
    {
        Futures.blockingGet( tx.rollbackAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while rolling back the transaction" ) );
    }

    @Override
    public void close()
    {
        Futures.blockingGet( tx.closeAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while closing the transaction" ) );
    }

    @Override
    public StatementResult run( Statement statement )
    {
        StatementResultCursor cursor = Futures.blockingGet( tx.runAsync( statement, false ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while running query in transaction" ) );
        return new InternalStatementResult( tx.connection(), cursor );
    }

    @Override
    public boolean isOpen()
    {
        return tx.isOpen();
    }

    private void terminateConnectionOnThreadInterrupt( String reason )
    {
        tx.connection().terminateAndRelease( reason );
    }
}
