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

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.StatementResult;
import com.mware.bigconnect.driver.async.StatementResultCursor;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.exceptions.NoSuchRecordException;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.util.Futures;
import com.mware.bigconnect.driver.summary.ResultSummary;

import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class InternalStatementResult implements StatementResult
{
    private final Connection connection;
    private final StatementResultCursor cursor;
    private List<String> keys;

    public InternalStatementResult( Connection connection, StatementResultCursor cursor )
    {
        this.connection = connection;
        this.cursor = cursor;
    }

    @Override
    public List<String> keys()
    {
        if ( keys == null )
        {
            blockingGet( cursor.peekAsync() );
            keys = cursor.keys();
        }
        return keys;
    }

    @Override
    public boolean hasNext()
    {
        return blockingGet( cursor.peekAsync() ) != null;
    }

    @Override
    public Record next()
    {
        Record record = blockingGet( cursor.nextAsync() );
        if ( record == null )
        {
            throw new NoSuchRecordException( "No more records" );
        }
        return record;
    }

    @Override
    public Record single()
    {
        return blockingGet( cursor.singleAsync() );
    }

    @Override
    public Record peek()
    {
        Record record = blockingGet( cursor.peekAsync() );
        if ( record == null )
        {
            throw new NoSuchRecordException( "Cannot peek past the last record" );
        }
        return record;
    }

    @Override
    public Stream<Record> stream()
    {
        Spliterator<Record> spliterator = Spliterators.spliteratorUnknownSize( this, Spliterator.IMMUTABLE | Spliterator.ORDERED );
        return StreamSupport.stream( spliterator, false );
    }

    @Override
    public List<Record> list()
    {
        return blockingGet( cursor.listAsync() );
    }

    @Override
    public <T> List<T> list(Function<Record, T> mapFunction )
    {
        return blockingGet( cursor.listAsync( mapFunction ) );
    }

    @Override
    public ResultSummary consume()
    {
        return blockingGet( cursor.consumeAsync() );
    }

    @Override
    public ResultSummary summary()
    {
        return blockingGet( cursor.summaryAsync() );
    }

    @Override
    public void remove()
    {
        throw new ClientException( "Removing records from a result is not supported." );
    }

    private <T> T blockingGet( CompletionStage<T> stage )
    {
        return Futures.blockingGet( stage, this::terminateConnectionOnThreadInterrupt );
    }

    private void terminateConnectionOnThreadInterrupt()
    {
        connection.terminateAndRelease( "Thread interrupted while waiting for result to arrive" );
    }
}
