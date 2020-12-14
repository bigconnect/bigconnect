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
package com.mware.bigconnect.driver.internal.handlers;

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.InternalRecord;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.util.Futures;
import com.mware.bigconnect.driver.internal.util.Iterables;
import com.mware.bigconnect.driver.internal.util.MetadataExtractor;
import com.mware.bigconnect.driver.summary.ResultSummary;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static com.mware.bigconnect.driver.internal.util.Futures.completedWithNull;
import static com.mware.bigconnect.driver.internal.util.Futures.failedFuture;

public abstract class AbstractPullAllResponseHandler implements PullAllResponseHandler
{
    private static final Queue<Record> UNINITIALIZED_RECORDS = Iterables.emptyQueue();

    static final int RECORD_BUFFER_LOW_WATERMARK = Integer.getInteger( "recordBufferLowWatermark", 300 );
    static final int RECORD_BUFFER_HIGH_WATERMARK = Integer.getInteger( "recordBufferHighWatermark", 1000 );

    private final Statement statement;
    private final RunResponseHandler runResponseHandler;
    protected final MetadataExtractor metadataExtractor;
    protected final Connection connection;

    // initialized lazily when first record arrives
    private Queue<Record> records = UNINITIALIZED_RECORDS;

    private boolean autoReadManagementEnabled = true;
    private boolean finished;
    private Throwable failure;
    private ResultSummary summary;

    private boolean ignoreRecords;
    private CompletableFuture<Record> recordFuture;
    private CompletableFuture<Throwable> failureFuture;

    public AbstractPullAllResponseHandler( Statement statement, RunResponseHandler runResponseHandler, Connection connection, MetadataExtractor metadataExtractor )
    {
        this.statement = requireNonNull( statement );
        this.runResponseHandler = requireNonNull( runResponseHandler );
        this.metadataExtractor = requireNonNull( metadataExtractor );
        this.connection = requireNonNull( connection );
    }

    @Override
    public boolean canManageAutoRead()
    {
        return true;
    }

    @Override
    public synchronized void onSuccess( Map<String,Value> metadata )
    {
        finished = true;
        summary = extractResultSummary( metadata );

        afterSuccess( metadata );

        completeRecordFuture( null );
        completeFailureFuture( null );
    }

    protected abstract void afterSuccess( Map<String,Value> metadata );

    @Override
    public synchronized void onFailure( Throwable error )
    {
        finished = true;
        summary = extractResultSummary( emptyMap() );

        afterFailure( error );

        boolean failedRecordFuture = failRecordFuture( error );
        if ( failedRecordFuture )
        {
            // error propagated through the record future
            completeFailureFuture( null );
        }
        else
        {
            boolean completedFailureFuture = completeFailureFuture( error );
            if ( !completedFailureFuture )
            {
                // error has not been propagated to the user, remember it
                failure = error;
            }
        }
    }

    protected abstract void afterFailure( Throwable error );

    @Override
    public synchronized void onRecord( Value[] fields )
    {
        if ( ignoreRecords )
        {
            completeRecordFuture( null );
        }
        else
        {
            Record record = new InternalRecord( runResponseHandler.statementKeys(), fields );
            enqueueRecord( record );
            completeRecordFuture( record );
        }
    }

    @Override
    public synchronized void disableAutoReadManagement()
    {
        autoReadManagementEnabled = false;
    }

    public synchronized CompletionStage<Record> peekAsync()
    {
        Record record = records.peek();
        if ( record == null )
        {
            if ( failure != null )
            {
                return failedFuture( extractFailure() );
            }

            if ( ignoreRecords || finished )
            {
                return completedWithNull();
            }

            if ( recordFuture == null )
            {
                recordFuture = new CompletableFuture<>();
            }
            return recordFuture;
        }
        else
        {
            return completedFuture( record );
        }
    }

    public synchronized CompletionStage<Record> nextAsync()
    {
        return peekAsync().thenApply( ignore -> dequeueRecord() );
    }

    public synchronized CompletionStage<ResultSummary> summaryAsync()
    {
        return failureAsync().thenApply( error ->
        {
            if ( error != null )
            {
                throw Futures.asCompletionException( error );
            }
            return summary;
        } );
    }

    public synchronized CompletionStage<ResultSummary> consumeAsync()
    {
        ignoreRecords = true;
        records.clear();
        return summaryAsync();
    }

    public synchronized <T> CompletionStage<List<T>> listAsync(Function<Record,T> mapFunction )
    {
        return failureAsync().thenApply( error ->
        {
            if ( error != null )
            {
                throw Futures.asCompletionException( error );
            }
            return recordsAsList( mapFunction );
        } );
    }

    public synchronized CompletionStage<Throwable> failureAsync()
    {
        if ( failure != null )
        {
            return completedFuture( extractFailure() );
        }
        else if ( finished )
        {
            return completedWithNull();
        }
        else
        {
            if ( failureFuture == null )
            {
                // neither SUCCESS nor FAILURE message has arrived, register future to be notified when it arrives
                // future will be completed with null on SUCCESS and completed with Throwable on FAILURE
                // enable auto-read, otherwise we might not read SUCCESS/FAILURE if records are not consumed
                enableAutoRead();
                failureFuture = new CompletableFuture<>();
            }
            return failureFuture;
        }
    }

    private void enqueueRecord( Record record )
    {
        if ( records == UNINITIALIZED_RECORDS )
        {
            records = new ArrayDeque<>();
        }

        records.add( record );

        boolean shouldBufferAllRecords = failureFuture != null;
        // when failure is requested we have to buffer all remaining records and then return the error
        // do not disable auto-read in this case, otherwise records will not be consumed and trailing
        // SUCCESS or FAILURE message will not arrive as well, so callers will get stuck waiting for the error
        if ( !shouldBufferAllRecords && records.size() > RECORD_BUFFER_HIGH_WATERMARK )
        {
            // more than high watermark records are already queued, tell connection to stop auto-reading from network
            // this is needed to deal with slow consumers, we do not want to buffer all records in memory if they are
            // fetched from network faster than consumed
            disableAutoRead();
        }
    }

    private Record dequeueRecord()
    {
        Record record = records.poll();

        if ( records.size() < RECORD_BUFFER_LOW_WATERMARK )
        {
            // less than low watermark records are now available in the buffer, tell connection to pre-fetch more
            // and populate queue with new records from network
            enableAutoRead();
        }

        return record;
    }

    private <T> List<T> recordsAsList(Function<Record,T> mapFunction )
    {
        if ( !finished )
        {
            throw new IllegalStateException( "Can't get records as list because SUCCESS or FAILURE did not arrive" );
        }

        List<T> result = new ArrayList<>( records.size() );
        while ( !records.isEmpty() )
        {
            Record record = records.poll();
            result.add( mapFunction.apply( record ) );
        }
        return result;
    }

    private Throwable extractFailure()
    {
        if ( failure == null )
        {
            throw new IllegalStateException( "Can't extract failure because it does not exist" );
        }

        Throwable error = failure;
        failure = null; // propagate failure only once
        return error;
    }

    private void completeRecordFuture( Record record )
    {
        if ( recordFuture != null )
        {
            CompletableFuture<Record> future = recordFuture;
            recordFuture = null;
            future.complete( record );
        }
    }

    private boolean failRecordFuture( Throwable error )
    {
        if ( recordFuture != null )
        {
            CompletableFuture<Record> future = recordFuture;
            recordFuture = null;
            future.completeExceptionally( error );
            return true;
        }
        return false;
    }

    private boolean completeFailureFuture( Throwable error )
    {
        if ( failureFuture != null )
        {
            CompletableFuture<Throwable> future = failureFuture;
            failureFuture = null;
            future.complete( error );
            return true;
        }
        return false;
    }

    private ResultSummary extractResultSummary( Map<String,Value> metadata )
    {
        long resultAvailableAfter = runResponseHandler.resultAvailableAfter();
        return metadataExtractor.extractSummary( statement, connection, resultAvailableAfter, metadata );
    }

    private void enableAutoRead()
    {
        if ( autoReadManagementEnabled )
        {
            connection.enableAutoRead();
        }
    }

    private void disableAutoRead()
    {
        if ( autoReadManagementEnabled )
        {
            connection.disableAutoRead();
        }
    }
}
