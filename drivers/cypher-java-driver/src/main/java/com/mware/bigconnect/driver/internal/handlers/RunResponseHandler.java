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

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.spi.ResponseHandler;
import com.mware.bigconnect.driver.internal.util.MetadataExtractor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyList;

public class RunResponseHandler implements ResponseHandler
{
    private final CompletableFuture<Throwable> runCompletedFuture;
    private final MetadataExtractor metadataExtractor;
    private long statementId = MetadataExtractor.ABSENT_QUERY_ID;

    private List<String> statementKeys = emptyList();
    private long resultAvailableAfter = -1;

    public RunResponseHandler( MetadataExtractor metadataExtractor )
    {
        this( new CompletableFuture<>(), metadataExtractor );
    }

    public RunResponseHandler(CompletableFuture<Throwable> runCompletedFuture, MetadataExtractor metadataExtractor )
    {
        this.runCompletedFuture = runCompletedFuture;
        this.metadataExtractor = metadataExtractor;
    }

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        statementKeys = metadataExtractor.extractStatementKeys( metadata );
        resultAvailableAfter = metadataExtractor.extractResultAvailableAfter( metadata );
        statementId = metadataExtractor.extractQueryId( metadata );

        completeRunFuture( null );
    }

    @Override
    public void onFailure( Throwable error )
    {
        completeRunFuture( error );
    }

    @Override
    public void onRecord( Value[] fields )
    {
        throw new UnsupportedOperationException();
    }

    public List<String> statementKeys()
    {
        return statementKeys;
    }

    public long resultAvailableAfter()
    {
        return resultAvailableAfter;
    }

    public long statementId()
    {
        return statementId;
    }

    /**
     * Complete the given future with error if the future was failed.
     * Future is never completed exceptionally.
     * Async API needs to wait for RUN because it needs to access statement keys.
     * Reactive API needs to know if RUN failed by checking the error.
     */
    private void completeRunFuture( Throwable error )
    {
        runCompletedFuture.complete( error );
    }

    public CompletableFuture<Throwable> runFuture()
    {
        return runCompletedFuture;
    }
}
