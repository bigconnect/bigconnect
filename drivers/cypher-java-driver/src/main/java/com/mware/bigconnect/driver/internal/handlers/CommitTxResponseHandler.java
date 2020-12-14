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
import com.mware.bigconnect.driver.internal.InternalBookmark;
import com.mware.bigconnect.driver.internal.spi.ResponseHandler;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class CommitTxResponseHandler implements ResponseHandler
{
    private final CompletableFuture<InternalBookmark> commitFuture;

    public CommitTxResponseHandler( CompletableFuture<InternalBookmark> commitFuture )
    {
        this.commitFuture = requireNonNull( commitFuture );
    }

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        Value bookmarkValue = metadata.get( "bookmark" );
        if ( bookmarkValue == null )
        {
            commitFuture.complete( null );
        }
        else
        {
            commitFuture.complete( InternalBookmark.parse( bookmarkValue.asString() ) );
        }
    }

    @Override
    public void onFailure( Throwable error )
    {
        commitFuture.completeExceptionally( error );
    }

    @Override
    public void onRecord( Value[] fields )
    {
        throw new UnsupportedOperationException(
                "Transaction commit is not expected to receive records: " + Arrays.toString( fields ) );
    }
}
