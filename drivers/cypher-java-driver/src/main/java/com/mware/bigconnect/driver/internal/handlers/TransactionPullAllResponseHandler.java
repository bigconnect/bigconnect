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

import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.async.ExplicitTransaction;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.util.MetadataExtractor;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TransactionPullAllResponseHandler extends AbstractPullAllResponseHandler
{
    private final ExplicitTransaction tx;

    public TransactionPullAllResponseHandler( Statement statement, RunResponseHandler runResponseHandler,
            Connection connection, ExplicitTransaction tx, MetadataExtractor metadataExtractor )
    {
        super( statement, runResponseHandler, connection, metadataExtractor );
        this.tx = requireNonNull( tx );
    }

    @Override
    protected void afterSuccess( Map<String,Value> metadata )
    {
    }

    @Override
    protected void afterFailure( Throwable error )
    {
        // always mark transaction as terminated because every error is "acknowledged" with a RESET message
        // so database forgets about the transaction after the first error
        // such transaction should not attempt to commit and can be considered as rolled back
        tx.markTerminated();
    }
}
