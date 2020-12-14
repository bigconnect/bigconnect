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
package com.mware.bigconnect.driver.internal.messaging.v4;

import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.internal.BookmarkHolder;
import com.mware.bigconnect.driver.internal.async.ExplicitTransaction;
import com.mware.bigconnect.driver.internal.cursor.InternalStatementResultCursorFactory;
import com.mware.bigconnect.driver.internal.cursor.StatementResultCursorFactory;
import com.mware.bigconnect.driver.internal.handlers.AbstractPullAllResponseHandler;
import com.mware.bigconnect.driver.internal.handlers.RunResponseHandler;
import com.mware.bigconnect.driver.internal.handlers.pulln.BasicPullResponseHandler;
import com.mware.bigconnect.driver.internal.messaging.BoltProtocol;
import com.mware.bigconnect.driver.internal.messaging.MessageFormat;
import com.mware.bigconnect.driver.internal.messaging.request.RunWithMetadataMessage;
import com.mware.bigconnect.driver.internal.messaging.v3.BoltProtocolV3;
import com.mware.bigconnect.driver.internal.spi.Connection;

import static com.mware.bigconnect.driver.internal.handlers.PullHandlers.newBoltV3PullAllHandler;
import static com.mware.bigconnect.driver.internal.handlers.PullHandlers.newBoltV4PullHandler;

public class BoltProtocolV4 extends BoltProtocolV3
{
    public static final int VERSION = 4;
    public static final BoltProtocol INSTANCE = new BoltProtocolV4();

    @Override
    public MessageFormat createMessageFormat()
    {
        return new MessageFormatV4();
    }

    @Override
    protected StatementResultCursorFactory buildResultCursorFactory( Connection connection, Statement statement, BookmarkHolder bookmarkHolder,
            ExplicitTransaction tx, RunWithMetadataMessage runMessage, boolean waitForRunResponse )
    {
        RunResponseHandler runHandler = new RunResponseHandler( METADATA_EXTRACTOR );

        AbstractPullAllResponseHandler pullAllHandler = newBoltV3PullAllHandler( statement, runHandler, connection, bookmarkHolder, tx );
        BasicPullResponseHandler pullHandler = newBoltV4PullHandler( statement, runHandler, connection, bookmarkHolder, tx );

        return new InternalStatementResultCursorFactory( connection, runMessage, runHandler, pullHandler, pullAllHandler, waitForRunResponse );
    }

    protected void verifyDatabaseNameBeforeTransaction( String databaseName )
    {
        // Bolt V4 accepts database name
    }

    @Override
    public int version()
    {
        return VERSION;
    }
}
