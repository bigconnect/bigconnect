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
import com.mware.bigconnect.driver.internal.BookmarkHolder;
import com.mware.bigconnect.driver.internal.async.ExplicitTransaction;
import com.mware.bigconnect.driver.internal.handlers.pulln.BasicPullResponseHandler;
import com.mware.bigconnect.driver.internal.handlers.pulln.SessionPullResponseHandler;
import com.mware.bigconnect.driver.internal.handlers.pulln.TransactionPullResponseHandler;
import com.mware.bigconnect.driver.internal.messaging.v1.BoltProtocolV1;
import com.mware.bigconnect.driver.internal.messaging.v3.BoltProtocolV3;
import com.mware.bigconnect.driver.internal.spi.Connection;

public class PullHandlers
{
    public static AbstractPullAllResponseHandler newBoltV1PullAllHandler( Statement statement, RunResponseHandler runHandler,
            Connection connection, ExplicitTransaction tx )
    {
        if ( tx != null )
        {
            return new TransactionPullAllResponseHandler( statement, runHandler, connection, tx, BoltProtocolV1.METADATA_EXTRACTOR );
        }
        return new SessionPullAllResponseHandler( statement, runHandler, connection, BookmarkHolder.NO_OP, BoltProtocolV1.METADATA_EXTRACTOR );
    }

    public static AbstractPullAllResponseHandler newBoltV3PullAllHandler( Statement statement, RunResponseHandler runHandler, Connection connection,
            BookmarkHolder bookmarkHolder, ExplicitTransaction tx )
    {
        if ( tx != null )
        {
            return new TransactionPullAllResponseHandler( statement, runHandler, connection, tx, BoltProtocolV3.METADATA_EXTRACTOR );
        }
        return new SessionPullAllResponseHandler( statement, runHandler, connection, bookmarkHolder, BoltProtocolV3.METADATA_EXTRACTOR );
    }

    public static BasicPullResponseHandler newBoltV4PullHandler( Statement statement, RunResponseHandler runHandler, Connection connection,
            BookmarkHolder bookmarkHolder, ExplicitTransaction tx )
    {
        if ( tx != null )
        {
            return new TransactionPullResponseHandler( statement, runHandler, connection, tx, BoltProtocolV3.METADATA_EXTRACTOR );
        }
        return new SessionPullResponseHandler( statement, runHandler, connection, bookmarkHolder, BoltProtocolV3.METADATA_EXTRACTOR );
    }

}
