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

import com.mware.bigconnect.driver.AccessMode;
import com.mware.bigconnect.driver.Config;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.SessionConfig;
import com.mware.bigconnect.driver.internal.async.LeakLoggingNetworkSession;
import com.mware.bigconnect.driver.internal.async.NetworkSession;
import com.mware.bigconnect.driver.internal.retry.RetryLogic;
import com.mware.bigconnect.driver.internal.spi.ConnectionProvider;

import java.util.concurrent.CompletionStage;

import static com.mware.bigconnect.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

public class SessionFactoryImpl implements SessionFactory
{
    private final ConnectionProvider connectionProvider;
    private final RetryLogic retryLogic;
    private final Logging logging;
    private final boolean leakedSessionsLoggingEnabled;

    SessionFactoryImpl( ConnectionProvider connectionProvider, RetryLogic retryLogic, Config config )
    {
        this.connectionProvider = connectionProvider;
        this.leakedSessionsLoggingEnabled = config.logLeakedSessions();
        this.retryLogic = retryLogic;
        this.logging = config.logging();
    }

    @Override
    public NetworkSession newInstance( SessionConfig sessionConfig )
    {
        BookmarkHolder bookmarkHolder = new DefaultBookmarkHolder( InternalBookmark.from( sessionConfig.bookmarks() ) );
        return createSession( connectionProvider, retryLogic, sessionConfig.database().orElse( ABSENT_DB_NAME ),
                sessionConfig.defaultAccessMode(), bookmarkHolder, logging );
    }

    @Override
    public CompletionStage<Void> verifyConnectivity()
    {
        return connectionProvider.verifyConnectivity();
    }

    @Override
    public CompletionStage<Void> close()
    {
        return connectionProvider.close();
    }

    /**
     * Get the underlying connection provider.
     * <p>
     * <b>This method is only for testing</b>
     *
     * @return the connection provider used by this factory.
     */
    public ConnectionProvider getConnectionProvider()
    {
        return connectionProvider;
    }

    private NetworkSession createSession(ConnectionProvider connectionProvider, RetryLogic retryLogic, String databaseName, AccessMode mode,
                                         BookmarkHolder bookmarkHolder, Logging logging )
    {
        return leakedSessionsLoggingEnabled
               ? new LeakLoggingNetworkSession( connectionProvider, retryLogic, databaseName, mode, bookmarkHolder, logging )
               : new NetworkSession( connectionProvider, retryLogic, databaseName, mode, bookmarkHolder, logging );
    }
}
