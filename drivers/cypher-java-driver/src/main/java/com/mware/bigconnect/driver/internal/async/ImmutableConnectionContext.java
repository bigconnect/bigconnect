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
package com.mware.bigconnect.driver.internal.async;

import com.mware.bigconnect.driver.AccessMode;
import com.mware.bigconnect.driver.internal.InternalBookmark;
import com.mware.bigconnect.driver.internal.spi.Connection;

import static com.mware.bigconnect.driver.internal.InternalBookmark.empty;
import static com.mware.bigconnect.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

/**
 * A {@link Connection} shall fulfil this {@link ImmutableConnectionContext} when acquired from a connection provider.
 */
public class ImmutableConnectionContext implements ConnectionContext
{
    private static final ConnectionContext SIMPLE = new ImmutableConnectionContext( ABSENT_DB_NAME, empty(), AccessMode.READ );

    private final String databaseName;
    private final AccessMode mode;
    private final InternalBookmark rediscoveryBookmark;

    public ImmutableConnectionContext(String databaseName, InternalBookmark bookmark, AccessMode mode )
    {
        this.databaseName = databaseName;
        this.rediscoveryBookmark = bookmark;
        this.mode = mode;
    }

    @Override
    public String databaseName()
    {
        return databaseName;
    }

    @Override
    public AccessMode mode()
    {
        return mode;
    }

    @Override
    public InternalBookmark rediscoveryBookmark()
    {
        return rediscoveryBookmark;
    }

    /**
     * A simple context is used to test connectivity with a remote server/cluster.
     * As long as there is a read only service, the connection shall be established successfully.
     * This context should be applicable for both bolt v4 and bolt v3 routing table rediscovery.
     */
    public static ConnectionContext simple()
    {
        return SIMPLE;
    }
}
