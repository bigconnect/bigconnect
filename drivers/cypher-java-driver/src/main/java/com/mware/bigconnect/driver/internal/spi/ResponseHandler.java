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
package com.mware.bigconnect.driver.internal.spi;

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.async.inbound.InboundMessageDispatcher;

import java.util.Map;

public interface ResponseHandler
{
    void onSuccess(Map<String, Value> metadata);

    void onFailure(Throwable error);

    void onRecord(Value[] fields);

    /**
     * Tells whether this response handler is able to manage auto-read of the underlying connection using {@link Connection#enableAutoRead()} and
     * {@link Connection#disableAutoRead()}.
     * <p>
     * Implementations can use auto-read management to apply network-level backpressure when receiving a stream of records.
     * There should only be a single such handler active for a connection at one point in time. Otherwise, handlers can interfere and turn on/off auto-read
     * racing with each other. {@link InboundMessageDispatcher} is responsible for tracking these handlers and disabling auto-read management to maintain just
     * a single auto-read managing handler per connection.
     */
    default boolean canManageAutoRead()
    {
        return false;
    }

    /**
     * If this response handler is able to manage auto-read of the underlying connection, then this method signals it to
     * stop changing auto-read setting for the connection.
     */
    default void disableAutoReadManagement()
    {

    }
}
