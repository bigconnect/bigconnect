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

import com.mware.bigconnect.driver.AccessMode;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.messaging.BoltProtocol;
import com.mware.bigconnect.driver.internal.messaging.Message;
import com.mware.bigconnect.driver.internal.util.ServerVersion;

import java.util.concurrent.CompletionStage;

import static java.lang.String.format;

public interface Connection
{
    boolean isOpen();

    void enableAutoRead();

    void disableAutoRead();

    void write(Message message, ResponseHandler handler);

    void write(Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2);

    void writeAndFlush(Message message, ResponseHandler handler);

    void writeAndFlush(Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2);

    CompletionStage<Void> reset();

    CompletionStage<Void> release();

    void terminateAndRelease(String reason);

    BoltServerAddress serverAddress();

    ServerVersion serverVersion();

    BoltProtocol protocol();

    default AccessMode mode()
    {
        throw new UnsupportedOperationException( format( "%s does not support access mode.", getClass() ) );
    }

    default String databaseName()
    {
        throw new UnsupportedOperationException( format( "%s does not support database name.", getClass() ) );
    }

    void flush();
}
