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
package com.mware.bigconnect.driver.internal.messaging.request;

import com.mware.bigconnect.driver.internal.messaging.Message;

/**
 * RESET request message
 * <p>
 * Sent by clients to reset a session to a clean state - closing any open transaction or result streams.
 * This also acknowledges receipt of failures sent by the server. This is required to
 * allow optimistic sending of multiple messages before responses have been received - pipelining.
 * <p>
 * When something goes wrong, we want the server to stop processing our already sent messages,
 * but the server cannot tell the difference between what was sent before and after we saw the
 * error.
 * <p>
 * This message acts as a barrier after an error, informing the server that we've seen the error
 * message, and that messages that follow this one are safe to execute.
 */
public class ResetMessage implements Message
{
    public static final byte SIGNATURE = 0x0F;

    public static final ResetMessage RESET = new ResetMessage();

    private ResetMessage()
    {
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }

    @Override
    public String toString()
    {
        return "RESET";
    }
}
