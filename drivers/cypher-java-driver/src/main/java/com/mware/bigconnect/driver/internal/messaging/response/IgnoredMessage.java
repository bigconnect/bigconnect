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
package com.mware.bigconnect.driver.internal.messaging.response;

import com.mware.bigconnect.driver.internal.messaging.Message;

/**
 * IGNORED response message
 * <p>
 * Sent by the server to signal that an operation has been ignored.
 * Terminates response sequence.
 */
public class IgnoredMessage implements Message
{
    public final static byte SIGNATURE = 0x7E;

    public static final IgnoredMessage IGNORED = new IgnoredMessage();

    private IgnoredMessage()
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
        return "IGNORED {}";
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && obj.getClass() == getClass();
    }

    @Override
    public int hashCode()
    {
        return 1;
    }
}
