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

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.messaging.Message;

import java.util.Map;

import static java.lang.String.format;

/**
 * INIT request message
 * <p>
 * Sent by clients to initialize a new connection. Must be sent as the very first message after protocol negotiation.
 */
public class InitMessage implements Message
{
    public final static byte SIGNATURE = 0x01;

    private final String userAgent;
    private Map<String,Value> authToken;

    public InitMessage(String userAgent, Map<String,Value> authToken )
    {
        this.userAgent = userAgent;
        this.authToken = authToken;
    }

    public String userAgent()
    {
        return userAgent;
    }

    public Map<String,Value> authToken()
    {
        return authToken;
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }

    @Override
    public String toString()
    {
        return format( "INIT \"%s\" {...}", userAgent );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        InitMessage that = (InitMessage) o;

        return !(userAgent != null ? !userAgent.equals( that.userAgent )
                                              : that.userAgent != null);

    }

    @Override
    public int hashCode()
    {
        return userAgent != null ? userAgent.hashCode() : 0;
    }
}
