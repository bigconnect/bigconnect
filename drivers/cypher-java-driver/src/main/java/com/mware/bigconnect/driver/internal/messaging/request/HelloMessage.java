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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.mware.bigconnect.driver.Values.value;
import static com.mware.bigconnect.driver.internal.security.InternalAuthToken.CREDENTIALS_KEY;

public class HelloMessage extends MessageWithMetadata
{
    public final static byte SIGNATURE = 0x01;

    private static final String USER_AGENT_METADATA_KEY = "user_agent";

    public HelloMessage(String userAgent, Map<String,Value> authToken )
    {
        super( buildMetadata( userAgent, authToken ) );
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        HelloMessage that = (HelloMessage) o;
        return Objects.equals( metadata(), that.metadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( metadata() );
    }

    @Override
    public String toString()
    {
        Map<String,Value> metadataCopy = new HashMap<>( metadata() );
        metadataCopy.replace( CREDENTIALS_KEY, value( "******" ) );
        return "HELLO " + metadataCopy;
    }

    private static Map<String,Value> buildMetadata(String userAgent, Map<String,Value> authToken )
    {
        Map<String,Value> result = new HashMap<>( authToken );
        result.put( USER_AGENT_METADATA_KEY, value( userAgent ) );
        return result;
    }
}
