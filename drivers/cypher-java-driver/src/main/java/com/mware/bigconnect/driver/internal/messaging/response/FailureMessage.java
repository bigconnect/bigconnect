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

import static java.lang.String.format;

/**
 * FAILURE response message
 * <p>
 * Sent by the server to signal a failed operation.
 * Terminates response sequence.
 */
public class FailureMessage implements Message
{
    public final static byte SIGNATURE = 0x7F;

    private final String code;
    private final String message;

    public FailureMessage(String code, String message )
    {
        super();
        this.code = code;
        this.message = message;
    }

    public String code()
    {
        return code;
    }

    public String message()
    {
        return message;
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }

    @Override
    public String toString()
    {
        return format( "FAILURE %s \"%s\"", code, message );
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

        FailureMessage that = (FailureMessage) o;

        return !(code != null ? !code.equals( that.code ) : that.code != null) &&
               !(message != null ? !message.equals( that.message ) : that.message != null);

    }

    @Override
    public int hashCode()
    {
        int result = code != null ? code.hashCode() : 0;
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }
}
