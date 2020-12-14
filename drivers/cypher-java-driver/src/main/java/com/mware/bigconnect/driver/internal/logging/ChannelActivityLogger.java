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
package com.mware.bigconnect.driver.internal.logging;

import io.netty.channel.Channel;
import com.mware.bigconnect.driver.Logger;
import com.mware.bigconnect.driver.Logging;
import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes;

import static com.mware.bigconnect.driver.internal.util.Format.valueOrEmpty;
import static java.lang.String.format;

public class ChannelActivityLogger extends ReformattedLogger
{
    private final Channel channel;
    private final String localChannelId;

    private String dbConnectionId;
    private String serverAddress;

    public ChannelActivityLogger( Channel channel, Logging logging, Class<?> owner )
    {
        this( channel, logging.getLog( owner.getSimpleName() ) );
    }

    private ChannelActivityLogger( Channel channel, Logger delegate )
    {
        super( delegate );
        this.channel = channel;
        this.localChannelId = channel != null ? channel.id().toString() : null;
    }

    @Override
    protected String reformat(String message )
    {
        if ( channel == null )
        {
            return message;
        }

        String dbConnectionId = getDbConnectionId();
        String serverAddress = getServerAddress();

        return format( "[0x%s][%s][%s] %s", localChannelId, valueOrEmpty( serverAddress ), valueOrEmpty( dbConnectionId ), message );
    }

    private String getDbConnectionId()
    {
        if ( dbConnectionId == null )
        {
            dbConnectionId = ChannelAttributes.connectionId( channel );
        }
        return dbConnectionId;
    }

    private String getServerAddress()
    {

        if ( serverAddress == null )
        {
            BoltServerAddress serverAddress = ChannelAttributes.serverAddress( channel );
            this.serverAddress = serverAddress != null ? serverAddress.toString() : null;
        }

        return serverAddress;
    }
}
