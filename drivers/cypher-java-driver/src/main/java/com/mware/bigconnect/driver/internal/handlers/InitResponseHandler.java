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
package com.mware.bigconnect.driver.internal.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.spi.ResponseHandler;
import com.mware.bigconnect.driver.internal.util.ServerVersion;

import java.util.Map;

import static com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes.setServerVersion;
import static com.mware.bigconnect.driver.internal.util.MetadataExtractor.extractBigConnectServerVersion;

public class InitResponseHandler implements ResponseHandler
{
    private final ChannelPromise connectionInitializedPromise;
    private final Channel channel;

    public InitResponseHandler( ChannelPromise connectionInitializedPromise )
    {
        this.connectionInitializedPromise = connectionInitializedPromise;
        this.channel = connectionInitializedPromise.channel();
    }

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        try
        {
            ServerVersion serverVersion = extractBigConnectServerVersion( metadata );
            setServerVersion( channel, serverVersion );
            connectionInitializedPromise.setSuccess();
        }
        catch ( Throwable error )
        {
            connectionInitializedPromise.setFailure( error );
            throw error;
        }
    }

    @Override
    public void onFailure( Throwable error )
    {
        channel.close().addListener( future -> connectionInitializedPromise.setFailure( error ) );
    }

    @Override
    public void onRecord( Value[] fields )
    {
        throw new UnsupportedOperationException();
    }
}
