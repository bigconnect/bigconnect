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

import com.mware.bigconnect.driver.AccessMode;
import com.mware.bigconnect.driver.TransactionConfig;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.InternalBookmark;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import static com.mware.bigconnect.driver.internal.messaging.request.TransactionMetadataBuilder.buildMetadata;

public class BeginMessage extends MessageWithMetadata
{
    public static final byte SIGNATURE = 0x11;

    public BeginMessage(InternalBookmark bookmark, TransactionConfig config, String databaseName, AccessMode mode )
    {
        this( bookmark, config.timeout(), config.metadata(), mode, databaseName );
    }

    public BeginMessage(InternalBookmark bookmark, Duration txTimeout, Map<String,Value> txMetadata, AccessMode mode, String databaseName )
    {
        super( buildMetadata( txTimeout, txMetadata, databaseName, mode, bookmark ) );
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
        BeginMessage that = (BeginMessage) o;
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
        return "BEGIN " + metadata();
    }
}
