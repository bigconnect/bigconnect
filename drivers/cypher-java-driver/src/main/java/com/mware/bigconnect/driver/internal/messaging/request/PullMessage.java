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

import static com.mware.bigconnect.driver.internal.util.MetadataExtractor.ABSENT_QUERY_ID;

/**
 * PULL request message
 * <p>
 * Sent by clients to pull the entirety of the remaining stream down.
 */
public class PullMessage extends AbstractStreamingMessage
{
    public static final byte SIGNATURE = 0x3F;
    public static final PullMessage PULL_ALL = new PullMessage( STREAM_LIMIT_UNLIMITED, ABSENT_QUERY_ID );

    public PullMessage( long n, long id )
    {
        super( n, id );
    }

    @Override
    protected String name()
    {
        return "PULL";
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }
}
