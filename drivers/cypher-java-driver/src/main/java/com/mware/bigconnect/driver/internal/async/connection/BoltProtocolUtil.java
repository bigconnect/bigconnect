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
package com.mware.bigconnect.driver.internal.async.connection;

import io.netty.buffer.ByteBuf;
import com.mware.bigconnect.driver.internal.messaging.v1.BoltProtocolV1;
import com.mware.bigconnect.driver.internal.messaging.v2.BoltProtocolV2;
import com.mware.bigconnect.driver.internal.messaging.v3.BoltProtocolV3;
import com.mware.bigconnect.driver.internal.messaging.v4.BoltProtocolV4;

import static io.netty.buffer.Unpooled.copyInt;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static java.lang.Integer.toHexString;

public final class BoltProtocolUtil
{
    public static final int HTTP = 1213486160; //== 0x48545450 == "HTTP"

    public static final int BOLT_MAGIC_PREAMBLE = 0x6060B017;
    public static final int NO_PROTOCOL_VERSION = 0;

    public static final int CHUNK_HEADER_SIZE_BYTES = 2;

    public static final int DEFAULT_MAX_OUTBOUND_CHUNK_SIZE_BYTES = Short.MAX_VALUE / 2;

    private static final ByteBuf HANDSHAKE_BUF = unreleasableBuffer( copyInt(
            BOLT_MAGIC_PREAMBLE,
            BoltProtocolV4.VERSION,
            BoltProtocolV3.VERSION,
            BoltProtocolV2.VERSION,
            BoltProtocolV1.VERSION ) ).asReadOnly();

    private static final String HANDSHAKE_STRING = createHandshakeString();

    private BoltProtocolUtil()
    {
    }

    public static ByteBuf handshakeBuf()
    {
        return HANDSHAKE_BUF.duplicate();
    }

    public static String handshakeString()
    {
        return HANDSHAKE_STRING;
    }

    public static void writeMessageBoundary( ByteBuf buf )
    {
        buf.writeShort( 0 );
    }

    public static void writeEmptyChunkHeader( ByteBuf buf )
    {
        buf.writeShort( 0 );
    }

    public static void writeChunkHeader( ByteBuf buf, int chunkStartIndex, int headerValue )
    {
        buf.setShort( chunkStartIndex, headerValue );
    }

    private static String createHandshakeString()
    {
        ByteBuf buf = handshakeBuf();
        return String.format( "[0x%s, %s, %s, %s, %s]", toHexString( buf.readInt() ), buf.readInt(), buf.readInt(), buf.readInt(), buf.readInt() );
    }
}
