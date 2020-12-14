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
package com.mware.bigconnect.driver.internal.messaging.v1;

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.messaging.MessageFormat;
import com.mware.bigconnect.driver.internal.messaging.ResponseMessageHandler;
import com.mware.bigconnect.driver.internal.messaging.ValueUnpacker;
import com.mware.bigconnect.driver.internal.messaging.response.FailureMessage;
import com.mware.bigconnect.driver.internal.messaging.response.IgnoredMessage;
import com.mware.bigconnect.driver.internal.messaging.response.RecordMessage;
import com.mware.bigconnect.driver.internal.messaging.response.SuccessMessage;
import com.mware.bigconnect.driver.internal.packstream.PackInput;

import java.io.IOException;
import java.util.Map;

public class MessageReaderV1 implements MessageFormat.Reader
{
    private final ValueUnpacker unpacker;

    public MessageReaderV1( PackInput input )
    {
        this( new ValueUnpackerV1( input ) );
    }

    protected MessageReaderV1( ValueUnpacker unpacker )
    {
        this.unpacker = unpacker;
    }

    @Override
    public void read( ResponseMessageHandler handler ) throws IOException
    {
        unpacker.unpackStructHeader();
        int type = unpacker.unpackStructSignature();
        switch ( type )
        {
        case SuccessMessage.SIGNATURE:
            unpackSuccessMessage( handler );
            break;
        case FailureMessage.SIGNATURE:
            unpackFailureMessage( handler );
            break;
        case IgnoredMessage.SIGNATURE:
            unpackIgnoredMessage( handler );
            break;
        case RecordMessage.SIGNATURE:
            unpackRecordMessage( handler );
            break;
        default:
            throw new IOException( "Unknown message type: " + type );
        }
    }

    private void unpackSuccessMessage( ResponseMessageHandler output ) throws IOException
    {
        Map<String,Value> map = unpacker.unpackMap();
        output.handleSuccessMessage( map );
    }

    private void unpackFailureMessage( ResponseMessageHandler output ) throws IOException
    {
        Map<String,Value> params = unpacker.unpackMap();
        String code = params.get( "code" ).asString();
        String message = params.get( "message" ).asString();
        output.handleFailureMessage( code, message );
    }

    private void unpackIgnoredMessage( ResponseMessageHandler output ) throws IOException
    {
        output.handleIgnoredMessage();
    }

    private void unpackRecordMessage( ResponseMessageHandler output ) throws IOException
    {
        Value[] fields = unpacker.unpackArray();
        output.handleRecordMessage( fields );
    }
}
