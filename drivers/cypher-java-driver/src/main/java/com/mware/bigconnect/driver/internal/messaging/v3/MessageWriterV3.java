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
package com.mware.bigconnect.driver.internal.messaging.v3;

import com.mware.bigconnect.driver.internal.messaging.AbstractMessageWriter;
import com.mware.bigconnect.driver.internal.messaging.MessageEncoder;
import com.mware.bigconnect.driver.internal.messaging.encode.*;
import com.mware.bigconnect.driver.internal.messaging.request.*;
import com.mware.bigconnect.driver.internal.messaging.v2.ValuePackerV2;
import com.mware.bigconnect.driver.internal.packstream.PackOutput;
import com.mware.bigconnect.driver.internal.util.Iterables;

import java.util.Map;

public class MessageWriterV3 extends AbstractMessageWriter
{
    public MessageWriterV3( PackOutput output )
    {
        super( new ValuePackerV2( output ), buildEncoders() );
    }

    private static Map<Byte,MessageEncoder> buildEncoders()
    {
        Map<Byte,MessageEncoder> result = Iterables.newHashMapWithSize( 9 );
        result.put( HelloMessage.SIGNATURE, new HelloMessageEncoder() );
        result.put( GoodbyeMessage.SIGNATURE, new GoodbyeMessageEncoder() );

        result.put( RunWithMetadataMessage.SIGNATURE, new RunWithMetadataMessageEncoder() );
        result.put( DiscardAllMessage.SIGNATURE, new DiscardAllMessageEncoder() );
        result.put( PullAllMessage.SIGNATURE, new PullAllMessageEncoder() );

        result.put( BeginMessage.SIGNATURE, new BeginMessageEncoder() );
        result.put( CommitMessage.SIGNATURE, new CommitMessageEncoder() );
        result.put( RollbackMessage.SIGNATURE, new RollbackMessageEncoder() );
        result.put( ResetMessage.SIGNATURE, new ResetMessageEncoder() );
        return result;
    }
}
