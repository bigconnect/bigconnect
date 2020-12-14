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
package com.mware.bigconnect.driver.internal.messaging.v2;

import com.mware.bigconnect.driver.internal.messaging.v1.MessageFormatV1;
import com.mware.bigconnect.driver.internal.packstream.PackInput;
import com.mware.bigconnect.driver.internal.packstream.PackOutput;

public class MessageFormatV2 extends MessageFormatV1
{
    public static final byte DATE = 'D';
    public static final int DATE_STRUCT_SIZE = 1;

    public static final byte TIME = 'T';
    public static final int TIME_STRUCT_SIZE = 2;

    public static final byte LOCAL_TIME = 't';
    public static final int LOCAL_TIME_STRUCT_SIZE = 1;

    public static final byte LOCAL_DATE_TIME = 'd';
    public static final int LOCAL_DATE_TIME_STRUCT_SIZE = 2;

    public static final byte DATE_TIME_WITH_ZONE_OFFSET = 'F';
    public static final byte DATE_TIME_WITH_ZONE_ID = 'f';
    public static final int DATE_TIME_STRUCT_SIZE = 3;

    public static final byte DURATION = 'E';
    public static final int DURATION_TIME_STRUCT_SIZE = 4;

    public static final byte POINT_2D_STRUCT_TYPE = 'X';
    public static final int POINT_2D_STRUCT_SIZE = 3;

    public static final byte POINT_3D_STRUCT_TYPE = 'Y';
    public static final int POINT_3D_STRUCT_SIZE = 4;

    @Override
    public Writer newWriter( PackOutput output )
    {
        return new MessageWriterV2( output );
    }

    @Override
    public Reader newReader( PackInput input )
    {
        return new MessageReaderV2( input );
    }
}
