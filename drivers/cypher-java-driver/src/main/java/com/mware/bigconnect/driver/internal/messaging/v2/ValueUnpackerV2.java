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

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.messaging.v1.ValueUnpackerV1;
import com.mware.bigconnect.driver.internal.packstream.PackInput;
import com.mware.bigconnect.driver.internal.types.TypeConstructor;

import java.io.IOException;
import java.time.*;

import static com.mware.bigconnect.driver.Values.*;
import static com.mware.bigconnect.driver.internal.messaging.v2.MessageFormatV2.*;
import static java.time.ZoneOffset.UTC;

public class ValueUnpackerV2 extends ValueUnpackerV1
{
    public ValueUnpackerV2( PackInput input )
    {
        super( input );
    }

    @Override
    protected Value unpackStruct( long size, byte type ) throws IOException
    {
        switch ( type )
        {
        case DATE:
            ensureCorrectStructSize( TypeConstructor.DATE, DATE_STRUCT_SIZE, size );
            return unpackDate();
        case TIME:
            ensureCorrectStructSize( TypeConstructor.TIME, TIME_STRUCT_SIZE, size );
            return unpackTime();
        case LOCAL_TIME:
            ensureCorrectStructSize( TypeConstructor.LOCAL_TIME, LOCAL_TIME_STRUCT_SIZE, size );
            return unpackLocalTime();
        case LOCAL_DATE_TIME:
            ensureCorrectStructSize( TypeConstructor.LOCAL_DATE_TIME, LOCAL_DATE_TIME_STRUCT_SIZE, size );
            return unpackLocalDateTime();
        case DATE_TIME_WITH_ZONE_OFFSET:
            ensureCorrectStructSize( TypeConstructor.DATE_TIME, DATE_TIME_STRUCT_SIZE, size );
            return unpackDateTimeWithZoneOffset();
        case DATE_TIME_WITH_ZONE_ID:
            ensureCorrectStructSize( TypeConstructor.DATE_TIME, DATE_TIME_STRUCT_SIZE, size );
            return unpackDateTimeWithZoneId();
        case DURATION:
            ensureCorrectStructSize( TypeConstructor.DURATION, DURATION_TIME_STRUCT_SIZE, size );
            return unpackDuration();
        case POINT_2D_STRUCT_TYPE:
            ensureCorrectStructSize( TypeConstructor.POINT, POINT_2D_STRUCT_SIZE, size );
            return unpackPoint2D();
        case POINT_3D_STRUCT_TYPE:
            ensureCorrectStructSize( TypeConstructor.POINT, POINT_3D_STRUCT_SIZE, size );
            return unpackPoint3D();
        default:
            return super.unpackStruct( size, type );
        }
    }

    private Value unpackDate() throws IOException
    {
        long epochDay = unpacker.unpackLong();
        return value( LocalDate.ofEpochDay( epochDay ) );
    }

    private Value unpackTime() throws IOException
    {
        long nanoOfDayLocal = unpacker.unpackLong();
        int offsetSeconds = Math.toIntExact( unpacker.unpackLong() );

        LocalTime localTime = LocalTime.ofNanoOfDay( nanoOfDayLocal );
        ZoneOffset offset = ZoneOffset.ofTotalSeconds( offsetSeconds );
        return value( OffsetTime.of( localTime, offset ) );
    }

    private Value unpackLocalTime() throws IOException
    {
        long nanoOfDayLocal = unpacker.unpackLong();
        return value( LocalTime.ofNanoOfDay( nanoOfDayLocal ) );
    }

    private Value unpackLocalDateTime() throws IOException
    {
        long epochSecondUtc = unpacker.unpackLong();
        int nano = Math.toIntExact( unpacker.unpackLong() );
        return value( LocalDateTime.ofEpochSecond( epochSecondUtc, nano, UTC ) );
    }

    private Value unpackDateTimeWithZoneOffset() throws IOException
    {
        long epochSecondLocal = unpacker.unpackLong();
        int nano = Math.toIntExact( unpacker.unpackLong() );
        int offsetSeconds = Math.toIntExact( unpacker.unpackLong() );
        return value( newZonedDateTime( epochSecondLocal, nano, ZoneOffset.ofTotalSeconds( offsetSeconds ) ) );
    }

    private Value unpackDateTimeWithZoneId() throws IOException
    {
        long epochSecondLocal = unpacker.unpackLong();
        int nano = Math.toIntExact( unpacker.unpackLong() );
        String zoneIdString = unpacker.unpackString();
        return value( newZonedDateTime( epochSecondLocal, nano, ZoneId.of( zoneIdString ) ) );
    }

    private Value unpackDuration() throws IOException
    {
        long months = unpacker.unpackLong();
        long days = unpacker.unpackLong();
        long seconds = unpacker.unpackLong();
        int nanoseconds = Math.toIntExact( unpacker.unpackLong() );
        return isoDuration( months, days, seconds, nanoseconds );
    }

    private Value unpackPoint2D() throws IOException
    {
        int srid = Math.toIntExact( unpacker.unpackLong() );
        double x = unpacker.unpackDouble();
        double y = unpacker.unpackDouble();
        return point( srid, x, y );
    }

    private Value unpackPoint3D() throws IOException
    {
        int srid = Math.toIntExact( unpacker.unpackLong() );
        double x = unpacker.unpackDouble();
        double y = unpacker.unpackDouble();
        double z = unpacker.unpackDouble();
        return point( srid, x, y, z );
    }

    private static ZonedDateTime newZonedDateTime(long epochSecondLocal, long nano, ZoneId zoneId )
    {
        Instant instant = Instant.ofEpochSecond( epochSecondLocal, nano );
        LocalDateTime localDateTime = LocalDateTime.ofInstant( instant, UTC );
        return ZonedDateTime.of( localDateTime, zoneId );
    }
}
