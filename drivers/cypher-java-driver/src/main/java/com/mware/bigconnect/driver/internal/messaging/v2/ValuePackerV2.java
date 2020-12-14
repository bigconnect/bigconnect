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

import com.mware.bigconnect.driver.internal.InternalPoint2D;
import com.mware.bigconnect.driver.internal.InternalPoint3D;
import com.mware.bigconnect.driver.internal.messaging.v1.ValuePackerV1;
import com.mware.bigconnect.driver.internal.packstream.PackOutput;
import com.mware.bigconnect.driver.internal.types.TypeConstructor;
import com.mware.bigconnect.driver.internal.value.InternalValue;
import com.mware.bigconnect.driver.types.IsoDuration;
import com.mware.bigconnect.driver.types.Point;

import java.io.IOException;
import java.time.*;
import java.util.Date;

import static java.time.ZoneOffset.UTC;
import static com.mware.bigconnect.driver.internal.messaging.v2.MessageFormatV2.*;

public class ValuePackerV2 extends ValuePackerV1
{
    public ValuePackerV2( PackOutput output )
    {
        super( output );
    }

    @Override
    protected void packInternalValue( InternalValue value ) throws IOException
    {
        TypeConstructor typeConstructor = value.typeConstructor();
        switch ( typeConstructor )
        {
        case DATE:
            packDate( value.asLocalDate() );
            break;
        case TIME:
            packTime( value.asOffsetTime() );
            break;
        case LOCAL_TIME:
            packLocalTime( value.asLocalTime() );
            break;
        case LOCAL_DATE_TIME:
            packLocalDateTime( value.asLocalDateTime() );
            break;
        case DATE_TIME:
            packZonedDateTime( value.asZonedDateTime() );
            break;
        case DURATION:
            packDuration( value.asIsoDuration() );
            break;
        case POINT:
            packPoint( value.asPoint() );
            break;
        default:
            super.packInternalValue( value );
        }
    }

    private void packDate( LocalDate localDate ) throws IOException
    {
        packer.packStructHeader( DATE_STRUCT_SIZE, DATE );
        packer.pack( localDate.toEpochDay() );
    }

    private void packTime( OffsetTime offsetTime ) throws IOException
    {
        long nanoOfDayLocal = offsetTime.toLocalTime().toNanoOfDay();
        int offsetSeconds = offsetTime.getOffset().getTotalSeconds();

        packer.packStructHeader( TIME_STRUCT_SIZE, TIME );
        packer.pack( nanoOfDayLocal );
        packer.pack( offsetSeconds );
    }

    private void packLocalTime( LocalTime localTime ) throws IOException
    {
        packer.packStructHeader( LOCAL_TIME_STRUCT_SIZE, LOCAL_TIME );
        packer.pack( localTime.toNanoOfDay() );
    }

    private void packLocalDateTime( LocalDateTime localDateTime ) throws IOException
    {
        long epochSecondUtc = localDateTime.toEpochSecond( UTC );
        int nano = localDateTime.getNano();

        packer.packStructHeader( LOCAL_DATE_TIME_STRUCT_SIZE, LOCAL_DATE_TIME );
        packer.pack( epochSecondUtc );
        packer.pack( nano );
    }

    private void packZonedDateTime( ZonedDateTime zonedDateTime ) throws IOException
    {
        long epochSecondLocal = zonedDateTime.toLocalDateTime().toEpochSecond( UTC );
        int nano = zonedDateTime.getNano();

        ZoneId zone = zonedDateTime.getZone();
        if ( zone instanceof ZoneOffset)
        {
            int offsetSeconds = ((ZoneOffset) zone).getTotalSeconds();

            packer.packStructHeader( DATE_TIME_STRUCT_SIZE, DATE_TIME_WITH_ZONE_OFFSET );
            packer.pack( epochSecondLocal );
            packer.pack( nano );
            packer.pack( offsetSeconds );
        }
        else
        {
            String zoneId = zone.getId();

            packer.packStructHeader( DATE_TIME_STRUCT_SIZE, DATE_TIME_WITH_ZONE_ID );
            packer.pack( epochSecondLocal );
            packer.pack( nano );
            packer.pack( zoneId );
        }
    }

    private void packDuration( IsoDuration duration ) throws IOException
    {
        packer.packStructHeader( DURATION_TIME_STRUCT_SIZE, DURATION );
        packer.pack( duration.months() );
        packer.pack( duration.days() );
        packer.pack( duration.seconds() );
        packer.pack( duration.nanoseconds() );
    }

    private void packPoint( Point point ) throws IOException
    {
        if ( point instanceof InternalPoint2D )
        {
            packPoint2D( point );
        }
        else if ( point instanceof InternalPoint3D )
        {
            packPoint3D( point );
        }
        else
        {
            throw new IOException( String.format( "Unknown type: type: %s, value: %s", point.getClass(), point.toString() ) );
        }
    }

    private void packPoint2D( Point point ) throws IOException
    {
        packer.packStructHeader( POINT_2D_STRUCT_SIZE, POINT_2D_STRUCT_TYPE );
        packer.pack( point.srid() );
        packer.pack( point.x() );
        packer.pack( point.y() );
    }

    private void packPoint3D( Point point ) throws IOException
    {
        packer.packStructHeader( POINT_3D_STRUCT_SIZE, POINT_3D_STRUCT_TYPE );
        packer.pack( point.srid() );
        packer.pack( point.x() );
        packer.pack( point.y() );
        packer.pack( point.z() );
    }
}
