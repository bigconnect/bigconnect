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
package com.mware.bigconnect.driver.internal;

import com.mware.bigconnect.driver.types.IsoDuration;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.List;
import java.util.Objects;

import static java.time.temporal.ChronoUnit.*;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public class InternalIsoDuration implements IsoDuration
{
    private static final long NANOS_PER_SECOND = 1_000_000_000;
    private static final List<TemporalUnit> SUPPORTED_UNITS = unmodifiableList( asList( MONTHS, DAYS, SECONDS, NANOS ) );

    private final long months;
    private final long days;
    private final long seconds;
    private final int nanoseconds;

    public InternalIsoDuration( Period period )
    {
        this( period.toTotalMonths(), period.getDays(), Duration.ZERO );
    }

    public InternalIsoDuration( Duration duration )
    {
        this( 0, 0, duration );
    }

    public InternalIsoDuration( long months, long days, long seconds, int nanoseconds )
    {
        this( months, days, Duration.ofSeconds( seconds, nanoseconds ) );
    }

    InternalIsoDuration( long months, long days, Duration duration )
    {
        this.months = months;
        this.days = days;
        this.seconds = duration.getSeconds(); // normalized value of seconds
        this.nanoseconds = duration.getNano(); // normalized value of nanoseconds in [0, 999_999_999]
    }

    @Override
    public long months()
    {
        return months;
    }

    @Override
    public long days()
    {
        return days;
    }

    @Override
    public long seconds()
    {
        return seconds;
    }

    @Override
    public int nanoseconds()
    {
        return nanoseconds;
    }

    @Override
    public long get( TemporalUnit unit )
    {
        if ( unit == MONTHS )
        {
            return months;
        }
        else if ( unit == DAYS )
        {
            return days;
        }
        else if ( unit == SECONDS )
        {
            return seconds;
        }
        else if ( unit == NANOS )
        {
            return nanoseconds;
        }
        else
        {
            throw new UnsupportedTemporalTypeException( "Unsupported unit: " + unit );
        }
    }

    @Override
    public List<TemporalUnit> getUnits()
    {
        return SUPPORTED_UNITS;
    }

    @Override
    public Temporal addTo(Temporal temporal )
    {
        if ( months != 0 )
        {
            temporal = temporal.plus( months, MONTHS );
        }
        if ( days != 0 )
        {
            temporal = temporal.plus( days, DAYS );
        }
        if ( seconds != 0 )
        {
            temporal = temporal.plus( seconds, SECONDS );
        }
        if ( nanoseconds != 0 )
        {
            temporal = temporal.plus( nanoseconds, NANOS );
        }
        return temporal;
    }

    @Override
    public Temporal subtractFrom(Temporal temporal )
    {
        if ( months != 0 )
        {
            temporal = temporal.minus( months, MONTHS );
        }
        if ( days != 0 )
        {
            temporal = temporal.minus( days, DAYS );
        }
        if ( seconds != 0 )
        {
            temporal = temporal.minus( seconds, SECONDS );
        }
        if ( nanoseconds != 0 )
        {
            temporal = temporal.minus( nanoseconds, NANOS );
        }
        return temporal;
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
        InternalIsoDuration that = (InternalIsoDuration) o;
        return months == that.months &&
               days == that.days &&
               seconds == that.seconds &&
               nanoseconds == that.nanoseconds;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( months, days, seconds, nanoseconds );
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append( 'P' );
        sb.append( months ).append( 'M' );
        sb.append( days ).append( 'D' );
        sb.append( 'T' );
        if ( seconds < 0 && nanoseconds > 0 )
        {
            if ( seconds == -1 )
            {
                sb.append( "-0" );
            }
            else
            {
                sb.append( seconds + 1 );
            }
        }
        else
        {
            sb.append( seconds );
        }
        if ( nanoseconds > 0 )
        {
            int pos = sb.length();
            // append nanoseconds as a 10-digit string with leading '1' that is later replaced by a '.'
            if ( seconds < 0 )
            {
                sb.append( 2 * NANOS_PER_SECOND - nanoseconds );
            }
            else
            {
                sb.append( NANOS_PER_SECOND + nanoseconds );
            }
            sb.setCharAt( pos, '.' ); // replace '1' with '.'
        }
        sb.append( 'S' );
        return sb.toString();
    }
}
