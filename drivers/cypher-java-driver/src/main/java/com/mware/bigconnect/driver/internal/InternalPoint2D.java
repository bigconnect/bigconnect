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

import com.mware.bigconnect.driver.types.Point;

import java.util.Objects;

public class InternalPoint2D implements Point
{
    private final int srid;
    private final double x;
    private final double y;

    public InternalPoint2D( int srid, double x, double y )
    {
        this.srid = srid;
        this.x = x;
        this.y = y;
    }

    @Override
    public int srid()
    {
        return srid;
    }

    @Override
    public double x()
    {
        return x;
    }

    @Override
    public double y()
    {
        return y;
    }

    @Override
    public double z()
    {
        return Double.NaN;
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
        InternalPoint2D that = (InternalPoint2D) o;
        return srid == that.srid &&
               Double.compare( that.x, x ) == 0 &&
               Double.compare( that.y, y ) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( srid, x, y );
    }

    @Override
    public String toString()
    {
        return "Point{" +
               "srid=" + srid +
               ", x=" + x +
               ", y=" + y +
               '}';
    }
}
