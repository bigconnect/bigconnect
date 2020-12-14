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

public class InternalPoint3D implements Point
{
    private final int srid;
    private final double x;
    private final double y;
    private final double z;

    public InternalPoint3D( int srid, double x, double y, double z )
    {
        this.srid = srid;
        this.x = x;
        this.y = y;
        this.z = z;
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
        return z;
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
        InternalPoint3D that = (InternalPoint3D) o;
        return srid == that.srid &&
               Double.compare( that.x, x ) == 0 &&
               Double.compare( that.y, y ) == 0 &&
               Double.compare( that.z, z ) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( srid, x, y, z );
    }

    @Override
    public String toString()
    {
        return "Point{" +
               "srid=" + srid +
               ", x=" + x +
               ", y=" + y +
               ", z=" + z +
               '}';
    }
}
