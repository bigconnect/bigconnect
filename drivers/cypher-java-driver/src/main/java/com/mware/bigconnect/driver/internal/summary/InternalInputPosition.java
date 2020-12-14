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
package com.mware.bigconnect.driver.internal.summary;

import com.mware.bigconnect.driver.summary.InputPosition;

import java.util.Objects;

/**
 * An input position refers to a specific point in a query string.
 */
public class InternalInputPosition implements InputPosition
{
    private final int offset;
    private final int line;
    private final int column;

    /**
     * Creating a position from and offset, line number and a column number.
     *
     * @param offset the offset from the start of the string, starting from 0.
     * @param line the line number, starting from 1.
     * @param column the column number, starting from 1.
     */
    public InternalInputPosition( int offset, int line, int column )
    {
        this.offset = offset;
        this.line = line;
        this.column = column;
    }

    @Override
    public int offset()
    {
        return offset;
    }

    @Override
    public int line()
    {
        return line;
    }

    @Override
    public int column()
    {
        return column;
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
        InternalInputPosition that = (InternalInputPosition) o;
        return offset == that.offset &&
               line == that.line &&
               column == that.column;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( offset, line, column );
    }

    @Override
    public String toString()
    {
        return "offset=" + offset + ", line=" + line + ", column=" + column;
    }
}
