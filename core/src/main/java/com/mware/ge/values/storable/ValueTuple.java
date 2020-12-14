/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package com.mware.ge.values.storable;

import java.util.Comparator;

/**
 * A tuple of n values.
 */
public class ValueTuple
{
    public static ValueTuple of( Value... values )
    {
        assert values.length > 0 : "Empty ValueTuple is not allowed";
        assert noNulls( values );
        return new ValueTuple( values );
    }

    public static ValueTuple of( Object... objects )
    {
        assert objects.length > 0 : "Empty ValueTuple is not allowed";
        assert noNulls( objects );
        Value[] values = new Value[objects.length];
        for ( int i = 0; i < values.length; i++ )
        {
            values[i] = Values.of( objects[i] );
        }
        return new ValueTuple( values );
    }

    private final Value[] values;

    protected ValueTuple( Value[] values )
    {
        this.values = values;
    }

    public int size()
    {
        return values.length;
    }

    public Value valueAt( int offset )
    {
        return values[offset];
    }

    /**
     * WARNING: this method does not create a defensive copy. Do not modify the returned array.
     */
    public Value[] getValues()
    {
        return values;
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

        ValueTuple that = (ValueTuple) o;

        if ( that.values.length != values.length )
        {
            return false;
        }

        for ( int i = 0; i < values.length; i++ )
        {
            if ( !values[i].equals( that.values[i] ) )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int result = 1;
        for ( Object value : values )
        {
            result = 31 * result + value.hashCode();
        }
        return result;
    }

    public Value getOnlyValue()
    {
        assert values.length == 1 : "Assumed single value tuple, but had " + values.length;
        return values[0];
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        String sep = "( ";
        for ( Value value : values )
        {
            sb.append( sep );
            sep = ", ";
            sb.append( value );
        }
        sb.append( " )" );
        return sb.toString();
    }

    private static boolean noNulls( Object[] values )
    {
        for ( Object v : values )
        {
            if ( v == null )
            {
                return false;
            }
        }
        return true;
    }

    public static final Comparator<ValueTuple> COMPARATOR = ( left, right ) ->
    {
        if ( left.values.length != right.values.length )
        {
            throw new IllegalStateException( "Comparing two ValueTuples of different lengths!" );
        }

        int compare = 0;
        for ( int i = 0; i < left.values.length; i++ )
        {
            compare = Values.COMPARATOR.compare( left.valueAt( i ), right.valueAt( i ) );
            if ( compare != 0 )
            {
                return compare;
            }
        }
        return compare;
    };
}
