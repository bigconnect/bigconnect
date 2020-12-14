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

import com.mware.bigconnect.driver.internal.BoltServerAddress;
import com.mware.bigconnect.driver.internal.util.ServerVersion;
import com.mware.bigconnect.driver.summary.ServerInfo;

import java.util.Objects;

public class InternalServerInfo implements ServerInfo
{
    private final String address;
    private final String version;

    public InternalServerInfo( BoltServerAddress address, ServerVersion version )
    {
        this.address = address.toString();
        this.version = version.toString();
    }

    @Override
    public String address()
    {
        return address;
    }

    @Override
    public String version()
    {
        return version;
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
        InternalServerInfo that = (InternalServerInfo) o;
        return Objects.equals( address, that.address ) && Objects.equals( version, that.version );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( address, version );
    }

    @Override
    public String toString()
    {
        return "InternalServerInfo{" + "address='" + address + '\'' + ", version='" + version + '\'' + '}';
    }
}
