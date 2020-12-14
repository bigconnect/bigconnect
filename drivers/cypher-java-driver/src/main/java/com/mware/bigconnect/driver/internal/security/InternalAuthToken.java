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
package com.mware.bigconnect.driver.internal.security;

import com.mware.bigconnect.driver.AuthToken;
import com.mware.bigconnect.driver.Value;

import java.util.Map;

/**
 * A simple common token for authentication schemes that easily convert to
 * an auth token map
 */
public class InternalAuthToken implements AuthToken
{
    public static final String SCHEME_KEY = "scheme";
    public static final String PRINCIPAL_KEY = "principal";
    public static final String CREDENTIALS_KEY = "credentials";
    public static final String REALM_KEY = "realm";
    public static final String PARAMETERS_KEY = "parameters";

    private final Map<String,Value> content;

    public InternalAuthToken( Map<String,Value> content )
    {
        this.content = content;
    }

    public Map<String, Value> toMap()
    {
        return content;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        InternalAuthToken that = (InternalAuthToken) o;

        return content != null ? content.equals( that.content ) : that.content == null;

    }

    @Override
    public int hashCode()
    {
        return content != null ? content.hashCode() : 0;
    }
}
