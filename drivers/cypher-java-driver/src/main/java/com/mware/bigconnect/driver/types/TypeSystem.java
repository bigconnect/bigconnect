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
package com.mware.bigconnect.driver.types;

import com.mware.bigconnect.driver.util.Experimental;
import com.mware.bigconnect.driver.util.Immutable;

/**
 * A listing of all database types this driver can handle.
 * @since 1.0
 */
@Immutable
@Experimental
public interface TypeSystem
{
    Type ANY();

    Type BOOLEAN();

    Type BYTES();

    Type STRING();

    Type NUMBER();

    Type INTEGER();

    Type FLOAT();

    Type LIST();

    Type MAP();

    Type NODE();

    Type RELATIONSHIP();

    Type PATH();

    Type POINT();

    Type DATE();

    Type TIME();

    Type LOCAL_TIME();

    Type LOCAL_DATE_TIME();

    Type DATE_TIME();

    Type DURATION();

    Type NULL();
}
