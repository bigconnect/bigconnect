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
package com.mware.bigconnect.driver.summary;

import com.mware.bigconnect.driver.exceptions.ClientException;

/**
 * The type of statement executed.
 * @since 1.0
 */
public enum StatementType
{
    READ_ONLY,
    READ_WRITE,
    WRITE_ONLY,
    SCHEMA_WRITE;

    public static StatementType fromCode( String type )
    {
        switch ( type )
        {
        case "r":
            return StatementType.READ_ONLY;
        case "rw":
            return StatementType.READ_WRITE;
        case "w":
            return StatementType.WRITE_ONLY;
        case "s":
            return StatementType.SCHEMA_WRITE;
        default:
            throw new ClientException( "Unknown statement type: `" + type + "`." );
        }
    }
}
