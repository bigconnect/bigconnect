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
package com.mware.bigconnect.driver.exceptions;

/**
 * This is the base class for all exceptions caused as part of communication with the remote BigConnect server.
 *
 * @since 1.0
 */
public abstract class BigConnectException extends RuntimeException
{
    private static final long serialVersionUID = -80579062276712566L;

    private final String code;

    public BigConnectException( String message )
    {
        this( "N/A", message );
    }

    public BigConnectException(String message, Throwable cause )
    {
        this( "N/A", message, cause );
    }

    public BigConnectException(String code, String message )
    {
        this( code, message, null );
    }

    public BigConnectException(String code, String message, Throwable cause )
    {
        super( message, cause );
        this.code = code;
    }

    /**
     * Access the standard BigConnect Status Code for this exception, you can use this to refer to the BigConnect manual for
     * details on what caused the error.
     *
     * @return the BigConnect Status Code for this exception, or 'N/A' if none is available
     */
    @Deprecated
    public String bcErrorCode()
    {
        return code;
    }

    /**
     * Access the status code for this exception. The BigConnect manual can
     * provide further details on the available codes and their meanings.
     *
     * @return textual code, such as "Cypher.ClientError.Procedure.ProcedureNotFound"
     */
    public String code()
    {
        return code;
    }

}
