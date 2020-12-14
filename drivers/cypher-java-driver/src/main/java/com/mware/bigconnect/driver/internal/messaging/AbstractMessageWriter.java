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
package com.mware.bigconnect.driver.internal.messaging;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public abstract class AbstractMessageWriter implements MessageFormat.Writer
{
    private final ValuePacker packer;
    private final Map<Byte,MessageEncoder> encodersByMessageSignature;

    protected AbstractMessageWriter( ValuePacker packer, Map<Byte,MessageEncoder> encodersByMessageSignature )
    {
        this.packer = requireNonNull( packer );
        this.encodersByMessageSignature = requireNonNull( encodersByMessageSignature );
    }

    @Override
    public final void write( Message msg ) throws IOException
    {
        byte signature = msg.signature();
        MessageEncoder encoder = encodersByMessageSignature.get( signature );
        if ( encoder == null )
        {
            throw new IOException( "No encoder found for message " + msg + " with signature " + signature );
        }
        encoder.encode( msg, packer );
    }
}
