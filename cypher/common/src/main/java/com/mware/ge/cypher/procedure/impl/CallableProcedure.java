/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
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
package com.mware.ge.cypher.procedure.impl;

import com.mware.ge.collection.RawIterator;
import com.mware.ge.cypher.exception.ProcedureException;
import com.mware.ge.io.ResourceTracker;

public interface CallableProcedure
{
    ProcedureSignature signature();
    RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input, ResourceTracker resourceTracker) throws ProcedureException;

    abstract class BasicProcedure implements CallableProcedure
    {
        private final ProcedureSignature signature;

        protected BasicProcedure( ProcedureSignature signature )
        {
            this.signature = signature;
        }

        @Override
        public ProcedureSignature signature()
        {
            return signature;
        }

        @Override
        public abstract RawIterator<Object[], ProcedureException> apply(
                Context ctx, Object[] input, ResourceTracker resourceTracker ) throws ProcedureException;
    }
}
