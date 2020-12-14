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
package com.mware.ge.cypher.builtin.proc;

import com.mware.ge.collection.RawIterator;
import com.mware.ge.cypher.exception.ProcedureException;
import com.mware.ge.cypher.procedure.impl.Neo4jTypes;
import com.mware.ge.io.ResourceTracker;
import com.mware.ge.cypher.procedure.impl.CallableProcedure;
import com.mware.ge.cypher.procedure.impl.Context;

import static com.mware.ge.cypher.procedure.impl.ProcedureSignature.procedureName;
import static com.mware.ge.cypher.procedure.impl.ProcedureSignature.procedureSignature;

public interface ClusterProcedures {
    class ClusterRoleProcedure extends CallableProcedure.BasicProcedure {
        public ClusterRoleProcedure() {
            super(procedureSignature(procedureName("dbms", "cluster", "role"))
                    .out("role", Neo4jTypes.NTString)
                    .description("The role of a BigConnect instance in the cluster.")
                    .build());
        }

        @Override
        public RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input, ResourceTracker resourceTracker) throws ProcedureException {
            return RawIterator.of(new Object[][]{
                    {RoleInfo.LEADER.name()}
            });
        }
    }

    enum RoleInfo {
        LEADER,
        FOLLOWER
    }
}
