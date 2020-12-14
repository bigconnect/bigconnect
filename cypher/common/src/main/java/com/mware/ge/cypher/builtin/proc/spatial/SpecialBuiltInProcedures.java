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
package com.mware.ge.cypher.builtin.proc.spatial;

import com.mware.ge.cypher.builtin.proc.JmxQueryProcedure;
import com.mware.ge.function.ThrowingConsumer;
import com.mware.ge.cypher.exception.ProcedureException;
import com.mware.ge.cypher.procedure.exec.Procedures;

import java.lang.management.ManagementFactory;

import static com.mware.ge.cypher.procedure.impl.ProcedureSignature.procedureName;

/**
 * This class houses built-in procedures which use a backdoor to inject dependencies.
 * <p>
 * TODO: The dependencies should be made available by a standard mechanism so the backdoor is not needed.
 */
public class SpecialBuiltInProcedures implements ThrowingConsumer<Procedures, ProcedureException> {
    public SpecialBuiltInProcedures() {
    }

    @Override
    public void accept(Procedures procs) throws ProcedureException {
        procs.register(new JmxQueryProcedure(procedureName("dbms", "queryJmx"),
                ManagementFactory.getPlatformMBeanServer()));
    }
}
