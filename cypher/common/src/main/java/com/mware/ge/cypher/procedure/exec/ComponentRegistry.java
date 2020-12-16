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
package com.mware.ge.cypher.procedure.exec;

import com.mware.ge.function.ThrowingFunction;
import com.mware.ge.cypher.exception.ProcedureException;
import com.mware.ge.cypher.procedure.impl.Context;

import java.util.HashMap;
import java.util.Map;

/**
 * Tracks components that can be injected into compiled procedures.
 */
public class ComponentRegistry
{
    /** Given the context of a procedure call, provide some component. */
    public interface Provider<T> extends ThrowingFunction<Context,T, ProcedureException>
    {
        // This interface intentionally empty, alias for the Function generic above
    }

    private final Map<Class<?>, Provider<?>> suppliers = new HashMap<>();

    public ComponentRegistry()
    {
    }

    @SuppressWarnings( "unchecked" )
    <T> Provider<T> providerFor( Class<T> type )
    {
        return (Provider<T>) suppliers.get( type );
    }

    public <T> void register( Class<T> cls, Provider<T> supplier )
    {
        suppliers.put( cls, supplier );
    }
}