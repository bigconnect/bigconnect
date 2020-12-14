/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
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
package com.mware.ge.cypher;

import com.mware.ge.cypher.query.KernelStatement;
import com.mware.ge.cypher.query.ExecutingQuery;
import com.mware.ge.cypher.query.ClientConnectionInfo;
import com.mware.ge.values.virtual.MapValue;

import java.util.Map;
import java.util.stream.Stream;

public class OperationsFacade implements QueryRegistryOperations
{
    private final KernelStatement statement;
    private StatementOperationParts operations;

    public OperationsFacade(KernelStatement statement,
                     StatementOperationParts operationParts )
    {
        this.statement = statement;
        this.operations = operationParts;
    }

    final QueryRegistrationOperations queryRegistrationOperations()
    {
        return operations.queryRegistrationOperations();
    }

    // query monitoring

    @Override
    public void setMetaData( Map<String,Object> data )
    {
        statement.assertOpen();
        statement.getTransaction().setMetaData( data );
    }

    @Override
    public Map<String,Object> getMetaData()
    {
        statement.assertOpen();
        return statement.getTransaction().getMetaData();
    }

    @Override
    public Stream<ExecutingQuery> executingQueries()
    {
        statement.assertOpen();
        return queryRegistrationOperations().executingQueries( statement );
    }

    @Override
    public ExecutingQuery startQueryExecution(
        ClientConnectionInfo descriptor,
        String queryText,
        MapValue queryParameters )
    {
        statement.assertOpen();
        return queryRegistrationOperations().startQueryExecution( statement, descriptor, queryText, queryParameters );
    }

    @Override
    public void registerExecutingQuery( ExecutingQuery executingQuery )
    {
        statement.assertOpen();
        queryRegistrationOperations().registerExecutingQuery( statement, executingQuery );
    }

    @Override
    public void unregisterExecutingQuery( ExecutingQuery executingQuery )
    {
        queryRegistrationOperations().unregisterExecutingQuery( statement, executingQuery );
    }

    // query monitoring
}
