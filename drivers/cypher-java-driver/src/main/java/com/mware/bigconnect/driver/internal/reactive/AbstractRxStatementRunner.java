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
package com.mware.bigconnect.driver.internal.reactive;

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.reactive.RxStatementResult;
import com.mware.bigconnect.driver.reactive.RxStatementRunner;

import java.util.Map;

import static com.mware.bigconnect.driver.internal.AbstractStatementRunner.parameters;

public abstract class AbstractRxStatementRunner implements RxStatementRunner
{
    @Override
    public final RxStatementResult run(String statementTemplate, Value parameters )
    {
        return run( new Statement( statementTemplate, parameters ) );
    }

    @Override
    public final RxStatementResult run(String statementTemplate, Map<String, Object> statementParameters )
    {
        return run( statementTemplate, parameters( statementParameters ) );
    }

    @Override
    public final RxStatementResult run(String statementTemplate, Record statementParameters )
    {
        return run( statementTemplate, parameters( statementParameters ) );
    }

    @Override
    public final RxStatementResult run( String statementTemplate )
    {
        return run( new Statement( statementTemplate ) );
    }
}
