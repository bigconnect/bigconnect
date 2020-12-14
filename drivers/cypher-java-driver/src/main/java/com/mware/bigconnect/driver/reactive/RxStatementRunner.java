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
package com.mware.bigconnect.driver.reactive;

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.Values;

import java.util.Map;

/**
 * Common interface for components that can execute BigConnect statements using Reactive API.
 * @see RxSession
 * @see RxTransaction
 * @since 2.0
 */
public interface RxStatementRunner
{
    /**
     * Register running of a statement and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
     *
     * This method takes a set of parameters that will be injected into the
     * statement by BigConnect. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * BigConnect can re-use query plans more often.
     *
     * This particular method takes a {@link Value} as its input. This is useful
     * if you want to take a map-like value that you've gotten from a prior result
     * and send it back as parameters.
     *
     * If you are creating parameters programmatically, {@link #run(String, Map)}
     * might be more helpful, it converts your map to a {@link Value} for you.
     *
     * @param statementTemplate text of a BigConnect statement
     * @param parameters input parameters, should be a map Value, see {@link Values#parameters(Object...)}.
     * @return a reactive result.
     */
    RxStatementResult run(String statementTemplate, Value parameters);

    /**
     * Register running of a statement and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
     *
     * This method takes a set of parameters that will be injected into the
     * statement by BigConnect. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * BigConnect can re-use query plans more often.
     *
     * This version of run takes a {@link Map} of parameters. The values in the map
     * must be values that can be converted to BigConnect types. See {@link Values#parameters(Object...)} for
     * a list of allowed types.
     *
     * @param statementTemplate text of a BigConnect statement
     * @param statementParameters input data for the statement
     * @return a reactive result.
     */
    RxStatementResult run(String statementTemplate, Map<String, Object> statementParameters);

    /**
     * Register running of a statement and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
     *
     * This method takes a set of parameters that will be injected into the
     * statement by BigConnect. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * BigConnect can re-use query plans more often.
     *
     * This version of run takes a {@link Record} of parameters, which can be useful
     * if you want to use the output of one statement as input for another.
     *
     * @param statementTemplate text of a BigConnect statement
     * @param statementParameters input data for the statement
     * @return a reactive result.
     */
    RxStatementResult run(String statementTemplate, Record statementParameters);

    /**
     * Register running of a statement and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
     *
     * @param statementTemplate text of a BigConnect statement
     * @return a reactive result.
     */
    RxStatementResult run(String statementTemplate);

    /**
     * Register running of a statement and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
     *
     * @param statement a BigConnect statement
     * @return a reactive result.
     */
    RxStatementResult run(Statement statement);
}
