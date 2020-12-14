/*
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
package com.mware.bolt.v1.runtime;

import com.mware.bolt.BoltChannel;
import com.mware.bolt.runtime.BoltResult;
import com.mware.bolt.runtime.BoltResultHandle;
import com.mware.bolt.runtime.TransactionStateMachineSPI;
import com.mware.bolt.security.auth.AuthenticationResult;
import com.mware.core.exception.BcException;
import com.mware.ge.cypher.GeCypherExecutionEngine;
import com.mware.ge.cypher.Result;
import com.mware.ge.cypher.internal.javacompat.QueryResultProvider;
import com.mware.ge.values.virtual.MapValue;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;

import static java.lang.String.format;

public class TransactionStateMachineV1SPI implements TransactionStateMachineSPI {
    private final BoltChannel boltChannel;
    private final Duration txAwaitDuration;
    private final Clock clock;
    private final GeCypherExecutionEngine executionEngine;

    public TransactionStateMachineV1SPI(GeCypherExecutionEngine executionEngine, BoltChannel boltChannel, Duration txAwaitDuration, Clock clock) {
        this.boltChannel = boltChannel;
        this.txAwaitDuration = txAwaitDuration;
        this.clock = clock;
        this.executionEngine = executionEngine;
    }

    @Override
    public BoltResultHandle executeQuery(AuthenticationResult authentication, String statement, MapValue params, Duration txTimeout, Map<String, Object> txMetaData) {
        return newBoltResultHandle(statement, params, authentication);
    }

    protected BoltResultHandle newBoltResultHandle(String statement, MapValue params, AuthenticationResult authentication) {
        return new BoltResultHandleV1(statement, params, authentication);
    }

    public class BoltResultHandleV1 implements BoltResultHandle {
        private final String statement;
        private final MapValue params;
        private AuthenticationResult authentication;

        public BoltResultHandleV1(String statement, MapValue params, AuthenticationResult authentication) {
            this.statement = statement;
            this.params = params;
            this.authentication = authentication;
        }

        @Override
        public BoltResult start() throws BcException {
            Result result = executionEngine.executeQuery(statement, params, authentication.getAuthorizations());
            if (result instanceof QueryResultProvider) {
                return newBoltResult((QueryResultProvider) result, clock);
            } else {
                throw new IllegalStateException(format("Unexpected query execution result. Expected to get instance of %s but was %s.",
                        QueryResultProvider.class.getName(), result.getClass().getName()));
            }
        }

        protected BoltResult newBoltResult(QueryResultProvider result, Clock clock) {
            return new CypherAdapterStream(result.queryResult(), clock);
        }

        @Override
        public void close(boolean success) {

        }

        @Override
        public void terminate() {

        }
    }
}
