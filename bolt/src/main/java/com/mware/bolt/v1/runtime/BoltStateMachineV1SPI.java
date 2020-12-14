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

import com.mware.bolt.BoltConnectionInfo;
import com.mware.bolt.BoltServer;
import com.mware.bolt.runtime.BigConnectError;
import com.mware.bolt.runtime.BoltStateMachineSPI;
import com.mware.bolt.runtime.TransactionStateMachineSPI;
import com.mware.bolt.security.auth.Authentication;
import com.mware.bolt.security.auth.AuthenticationException;
import com.mware.bolt.security.auth.AuthenticationResult;
import com.mware.bolt.security.auth.BasicAuthentication;
import com.mware.ge.cypher.GeCypherExecutionEngine;

import java.util.Map;

public class BoltStateMachineV1SPI implements BoltStateMachineSPI {
    public static final String BOLT_SERVER_VERSION_PREFIX = "Neo4j/";

    private final GeCypherExecutionEngine executionEngine;
    private final TransactionStateMachineSPI transactionStateMachineSPI;
    private final ErrorReporter errorReporter;
    private final Authentication authentication;
    private final String version;

    public BoltStateMachineV1SPI(GeCypherExecutionEngine executionEngine, TransactionStateMachineSPI transactionStateMachineSPI) {
        this.executionEngine = executionEngine;
        this.transactionStateMachineSPI = transactionStateMachineSPI;
        this.errorReporter = new ErrorReporter();
        this.authentication = new BasicAuthentication(executionEngine);
        this.version = BOLT_SERVER_VERSION_PREFIX + BoltServer.getVersion();
    }

    @Override
    public void reportError(BigConnectError err) {
        errorReporter.report(err);
    }

    @Override
    public AuthenticationResult authenticate(Map<String, Object> authToken, BoltConnectionInfo info) throws AuthenticationException {
        return authentication.authenticate(authToken, info);
    }

    @Override
    public TransactionStateMachineSPI transactionSpi() {
        return transactionStateMachineSPI;
    }

    @Override
    public String version() {
        return version;
    }
}
