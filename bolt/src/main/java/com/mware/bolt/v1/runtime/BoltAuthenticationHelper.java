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

import com.mware.bolt.runtime.BoltConnectionFatality;
import com.mware.bolt.runtime.BoltStateMachineSPI;
import com.mware.bolt.runtime.StateMachineContext;
import com.mware.bolt.runtime.StatementProcessor;
import com.mware.bolt.security.auth.AuthenticationResult;
import com.mware.ge.values.storable.Values;

import java.util.Map;

public class BoltAuthenticationHelper {
    public static boolean processAuthentication(String userAgent, Map<String,Object> authToken, StateMachineContext context) throws BoltConnectionFatality {
        try {
            BoltStateMachineSPI boltSpi = context.boltSpi();

            AuthenticationResult authResult = boltSpi.authenticate(authToken, context.boltChannel().info());
            context.authenticatedAsUser(authResult.getUserName(), userAgent, authResult.getAuthorizations());

            StatementProcessor statementProcessor = new TransactionStateMachine(boltSpi.transactionSpi(), authResult, context.clock());
            context.connectionState().setStatementProcessor(statementProcessor);

            context.connectionState().onMetadata("server", Values.stringValue(boltSpi.version()));

            return true;
        } catch (Throwable t) {
            context.handleFailure(t, true);
            return false;
        }
    }
}
