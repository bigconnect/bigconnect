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
package com.mware.bolt.security.auth;

import com.mware.bolt.BoltConnectionInfo;
import com.mware.core.model.user.UserNameAuthorizationContext;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workspace.Workspace;
import com.mware.core.security.AuthTokenException;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.cypher.GeCypherExecutionEngine;
import com.mware.ge.cypher.exception.Status;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BasicAuthentication implements Authentication {
    final static String SCHEME = "scheme";
    final static String SCHEME_PASSWORD = "password";
    final static String SCHEME_BASIC = "basic";
    final static String SCHEME_TOKEN = "token";
    final static String PRINCIPAL = "principal";
    final static String CREDENTIALS = "credentials";

    private GeCypherExecutionEngine executionEngine;

    public BasicAuthentication(GeCypherExecutionEngine executionEngine) {
        this.executionEngine = executionEngine;
    }

    @Override
    public AuthenticationResult authenticate(Map<String, Object> authToken, BoltConnectionInfo info) throws AuthenticationException {
        String principal = (String) authToken.get(PRINCIPAL);
        String scheme = (String) authToken.get(SCHEME);
        String passwordOrToken = (String) authToken.get(CREDENTIALS);

        User user = null;

        if (SCHEME_PASSWORD.equals(scheme) || SCHEME_BASIC.equals(scheme)) {
            UserRepository userRepository = executionEngine.getUserRepository();
            User tmpUser = userRepository.findByUsername(principal);
            if (tmpUser != null && userRepository.isPasswordValid(tmpUser, passwordOrToken)) {
                user = tmpUser;
            }
        } else if (SCHEME_TOKEN.equals(scheme)) {
            // validate token
            try {
                user = executionEngine.getAuthTokenService().validateToken(passwordOrToken);
            } catch (AuthTokenException e) {
                throw new AuthenticationException( Status.Security.Unauthorized );
            }
        }

        if (user != null) {
            UserNameAuthorizationContext authorizationContext = new UserNameAuthorizationContext(principal, info.clientAddress());
            executionEngine.getUserRepository().updateUser(user, authorizationContext);
            executionEngine.getAuditService().auditLogin(user);
            Set<String> authorizationSet = new HashSet<>();
            Authorizations authorizations = null;
            if (executionEngine.getWorkspaceRepository() != null) {
                Iterable<Workspace> workspaces = executionEngine.getWorkspaceRepository().findAllForUser(user);
                workspaces.forEach(w -> authorizationSet.add(w.getWorkspaceId()));
                authorizations = executionEngine.getAuthorizationRepository().getGraphAuthorizations(user, authorizationSet.toArray(new String[0]));
            }

            if (authorizations == null)
                authorizations = executionEngine.getAuthorizationRepository().getGraphAuthorizations(principal);

            return new AuthenticationResult(principal, authorizations);
        }

        throw new AuthenticationException( Status.Security.Unauthorized );
    }
}
