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
package com.mware.ge.cypher.builtin.proc.jdbc;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;

public class JdbcUtil {
    private JdbcUtil() {}

    public static Connection getConnection(String jdbcUrl, LoadJdbcConfig config) throws Exception {
        if(config.hasCredentials()) {
            return createConnection(jdbcUrl, config.getCredentials().getUser(), config.getCredentials().getPassword());
        } else {
            URI uri = new URI(jdbcUrl.substring("jdbc:".length()));
            String userInfo = uri.getUserInfo();
            if (userInfo != null) {
                String cleanUrl = jdbcUrl.substring(0, jdbcUrl.indexOf("://") + 3) + jdbcUrl.substring(jdbcUrl.indexOf("@") + 1);
                String[] user = userInfo.split(":");
                return createConnection(cleanUrl, user[0], user[1]);
            }
            return DriverManager.getConnection(jdbcUrl);
        }
    }

    private static Connection createConnection(String jdbcUrl, String userName, String password) throws Exception {
        if (jdbcUrl.contains(";auth=kerberos")) {
            String client = System.getProperty("java.security.auth.login.config.client", "KerberosClient");
            LoginContext lc = new LoginContext(client, callbacks -> {
                for (Callback cb : callbacks) {
                    if (cb instanceof NameCallback) ((NameCallback) cb).setName(userName);
                    if (cb instanceof PasswordCallback) ((PasswordCallback) cb).setPassword(password.toCharArray());
                }
            });
            lc.login();
            Subject subject = lc.getSubject();
            try {
                return Subject.doAs(subject, (PrivilegedExceptionAction<Connection>) () -> DriverManager.getConnection(jdbcUrl, userName, password));
            } catch (PrivilegedActionException pae) {
                throw pae.getException();
            }
        } else {
            return DriverManager.getConnection(jdbcUrl, userName, password);
        }
    }
}
