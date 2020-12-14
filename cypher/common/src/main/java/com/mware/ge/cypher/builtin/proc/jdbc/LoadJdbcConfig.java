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

import org.apache.commons.lang.StringUtils;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

/**
 * @author ab-Larus
 * @since 03-10-18
 */
public class LoadJdbcConfig {

    private ZoneId zoneId = null;

    private Credentials credentials;

    public LoadJdbcConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        try {
            this.zoneId = config.containsKey("timezone") ?
                    ZoneId.of(config.get("timezone").toString()) : null;
        } catch (DateTimeException e) {
            throw new IllegalArgumentException(String.format("The timezone field contains an error: %s", e.getMessage()));
        }
        this.credentials = config.containsKey("credentials") ? createCredentials((Map<String, String>) config.get("credentials")) : null;
    }

    public ZoneId getZoneId(){
        return this.zoneId;
    }

    public Credentials getCredentials() {
        return this.credentials;
    }

    public static Credentials createCredentials(Map<String,String> credentials) {
        if (!credentials.getOrDefault("user", StringUtils.EMPTY).equals(StringUtils.EMPTY) && !credentials.getOrDefault("password", StringUtils.EMPTY).equals(StringUtils.EMPTY)) {
            return new Credentials(credentials.get("user"), credentials.get("password"));
        } else {
            throw new IllegalArgumentException("In config param credentials must be passed both user and password.");
        }
    }

    public static class Credentials {
        private String user;

        private String password;

        public Credentials(String user, String password){
            this.user = user;

            this.password = password;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }
    }

    public boolean hasCredentials() {
        return this.credentials != null;
    }

}
