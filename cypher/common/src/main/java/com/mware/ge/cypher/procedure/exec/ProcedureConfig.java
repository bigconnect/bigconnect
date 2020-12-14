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

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.stream;

public class ProcedureConfig {
    private final String defaultValue;
    private final List<ProcMatcher> matchers;
    private List<Pattern> accessPatterns;
    private List<Pattern> whiteList;
    private final ZoneId defaultTemporalTimeZone;

    public ProcedureConfig() {
        this.defaultValue = "";
        this.matchers = Collections.emptyList();
        this.accessPatterns = Collections.emptyList();
        this.whiteList = Collections.singletonList(compilePattern("*"));
        this.defaultTemporalTimeZone = UTC;
    }

    String[] rolesFor(String procedureName) {
        String[] wildCardRoles = matchers.stream().filter(matcher -> matcher.matches(procedureName))
                .map(ProcMatcher::roles).reduce(new String[0],
                        (acc, next) -> Stream.concat(stream(acc), stream(next)).toArray(String[]::new));
        if (wildCardRoles.length > 0) {
            return wildCardRoles;
        } else {
            return getDefaultValue();
        }
    }

    boolean fullAccessFor(String procedureName) {
        return accessPatterns.stream().anyMatch(pattern -> pattern.matcher(procedureName).matches());
    }

    boolean isWhitelisted(String procedureName) {
        return whiteList.stream().anyMatch(pattern -> pattern.matcher(procedureName).matches());
    }

    private static Pattern compilePattern(String procedure) {
        procedure = procedure.trim().replaceAll("([\\[\\]\\\\?()^${}+|.])", "\\\\$1");
        return Pattern.compile(procedure.replaceAll("\\*", ".*"));
    }

    private String[] getDefaultValue() {
        return defaultValue == null || defaultValue.isEmpty() ? new String[0] : new String[]{defaultValue};
    }

    static final ProcedureConfig DEFAULT = new ProcedureConfig();

    public ZoneId getDefaultTemporalTimeZone() {
        return defaultTemporalTimeZone;
    }

    private static class ProcMatcher {
        private final Pattern pattern;
        private final String[] roles;

        private ProcMatcher(String procedurePattern, String[] roles) {
            this.pattern = Pattern.compile(procedurePattern.replaceAll("\\.", "\\\\.").replaceAll("\\*", ".*"));
            this.roles = roles;
        }

        boolean matches(String procedureName) {
            return pattern.matcher(procedureName).matches();
        }

        String[] roles() {
            return roles;
        }
    }
}
