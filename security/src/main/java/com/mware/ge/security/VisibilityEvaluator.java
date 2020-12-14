/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mware.ge.security;

import java.util.ArrayList;

/**
 * A class which evaluates visibility expressions against a set of authorizations.
 */
public class VisibilityEvaluator {
    private AuthorizationContainer auths;

    /**
     * Creates a new {@link SecurityAuthorizations} object with escaped forms of the
     * authorizations in the given object.
     *
     * @param auths original authorizations
     * @return authorizations object with escaped authorization strings
     * @see #escape(byte[], boolean)
     */
    static SecurityAuthorizations escape(SecurityAuthorizations auths) {
        ArrayList<byte[]> retAuths = new ArrayList<byte[]>(auths.getAuthorizations().size());

        for (byte[] auth : auths.getAuthorizations())
            retAuths.add(escape(auth, false));

        return new SecurityAuthorizations(retAuths);
    }

    /**
     * Properly escapes an authorization string. The string can be quoted if
     * desired.
     *
     * @param auth  authorization string, as UTF-8 encoded bytes
     * @param quote true to wrap escaped authorization in quotes
     * @return escaped authorization string
     */
    public static byte[] escape(byte[] auth, boolean quote) {
        int escapeCount = 0;

        for (int i = 0; i < auth.length; i++)
            if (auth[i] == '"' || auth[i] == '\\')
                escapeCount++;

        if (escapeCount > 0 || quote) {
            byte[] escapedAuth = new byte[auth.length + escapeCount + (quote ? 2 : 0)];
            int index = quote ? 1 : 0;
            for (int i = 0; i < auth.length; i++) {
                if (auth[i] == '"' || auth[i] == '\\')
                    escapedAuth[index++] = '\\';
                escapedAuth[index++] = auth[i];
            }

            if (quote) {
                escapedAuth[0] = '"';
                escapedAuth[escapedAuth.length - 1] = '"';
            }

            auth = escapedAuth;
        }
        return auth;
    }

    /**
     * Creates a new evaluator for the given collection of authorizations.
     * Each authorization string is escaped before handling, and the original
     * strings are unchanged.
     *
     * @param authorizations authorizations object
     */
    public VisibilityEvaluator(SecurityAuthorizations authorizations) {
        this.auths = escape((SecurityAuthorizations) authorizations);
    }

    /**
     * Evaluates the given column visibility against the authorizations provided to this evaluator.
     * A visibility passes evaluation if all authorizations in it are contained in those known to the evaluator, and
     * all AND and OR subexpressions have at least two children.
     *
     * @param visibility column visibility to evaluate
     * @return true if visibility passes evaluation
     * @throws VisibilityParseException if an AND or OR subexpression has less than two children, or a subexpression is of an unknown type
     */
    public boolean evaluate(ColumnVisibility visibility) throws VisibilityParseException {
        // The VisibilityEvaluator computes a trie from the given Authorizations, that ColumnVisibility expressions can be evaluated against.
        return evaluate(visibility.getExpression(), visibility.getParseTree());
    }

    private final boolean evaluate(final byte[] expression, final ColumnVisibility.Node root) throws VisibilityParseException {
        if (expression.length == 0)
            return true;
        switch (root.type) {
            case TERM:
                return auths.contains(root.getTerm(expression));
            case AND:
                if (root.children == null || root.children.size() < 2)
                    throw new VisibilityParseException("AND has less than 2 children", expression, root.start);
                for (ColumnVisibility.Node child : root.children) {
                    if (!evaluate(expression, child))
                        return false;
                }
                return true;
            case OR:
                if (root.children == null || root.children.size() < 2)
                    throw new VisibilityParseException("OR has less than 2 children", expression, root.start);
                for (ColumnVisibility.Node child : root.children) {
                    if (evaluate(expression, child))
                        return true;
                }
                return false;
            default:
                throw new VisibilityParseException("No such node type", expression, root.start);
        }
    }
}
