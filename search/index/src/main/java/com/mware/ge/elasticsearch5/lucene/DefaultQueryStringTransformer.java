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
package com.mware.ge.elasticsearch5.lucene;

import com.mware.ge.Authorizations;
import com.mware.ge.GeException;
import com.mware.ge.Graph;
import com.mware.ge.GraphWithSearchIndex;
import com.mware.ge.elasticsearch5.Elasticsearch5SearchIndex;
import org.apache.lucene.queryparser.classic.QueryParserConstants;

import java.util.Locale;

public class DefaultQueryStringTransformer implements QueryStringTransformer {
    private final Graph graph;

    public DefaultQueryStringTransformer(Graph graph) {
        this.graph = graph;
    }

    public String transform(String queryString, Authorizations authorizations) {
        if (queryString == null) {
            return queryString;
        }
        queryString = queryString.trim();
        if (queryString.length() == 0 || "*".equals(queryString)) {
            return queryString;
        }

        LuceneQueryParser parser = new LuceneQueryParser(queryString);
        QueryStringNode node = parser.parse();
        return visit(node, authorizations);
    }

    protected String visit(QueryStringNode node, Authorizations authorizations) {
        if (node instanceof StringQueryStringNode) {
            return visitStringQueryStringNode((StringQueryStringNode) node);
        } else if (node instanceof ListQueryStringNode) {
            return visitListQueryStringNode((ListQueryStringNode) node, authorizations);
        } else if (node instanceof BooleanQueryStringNode) {
            return visitBooleanQueryStringNode((BooleanQueryStringNode) node, authorizations);
        } else if (node instanceof ClauseQueryStringNode) {
            return visitClauseQueryStringNode((ClauseQueryStringNode) node, authorizations);
        } else if (node instanceof GeSimpleNode) {
            return visitGeSimpleNode((GeSimpleNode) node);
        } else if (node instanceof MultiTermQueryStringNode) {
            return visitMultiTermQueryStringNode((MultiTermQueryStringNode) node);
        } else {
            throw new GeException("Unsupported query string node type: " + node.getClass().getName());
        }
    }

    protected String visitMultiTermQueryStringNode(MultiTermQueryStringNode node) {
        StringBuilder ret = new StringBuilder();
        boolean first = true;
        for (Token token : node.getTokens()) {
            if (!first) {
                ret.append(" ");
            }
            ret.append(token.image);
            first = false;
        }
        return ret.toString();
    }

    protected String visitGeSimpleNode(GeSimpleNode simpleNode) {
        StringBuilder ret = new StringBuilder();
        for (Token t = simpleNode.jjtGetFirstToken(); ; t = t.next) {
            if (t == null) {
                break;
            }
            if (t.kind == QueryParserConstants.RANGE_TO) {
                ret.append(" TO ");
            } else {
                ret.append(t.image);
            }
            if (t == simpleNode.jjtGetLastToken()) {
                break;
            }
        }
        return ret.toString();
    }

    protected String visitClauseQueryStringNode(ClauseQueryStringNode clauseQueryStringNode, Authorizations authorizations) {
        StringBuilder ret = new StringBuilder();
        if (clauseQueryStringNode.getField() != null) {
            String fieldName = EscapeQuerySyntax.discardEscapeChar(clauseQueryStringNode.getField().image);
            fieldName = cleanupFieldName(fieldName);

            String[] fieldNames = expandFieldName(fieldName, authorizations);
            if (fieldNames == null || fieldNames.length == 0) {
                ret.append(clauseQueryStringNode.getField().image).append(":");
            } else if (fieldNames.length == 1) {
                ret.append(EscapeQuerySyntax.escapeTerm(fieldNames[0], Locale.getDefault())).append(":");
            } else {
                boolean first = true;
                ret.append("(");
                for (String propertyName : fieldNames) {
                    if (!first) {
                        ret.append(" OR ");
                    }
                    ret.append(EscapeQuerySyntax.escapeTerm(propertyName, Locale.getDefault())).append(":");
                    visitClauseQueryStringNodeValue(clauseQueryStringNode, ret, authorizations);
                    first = false;
                }
                ret.append(")");
                return ret.toString();
            }
        }
        visitClauseQueryStringNodeValue(clauseQueryStringNode, ret, authorizations);
        return ret.toString();
    }

    protected void visitClauseQueryStringNodeValue(ClauseQueryStringNode clauseQueryStringNode, StringBuilder ret, Authorizations authorizations) {
        if (clauseQueryStringNode.isIncludeParenthesis() || clauseQueryStringNode.getBoost() != null) {
            ret.append("(")
                .append(visit(clauseQueryStringNode.getChild(), authorizations))
                .append(")");
        } else {
            ret.append(visit(clauseQueryStringNode.getChild(), authorizations));
        }

        if (clauseQueryStringNode.getBoost() != null) {
            ret.append("^").append(clauseQueryStringNode.getBoost().toString());
        }
    }

    protected String visitBooleanQueryStringNode(BooleanQueryStringNode booleanQueryStringNode, Authorizations authorizations) {
        StringBuilder ret = new StringBuilder();
        if (booleanQueryStringNode.getConjunction() != null) {
            ret.append(booleanQueryStringNode.getConjunction()).append(" ");
        }
        if (booleanQueryStringNode.getModifiers() != null) {
            ret.append(booleanQueryStringNode.getModifiers());
        }
        ret.append(visit(booleanQueryStringNode.getClause(), authorizations));
        return ret.toString();
    }

    protected String visitListQueryStringNode(ListQueryStringNode listQueryStringNode, Authorizations authorizations) {
        StringBuilder ret = new StringBuilder();
        boolean first = true;
        for (QueryStringNode child : listQueryStringNode.getChildren()) {
            if (!first) {
                ret.append(" ");
            }
            ret.append(visit(child, authorizations));
            first = false;
        }
        return ret.toString();
    }

    protected String visitStringQueryStringNode(StringQueryStringNode stringQueryStringNode) {
        return stringQueryStringNode.getValue();
    }

    protected String[] expandFieldName(String fieldName, Authorizations authorizations) {
        return getSearchIndex().getPropertyNames(graph, fieldName, authorizations);
    }

    protected String cleanupFieldName(String fieldName) {
        fieldName = fieldName.trim();
        if (fieldName.startsWith("\"") && fieldName.endsWith("\"")) {
            fieldName = fieldName.substring(1, fieldName.length() - 1);
        }
        return fieldName;
    }

    public Elasticsearch5SearchIndex getSearchIndex() {
        return (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
    }

    public Graph getGraph() {
        return graph;
    }
}
