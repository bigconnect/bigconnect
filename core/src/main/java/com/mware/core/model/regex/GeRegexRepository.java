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
package com.mware.core.model.regex;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import com.mware.core.model.clientapi.dto.ClientApiRegex;
import com.mware.core.model.clientapi.dto.ClientApiRegexes;
import com.mware.core.model.clientapi.util.StringUtils;
import com.mware.core.model.properties.RegexSchema;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.trace.Traced;
import com.mware.ge.*;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.values.storable.Values;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.mware.ge.util.IterableUtils.singleOrDefault;

public class GeRegexRepository implements RegexRepository {
    private static final String GRAPH_REGEX_ID_PREFIX = "RGX_";
    public static final BcVisibility VISIBILITY = new BcVisibility(VISIBILITY_STRING);

    private final Graph graph;
    private final com.mware.ge.Authorizations authorizations;

    private final Cache<String, Vertex> regexVertexCache = CacheBuilder.newBuilder()
            .expireAfterWrite(15, TimeUnit.SECONDS)
            .build();

    @Inject
    public GeRegexRepository(
            GraphAuthorizationRepository graphAuthorizationRepository,
            Graph graph) {
        this.graph = graph;
        graphAuthorizationRepository.addAuthorizationToGraph(VISIBILITY_STRING);
        Set<String> authorizationsSet = new HashSet<>();
        authorizationsSet.add(VISIBILITY_STRING);
        authorizationsSet.add(UserRepository.VISIBILITY_STRING);
        authorizationsSet.add(BcVisibility.SUPER_USER_VISIBILITY_STRING);
        this.authorizations = graph.createAuthorizations(authorizationsSet);

    }

    @Override
    public Iterable<Regex> getAllRegexes() {
        try (QueryResultsIterable<Vertex> vertices = graph.query(authorizations)
                .hasConceptType(RegexSchema.REGEX_CONCEPT_NAME)
                .vertices()) {
            return new ConvertingIterable<Vertex, Regex>(vertices) {
                @Override
                protected Regex convert(Vertex vertex) {
                    return createRegexFromVertex(vertex);
                }
            };
        } catch (Exception e) {
            throw new GeException("Could not close scroll iterable: ", e);
        }
    }

    @Override
    public Regex findRegexById(String regexId) {
        return createRegexFromVertex(findByIdRegexVertex(regexId, graph.getDefaultFetchHints()));
    }

    @Override
    public Regex findByName(String rgxName) {
        Iterable<Vertex> vertices = graph.query(authorizations)
                .has(RegexSchema.REGEX_NAME.getPropertyName(), Values.stringValue(rgxName))
                .hasConceptType(RegexSchema.REGEX_CONCEPT_NAME)
                .vertices();
        Vertex vertex = singleOrDefault(vertices, null);
        if (vertex == null) {
            return null;
        }
        regexVertexCache.put(vertex.getId(), vertex);
        return createRegexFromVertex(vertex);
    }


    @Override
    public Regex createRegex(String name, String pattern, String concept) {
        name = StringUtils.trimIfNull(name);
        pattern = StringUtils.trimIfNull(pattern);

        String id = GRAPH_REGEX_ID_PREFIX + graph.getIdGenerator().nextId();
        VertexBuilder builder = graph.prepareVertex(id, VISIBILITY.getVisibility(), RegexSchema.REGEX_CONCEPT_NAME);
        RegexSchema.REGEX_NAME.setProperty(builder, name, VISIBILITY.getVisibility());
        RegexSchema.REGEX_PATTERN.setProperty(builder, pattern, VISIBILITY.getVisibility());
        RegexSchema.REGEX_CONCEPT.setProperty(builder, concept, VISIBILITY.getVisibility());

        Regex regex = createRegexFromVertex(builder.save(authorizations));
        graph.flush();

        return regex;
    }

    @Override
    public void updateRegex(String id, String name, String pattern, String concept) {
        name = StringUtils.trimIfNull(name);
        pattern = StringUtils.trimIfNull(pattern);
        Vertex vertex = graph.getVertex(id, FetchHints.ALL, authorizations);
        ExistingElementMutation<Vertex> mutation = vertex.prepareMutation();

        RegexSchema.REGEX_NAME.setProperty(mutation, name, VISIBILITY.getVisibility());
        RegexSchema.REGEX_PATTERN.setProperty(mutation, pattern, VISIBILITY.getVisibility());
        RegexSchema.REGEX_CONCEPT.setProperty(mutation, concept, VISIBILITY.getVisibility());
        vertex = mutation.save(authorizations);
        regexVertexCache.put(id, vertex);
        graph.flush();
    }

    @Override
    public void deleteRegex(String id) {
        graph.deleteVertex(id, authorizations);
        graph.flush();
    }


    private GeRegex createRegexFromVertex(Vertex vertex) {
        if (vertex == null) {
            return null;
        }

        return new GeRegex(vertex);
    }


    @Traced
    private Vertex findByIdRegexVertex(String id, FetchHints fetchHints) {
        Vertex vertex = regexVertexCache.getIfPresent(id);
        if (vertex != null) {
            return vertex;
        }
        vertex = graph.getVertex(id, fetchHints, authorizations);
        if (vertex != null) {
            regexVertexCache.put(id, vertex);
        }
        return vertex;
    }



    @Override
    public ClientApiRegexes toClientApi(Iterable<Regex> regexes) {
        ClientApiRegexes clientApi = new ClientApiRegexes();
        for(Regex regex : regexes)
            clientApi.getRegexes().add(toClientApi(regex));

        return clientApi;
    }

    @Override
    public ClientApiRegex toClientApi(Regex regex) {
        ClientApiRegex clientApi = new ClientApiRegex();
        clientApi.setId(regex.getId());
        clientApi.setName(regex.getName());
        clientApi.setPattern(regex.getPattern());
        clientApi.setConcept(regex.getConcept());
        return clientApi;
    }

}
