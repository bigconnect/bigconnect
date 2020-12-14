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
package com.mware.core.model.termMention;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.model.PropertyJustificationMetadata;
import com.mware.core.model.clientapi.dto.ClientApiSourceInfo;
import com.mware.core.model.clientapi.dto.ClientApiTermMentionsResponse;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ClientApiConverter;
import com.mware.core.util.SourceInfoSnippetSanitizer;
import com.mware.ge.*;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.util.FilterIterable;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.JoinIterable;
import com.mware.ge.util.StreamUtils;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.core.util.StreamUtil.stream;
import static com.mware.ge.util.IterableUtils.single;
import static com.mware.ge.util.IterableUtils.singleOrDefault;

@Singleton
public class TermMentionRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(TermMentionRepository.class);
    public static final String VISIBILITY_STRING = "termMention";
    private final Graph graph;

    @Inject
    public TermMentionRepository(Graph graph, GraphAuthorizationRepository graphAuthorizationRepository) {
        this.graph = graph;
        graphAuthorizationRepository.addAuthorizationToGraph(VISIBILITY_STRING);
    }

    public Iterable<Vertex> findByOutVertexAndProperty(
            String outVertexId,
            String propertyKey,
            String propertyName,
            Authorizations authorizations
    ) {
        authorizations = getAuthorizations(authorizations);
        return new FilterIterable<Vertex>(findByOutVertex(outVertexId, authorizations)) {
            @Override
            protected boolean isIncluded(Vertex v) {
                String vertexPropertyKey = BcSchema.TERM_MENTION_PROPERTY_KEY.getPropertyValue(v);
                if (!propertyKey.equals(vertexPropertyKey)) {
                    return false;
                }

                // handle legacy data which did not have property name
                String vertexPropertyName = BcSchema.TERM_MENTION_PROPERTY_NAME.getPropertyValue(v, null);
                if (BcSchema.TEXT.getPropertyName().equals(propertyName) && vertexPropertyName == null) {
                    return true;
                }

                return propertyName.equals(vertexPropertyName);
            }
        };
    }

    public Iterable<Vertex> findByOutVertex(String outVertexId, Authorizations authorizations) {
        Authorizations authorizationsWithTermMention = getAuthorizations(authorizations);
        Vertex outVertex = graph.getVertex(outVertexId, authorizationsWithTermMention);
        return outVertex.getVertices(
                Direction.OUT,
                BcSchema.TERM_MENTION_LABEL_HAS_TERM_MENTION,
                authorizationsWithTermMention
        );
    }

    public void deleteTermMentions(String outVertexId, Authorizations authorizations) {
        Authorizations authorizationsWithTermMention = getAuthorizations(authorizations);
        Vertex outVertex = graph.getVertex(outVertexId, authorizationsWithTermMention);
        Iterable<String> termMentionVertices = outVertex.getVertexIds(
                Direction.OUT,
                BcSchema.TERM_MENTION_LABEL_HAS_TERM_MENTION,
                authorizationsWithTermMention
        );
        graph.deleteElements(
                StreamUtils.stream(termMentionVertices).map(id -> ElementId.create(ElementType.VERTEX, id)),
                authorizationsWithTermMention
        );
    }

    /**
     * Find all term mentions connected to the vertex.
     */
    public Iterable<Vertex> findByVertexId(String vertexId, Authorizations authorizations) {
        Authorizations authorizationsWithTermMention = getAuthorizations(authorizations);
        Vertex vertex = graph.getVertex(vertexId, authorizationsWithTermMention);
        String[] labels = new String[]{
                BcSchema.TERM_MENTION_LABEL_HAS_TERM_MENTION,
                BcSchema.TERM_MENTION_LABEL_RESOLVED_TO
        };
        return vertex.getVertices(Direction.BOTH, labels, authorizationsWithTermMention);
    }

    /**
     * Find all term mentions connected to either side of the edge.
     */
    public Iterable<Vertex> findByEdge(Edge edge, Authorizations authorizations) {
        return new JoinIterable<>(
                findByVertexId(edge.getVertexId(Direction.IN), authorizations),
                findByVertexId(edge.getVertexId(Direction.OUT), authorizations)
        );
    }

    /**
     * Finds term mention vertices that were created for the justification of a new vertex.
     *
     * @param vertexId The vertex id of the vertex with the justification.
     * @return term mention vertices matching the criteria.
     */
    public Iterable<Vertex> findByVertexIdForVertex(final String vertexId, Authorizations authorizations) {
        return new FilterIterable<Vertex>(findByVertexId(vertexId, authorizations)) {
            @Override
            protected boolean isIncluded(Vertex termMention) {
                String forElementId = BcSchema.TERM_MENTION_FOR_ELEMENT_ID.getPropertyValue(termMention);
                if (forElementId == null || !forElementId.equals(vertexId)) {
                    return false;
                }

                TermMentionFor forType = BcSchema.TERM_MENTION_FOR_TYPE.getPropertyValue(termMention);
                if (forType == null || forType != TermMentionFor.VERTEX) {
                    return false;
                }

                return true;
            }
        };
    }

    /**
     * Finds term mention vertices that were created for the justification of a new edge.
     *
     * @param edge The edge id of the edge with the justification.
     * @return term mention vertices matching the criteria.
     */
    public Iterable<Vertex> findByEdgeForEdge(final Edge edge, Authorizations authorizations) {
        return new FilterIterable<Vertex>(findByEdge(edge, authorizations)) {
            @Override
            protected boolean isIncluded(Vertex termMention) {
                String forElementId = BcSchema.TERM_MENTION_FOR_ELEMENT_ID.getPropertyValue(termMention);
                if (forElementId == null || !forElementId.equals(edge.getId())) {
                    return false;
                }

                TermMentionFor forType = BcSchema.TERM_MENTION_FOR_TYPE.getPropertyValue(termMention);
                if (forType == null || forType != TermMentionFor.EDGE) {
                    return false;
                }

                return true;
            }
        };
    }

    /**
     * Finds all term mentions connected to a vertex that match propertyKey, propertyName, and propertyVisibility.
     */
    public Iterable<Vertex> findByVertexIdAndProperty(
            final String vertexId,
            final String propertyKey,
            final String propertyName,
            final Visibility propertyVisibility,
            Authorizations authorizations
    ) {
        return new FilterIterable<Vertex>(findByVertexId(vertexId, authorizations)) {
            @Override
            protected boolean isIncluded(Vertex termMention) {
                String forElementId = BcSchema.TERM_MENTION_FOR_ELEMENT_ID.getPropertyValue(termMention);
                if (forElementId == null || !forElementId.equals(vertexId)) {
                    return false;
                }
                return isTermMentionForProperty(termMention, propertyKey, propertyName, propertyVisibility);
            }
        };
    }

    /**
     * Finds all term mentions connected to either side of an edge that match propertyKey, propertyName, and propertyVisibility.
     */
    public Iterable<Vertex> findByEdgeIdAndProperty(
            final Edge edge,
            final String propertyKey,
            final String propertyName,
            final Visibility propertyVisibility,
            Authorizations authorizations
    ) {
        return new FilterIterable<Vertex>(findByEdge(edge, authorizations)) {
            @Override
            protected boolean isIncluded(Vertex termMention) {
                String forElementId = BcSchema.TERM_MENTION_FOR_ELEMENT_ID.getPropertyValue(termMention);
                if (forElementId == null || !forElementId.equals(edge.getId())) {
                    return false;
                }
                return isTermMentionForProperty(termMention, propertyKey, propertyName, propertyVisibility);
            }
        };
    }

    private boolean isTermMentionForProperty(
            Vertex termMention,
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility
    ) {
        TermMentionFor forType = BcSchema.TERM_MENTION_FOR_TYPE.getPropertyValue(termMention);
        if (forType == null || forType != TermMentionFor.PROPERTY) {
            return false;
        }

        String refPropertyKey = BcSchema.TERM_MENTION_REF_PROPERTY_KEY.getPropertyValue(termMention);
        if (refPropertyKey == null || !refPropertyKey.equals(propertyKey)) {
            return false;
        }

        String refPropertyName = BcSchema.TERM_MENTION_REF_PROPERTY_NAME.getPropertyValue(termMention);
        if (refPropertyName == null || !refPropertyName.equals(propertyName)) {
            return false;
        }

        String refPropertyVisibilityString = BcSchema.TERM_MENTION_REF_PROPERTY_VISIBILITY.getPropertyValue(
                termMention
        );
        if (refPropertyVisibilityString == null || !refPropertyVisibilityString.equals(propertyVisibility.getVisibilityString())) {
            return false;
        }

        return true;
    }

    public Vertex findById(String termMentionId, Authorizations authorizations) {
        Authorizations authorizationsWithTermMention = getAuthorizations(authorizations);
        return graph.getVertex(termMentionId, authorizationsWithTermMention);
    }

    public void updateVisibility(Vertex termMention, Visibility newVisibility, Authorizations authorizations) {
        Authorizations authorizationsWithTermMention = getAuthorizations(authorizations);
        Visibility newVisibilityWithTermMention = BcVisibility.and(newVisibility, VISIBILITY_STRING);
        ExistingElementMutation<Vertex> m = termMention.prepareMutation();
        m.alterElementVisibility(newVisibilityWithTermMention);
        for (Property property : termMention.getProperties()) {
            m.alterPropertyVisibility(property, newVisibilityWithTermMention);
        }
        Property refPropertyVisibility = BcSchema.TERM_MENTION_REF_PROPERTY_VISIBILITY.getProperty(termMention);
        if (refPropertyVisibility != null) {
            BcSchema.TERM_MENTION_REF_PROPERTY_VISIBILITY.setProperty(
                    m,
                    newVisibility.getVisibilityString(),
                    refPropertyVisibility.getMetadata(),
                    newVisibilityWithTermMention
            );
        }
        m.save(authorizationsWithTermMention);
        for (Edge edge : termMention.getEdges(Direction.BOTH, authorizationsWithTermMention)) {
            ExistingElementMutation<Edge> edgeMutation = edge.prepareMutation();
            edgeMutation.alterElementVisibility(newVisibilityWithTermMention);
            for (Property property : edge.getProperties()) {
                edgeMutation.alterPropertyVisibility(property, newVisibilityWithTermMention);
            }
            edgeMutation.save(authorizationsWithTermMention);
        }
    }

    public Iterable<Vertex> findResolvedTo(String inVertexId, Authorizations authorizations) {
        Authorizations authorizationsWithTermMention = getAuthorizations(authorizations);
        Vertex inVertex = graph.getVertex(inVertexId, authorizationsWithTermMention);
        return inVertex.getVertices(
                Direction.IN,
                BcSchema.TERM_MENTION_LABEL_RESOLVED_TO,
                authorizationsWithTermMention
        );
    }

    public Stream<Vertex> findResolvedToForRef(
            String inVertexId,
            String refPropertyKey,
            String refPropertyName,
            Authorizations authorizations
    ) {
        checkNotNull(refPropertyKey, "refPropertyKey cannot be null");
        checkNotNull(refPropertyName, "refPropertyName cannot be null");

        return stream(findResolvedTo(inVertexId, authorizations))
                .filter(vertex -> {
                    String vertexRefPropertyKey = BcSchema.TERM_MENTION_REF_PROPERTY_KEY.getPropertyValue(
                            vertex,
                            null
                    );
                    String vertexRefPropertyName = BcSchema.TERM_MENTION_REF_PROPERTY_NAME.getPropertyValue(
                            vertex,
                            null
                    );
                    return refPropertyKey.equals(vertexRefPropertyKey) && refPropertyName.equals(vertexRefPropertyName);
                });
    }

    /**
     * Gets all the resolve to term mentions for the element not a particular property.
     */
    public Stream<Vertex> findResolvedToForRefElement(String inVertexId, Authorizations authorizations) {
        return stream(findResolvedTo(inVertexId, authorizations))
                .filter(vertex -> {
                    String vertexRefPropertyKey = BcSchema.TERM_MENTION_REF_PROPERTY_KEY.getPropertyValue(
                            vertex,
                            null
                    );
                    String vertexRefPropertyName = BcSchema.TERM_MENTION_REF_PROPERTY_NAME.getPropertyValue(
                            vertex,
                            null
                    );
                    return vertexRefPropertyKey == null && vertexRefPropertyName == null;
                });
    }

    public void delete(Vertex termMention, Authorizations authorizations) {
        Authorizations authorizationsWithTermMention = getAuthorizations(authorizations);
        graph.softDeleteVertex(termMention, authorizationsWithTermMention);
    }

    public void markHidden(Vertex termMention, Visibility hiddenVisibility, Authorizations authorizations) {
        Authorizations authorizationsWithTermMention = getAuthorizations(authorizations);
        graph.markVertexHidden(termMention, hiddenVisibility, authorizationsWithTermMention);
    }

    public Iterable<Vertex> findByEdgeId(String outVertexId, final String edgeId, Authorizations authorizations) {
        Authorizations authorizationsWithTermMention = getAuthorizations(authorizations);
        Vertex outVertex = graph.getVertex(outVertexId, authorizationsWithTermMention);
        return new FilterIterable<Vertex>(outVertex.getVertices(
                Direction.OUT,
                BcSchema.TERM_MENTION_LABEL_HAS_TERM_MENTION,
                authorizationsWithTermMention
        )) {
            @Override
            protected boolean isIncluded(Vertex v) {
                String vertexEdgeId = BcSchema.TERM_MENTION_RESOLVED_EDGE_ID.getPropertyValue(v);
                return edgeId.equals(vertexEdgeId);
            }
        };
    }

    public Vertex findOutVertex(Vertex termMention, Authorizations authorizations) {
        Authorizations authorizationsWithTermMention = getAuthorizations(authorizations);
        return singleOrDefault(
                termMention.getVertices(
                        Direction.IN,
                        BcSchema.TERM_MENTION_LABEL_HAS_TERM_MENTION,
                        authorizationsWithTermMention
                ),
                null
        );
    }

    public Authorizations getAuthorizations(Authorizations authorizations) {
        return graph.createAuthorizations(authorizations, VISIBILITY_STRING);
    }

    public void addJustification(
            Vertex vertex,
            String justificationText,
            ClientApiSourceInfo sourceInfo,
            BcVisibility bcVisibility,
            Authorizations authorizations
    ) {
        if (justificationText != null) {
            removeSourceInfoEdgeFromVertex(
                    vertex.getId(),
                    vertex.getId(),
                    null,
                    null,
                    bcVisibility,
                    authorizations
            );
            BcSchema.JUSTIFICATION.setProperty(
                    vertex,
                    justificationText,
                    bcVisibility.getVisibility(),
                    authorizations
            );
        } else if (sourceInfo != null) {
            Vertex outVertex = graph.getVertex(sourceInfo.vertexId, authorizations);
            BcSchema.JUSTIFICATION.removeProperty(vertex, authorizations);
            addSourceInfoToVertex(
                    vertex,
                    sourceInfo.vertexId,
                    TermMentionFor.VERTEX,
                    null,
                    null,
                    null,
                    sourceInfo.snippet,
                    sourceInfo.textPropertyKey,
                    sourceInfo.textPropertyName,
                    sourceInfo.startOffset,
                    sourceInfo.endOffset,
                    outVertex,
                    bcVisibility.getVisibility(),
                    authorizations
            );
        }
    }

    public <T extends Element> void addSourceInfo(
            T element,
            String forElementId,
            TermMentionFor forType,
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility,
            String snippet,
            String textPropertyKey,
            String textPropertyName,
            long startOffset,
            long endOffset,
            Vertex outVertex,
            Visibility visibility,
            Authorizations authorizations
    ) {
        if (element instanceof Vertex) {
            addSourceInfoToVertex(
                    (Vertex) element,
                    forElementId,
                    forType,
                    propertyKey,
                    propertyName,
                    propertyVisibility,
                    snippet,
                    textPropertyKey,
                    textPropertyName,
                    startOffset,
                    endOffset,
                    outVertex,
                    visibility,
                    authorizations
            );
        } else {
            addSourceInfoEdgeToEdge(
                    (Edge) element,
                    forElementId,
                    forType,
                    propertyKey,
                    propertyName,
                    propertyVisibility,
                    snippet,
                    textPropertyKey,
                    textPropertyName,
                    startOffset,
                    endOffset,
                    outVertex,
                    visibility,
                    authorizations
            );
        }
    }

    public void addSourceInfoToVertex(
            Vertex vertex,
            String forElementId,
            TermMentionFor forType,
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility,
            String snippet,
            String textPropertyKey,
            String textPropertyName,
            long startOffset,
            long endOffset,
            Vertex outVertex,
            Visibility visibility,
            Authorizations authorizations
    ) {
        addSourceInfoToVertex(
                vertex,
                forElementId,
                forType,
                propertyKey,
                propertyName,
                propertyVisibility,
                null,
                snippet,
                textPropertyKey,
                textPropertyName,
                startOffset,
                endOffset,
                outVertex,
                visibility,
                authorizations
        );
    }

    public void addSourceInfoToVertex(
            Vertex vertex,
            String forElementId,
            TermMentionFor forType,
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility,
            String edgeId,
            String snippet,
            String textPropertyKey,
            String textPropertyName,
            long startOffset,
            long endOffset,
            Vertex outVertex,
            Visibility visibility,
            Authorizations authorizations
    ) {
        visibility = BcVisibility.and(visibility, VISIBILITY_STRING);
        String termMentionVertexId = vertex.getId() + "hasSource" + outVertex.getId();
        if (propertyKey != null) {
            termMentionVertexId += ":" + propertyKey;
        }
        if (propertyName != null) {
            termMentionVertexId += ":" + propertyName;
        }
        if (propertyVisibility != null) {
            termMentionVertexId += ":" + propertyVisibility;
        }
        if (edgeId != null) {
            termMentionVertexId += ":" + edgeId;
        }
        VertexBuilder m = graph.prepareVertex(termMentionVertexId, visibility, SchemaConstants.CONCEPT_TYPE_THING);
        BcSchema.TERM_MENTION_FOR_ELEMENT_ID.setProperty(m, forElementId, visibility);
        BcSchema.TERM_MENTION_FOR_TYPE.setProperty(m, forType, visibility);
        if (propertyKey != null) {
            BcSchema.TERM_MENTION_REF_PROPERTY_KEY.setProperty(m, propertyKey, visibility);
        }
        if (propertyName != null) {
            BcSchema.TERM_MENTION_REF_PROPERTY_NAME.setProperty(m, propertyName, visibility);
        }
        if (propertyVisibility != null) {
            BcSchema.TERM_MENTION_REF_PROPERTY_VISIBILITY.setProperty(
                    m,
                    propertyVisibility.getVisibilityString(),
                    visibility
            );
        }
        BcSchema.TERM_MENTION_SNIPPET.setProperty(m, snippet, visibility);
        BcSchema.TERM_MENTION_PROPERTY_KEY.setProperty(m, textPropertyKey, visibility);
        if (textPropertyName == null) {
            LOGGER.warn("not providing a property name for a term mention is deprecate");
        } else {
            BcSchema.TERM_MENTION_PROPERTY_NAME.setProperty(m, textPropertyName, visibility);
        }
        BcSchema.TERM_MENTION_START_OFFSET.setProperty(m, startOffset, visibility);
        BcSchema.TERM_MENTION_END_OFFSET.setProperty(m, endOffset, visibility);
        Vertex termMention = m.save(authorizations);

        graph.addEdge(
                BcSchema.TERM_MENTION_LABEL_HAS_TERM_MENTION + termMentionVertexId,
                outVertex,
                termMention,
                BcSchema.TERM_MENTION_LABEL_HAS_TERM_MENTION,
                visibility,
                authorizations
        );
        graph.addEdge(
                BcSchema.TERM_MENTION_LABEL_RESOLVED_TO + termMentionVertexId,
                termMention,
                vertex,
                BcSchema.TERM_MENTION_LABEL_RESOLVED_TO,
                visibility,
                authorizations
        );

        graph.flush();
        LOGGER.debug("added source info: %s", termMention.getId());
    }

    public void addSourceInfoEdgeToEdge(
            Edge edge,
            String forElementId,
            TermMentionFor forType,
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility,
            String snippet,
            String textPropertyKey,
            String textPropertyName,
            long startOffset,
            long endOffset,
            Vertex originalVertex,
            Visibility visibility,
            Authorizations authorizations
    ) {
        Vertex inVertex = edge.getVertex(Direction.IN, authorizations);
        addSourceInfoToVertex(
                inVertex,
                forElementId,
                forType,
                propertyKey,
                propertyName,
                propertyVisibility,
                edge.getId(),
                snippet,
                textPropertyKey,
                textPropertyName,
                startOffset,
                endOffset,
                originalVertex,
                visibility,
                authorizations
        );
    }

    public void removeSourceInfoEdge(
            Element element,
            String propertyKey,
            String propertyName,
            BcVisibility bcVisibility,
            Authorizations authorizations
    ) {
        if (element instanceof Vertex) {
            removeSourceInfoEdgeFromVertex(
                    element.getId(),
                    element.getId(),
                    propertyKey,
                    propertyName,
                    bcVisibility,
                    authorizations
            );
        } else {
            removeSourceInfoEdgeFromEdge((Edge) element, propertyKey, propertyName, bcVisibility, authorizations);
        }
    }

    public void removeSourceInfoEdgeFromVertex(
            String vertexId,
            String sourceInfoElementId,
            String propertyKey,
            String propertyName,
            BcVisibility bcVisibility,
            Authorizations authorizations
    ) {
        Vertex termMention = findTermMention(
                vertexId,
                sourceInfoElementId,
                propertyKey,
                propertyName,
                bcVisibility.getVisibility(),
                authorizations
        );
        if (termMention != null) {
            graph.softDeleteVertex(termMention, authorizations);
        }
    }

    public void removeSourceInfoEdgeFromEdge(
            Edge edge,
            String propertyKey,
            String propertyName,
            BcVisibility bcVisibility,
            Authorizations authorizations
    ) {
        String inVertexId = edge.getVertexId(Direction.IN);
        removeSourceInfoEdgeFromVertex(
                inVertexId,
                edge.getId(),
                propertyKey,
                propertyName,
                bcVisibility,
                authorizations
        );
    }

    private Vertex findTermMention(
            String vertexId,
            String forElementId,
            String propertyKey,
            String propertyName,
            Visibility propertyVisibility,
            Authorizations authorizations
    ) {
        Authorizations authorizationsWithTermMentions = getAuthorizations(authorizations);
        Vertex vertex = graph.getVertex(vertexId, authorizationsWithTermMentions);

        if (vertex == null) {
            return null;
        }

        List<Vertex> termMentions = IterableUtils.toList(vertex.getVertices(
                Direction.IN,
                BcSchema.TERM_MENTION_LABEL_RESOLVED_TO,
                authorizationsWithTermMentions
        ));
        for (Vertex termMention : termMentions) {
            if (forElementId != null && !forElementId.equals(
                    BcSchema.TERM_MENTION_FOR_ELEMENT_ID.getPropertyValue(termMention))) {
                continue;
            }
            if (propertyKey != null && !propertyKey.equals(
                    BcSchema.TERM_MENTION_REF_PROPERTY_KEY.getPropertyValue(termMention))) {
                continue;
            }
            if (propertyName != null && !propertyName.equals(
                    BcSchema.TERM_MENTION_REF_PROPERTY_NAME.getPropertyValue(termMention))) {
                continue;
            }
            if (propertyVisibility != null && !propertyVisibility.toString().equals(
                    BcSchema.TERM_MENTION_REF_PROPERTY_VISIBILITY.getPropertyValue(termMention))) {
                continue;
            }
            return termMention;
        }
        return null;
    }

    public ClientApiSourceInfo getSourceInfoForEdge(Edge edge, Authorizations authorizations) {
        String inVertexId = edge.getVertexId(Direction.IN);
        Vertex termMention = findTermMention(inVertexId, edge.getId(), null, null, null, authorizations);
        return getSourceInfoFromTermMention(termMention, authorizations);
    }

    public ClientApiSourceInfo getSourceInfoForVertex(Vertex vertex, Authorizations authorizations) {
        Vertex termMention = findTermMention(vertex.getId(), vertex.getId(), null, null, null, authorizations);
        return getSourceInfoFromTermMention(termMention, authorizations);
    }

    public ClientApiSourceInfo getSourceInfoForEdgeProperty(
            Edge edge,
            Property property,
            Authorizations authorizations
    ) {
        String inVertexId = edge.getVertexId(Direction.IN);
        Vertex termMention = findTermMention(
                inVertexId,
                edge.getId(),
                property.getKey(),
                property.getName(),
                property.getVisibility(),
                authorizations
        );
        return getSourceInfoFromTermMention(termMention, authorizations);
    }

    public ClientApiSourceInfo getSourceInfoForVertexProperty(
            String vertexId,
            Property property,
            Authorizations authorizations
    ) {
        Vertex termMention = findTermMention(
                vertexId,
                vertexId,
                property.getKey(),
                property.getName(),
                property.getVisibility(),
                authorizations
        );
        return getSourceInfoFromTermMention(termMention, authorizations);
    }

    private ClientApiSourceInfo getSourceInfoFromTermMention(Vertex termMention, Authorizations authorizations) {
        if (termMention == null) {
            return null;
        }
        Authorizations authorizationsWithTermMention = getAuthorizations(authorizations);
        ClientApiSourceInfo result = new ClientApiSourceInfo();
        result.vertexId = single(termMention.getVertexIds(
                Direction.IN,
                BcSchema.TERM_MENTION_LABEL_HAS_TERM_MENTION,
                authorizationsWithTermMention
        ));
        result.textPropertyKey = BcSchema.TERM_MENTION_PROPERTY_KEY.getPropertyValue(termMention);
        result.textPropertyName = BcSchema.TERM_MENTION_PROPERTY_NAME.getPropertyValue(termMention);
        result.startOffset = BcSchema.TERM_MENTION_START_OFFSET.getPropertyValue(termMention);
        result.endOffset = BcSchema.TERM_MENTION_END_OFFSET.getPropertyValue(termMention);
        result.snippet = SourceInfoSnippetSanitizer.sanitizeSnippet(
                BcSchema.TERM_MENTION_SNIPPET.getPropertyValue(termMention)
        );
        return result;
    }

    public ClientApiTermMentionsResponse toClientApi(
            Iterable<Vertex> termMentions,
            String workspaceId,
            Authorizations authorizations
    ) {
        authorizations = getAuthorizations(authorizations);
        ClientApiTermMentionsResponse termMentionsResponse = new ClientApiTermMentionsResponse();
        termMentionsResponse.getTermMentions().addAll(
                ClientApiConverter.toClientApi(termMentions, workspaceId, true, authorizations)
        );
        return termMentionsResponse;
    }
}

