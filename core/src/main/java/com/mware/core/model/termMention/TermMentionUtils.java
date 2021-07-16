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

import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.util.StreamUtils;
import com.mware.ge.values.storable.Values;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TermMentionUtils {
    private Graph graph;
    private VisibilityTranslator visibilityTranslator;
    private Authorizations authorizations;
    private User user;

    public TermMentionUtils(Graph graph, VisibilityTranslator visibilityTranslator, Authorizations authorizations, User user) {
        this.graph = graph;
        this.visibilityTranslator = visibilityTranslator;
        this.authorizations = authorizations;
        this.user = user;
    }

    public Vertex createTermMention(
            Vertex outVertex,
            String propertyKey,
            String propertyName,
            String tmName,
            String tmConceptType,
            int tmStartOffset,
            int tmEndOffset,
            VisibilityJson visibilityJson
    ) {
        String schemaClassName = mapToConceptType(tmConceptType);

        if (visibilityJson == null) {
            visibilityJson = new VisibilityJson(Visibility.EMPTY.getVisibilityString());
        }

        Vertex termMention = new TermMentionBuilder()
                .outVertex(outVertex)
                .propertyKey(propertyKey)
                .propertyName(propertyName)
                .start(tmStartOffset)
                .end(tmEndOffset)
                .title(tmName)
                .conceptName(schemaClassName)
                .visibilityJson(visibilityJson)
                .process(getClass().getName())
                .save(graph, visibilityTranslator, user, authorizations);

        return termMention;
    }

    public List<Element> resolveTermMentions(Vertex outVertex, List<Vertex> termMentions) {
        VisibilityJson visibilityJson = new VisibilityJson();
        visibilityJson.setSource("");

        List<Element> resolvedVertices = new ArrayList<>();
        for (Vertex termMention : termMentions) {
            String conceptType = BcSchema.TERM_MENTION_CONCEPT_TYPE.getPropertyValue(termMention);
            String tmTitle = BcSchema.TERM_MENTION_TITLE.getPropertyValue(termMention);
            if (tmTitle == null) {
                continue;
            }
            VisibilityJson outVertexVisibilityJson = new VisibilityJson();
            Metadata metadata = Metadata.create();
            BcSchema.VISIBILITY_JSON_METADATA.setMetadata(metadata, outVertexVisibilityJson, this.visibilityTranslator.getDefaultVisibility());

            Vertex resolvedToVertex = findExistingVertexWithTitle(tmTitle, authorizations);
            if (resolvedToVertex == null) {
                String id = this.graph.getIdGenerator().nextId();
                ElementMutation<Vertex> vertexMutation = this.graph.prepareVertex(id, outVertex.getVisibility(), conceptType);
                BcSchema.TITLE.addPropertyValue(vertexMutation, "NLP", tmTitle, metadata, outVertex.getVisibility());
                resolvedToVertex = vertexMutation.save(authorizations);
            }
            resolvedVertices.add(resolvedToVertex);

            String edgeId = outVertex.getId() + "-" + SchemaConstants.EDGE_LABEL_HAS_DETECTED_ENTITY + "-" + resolvedToVertex.getId();
            Edge resolvedEdge = this.graph.prepareEdge(edgeId, outVertex, resolvedToVertex, SchemaConstants.EDGE_LABEL_HAS_DETECTED_ENTITY, outVertex.getVisibility()).save(authorizations);

            String processId = getClass().getName();
            new TermMentionBuilder(termMention, outVertex)
                    .resolvedTo(resolvedToVertex, resolvedEdge)
                    .title(tmTitle)
                    .conceptName(conceptType)
                    .process(processId)
                    .resolvedFromTermMention(null)
                    .visibilityJson(BcSchema.TERM_MENTION_VISIBILITY_JSON.getPropertyValue(termMention, new VisibilityJson()))
                    .save(this.graph, this.visibilityTranslator, this.user, authorizations);
        }

        return resolvedVertices;
    }

    public void removeHasDetectedEntityRelations(Vertex sourceVertex) {
        Iterable<String> detectedEntityEdges = sourceVertex.getEdgeIds(
                Direction.OUT,
                SchemaConstants.EDGE_LABEL_HAS_DETECTED_ENTITY,
                sourceVertex.getAuthorizations()
        );
        graph.deleteElements(
                StreamUtils.stream(detectedEntityEdges).map(id -> ElementId.create(ElementType.EDGE, id)),
                sourceVertex.getAuthorizations()
        );
    }

    private Vertex findExistingVertexWithTitle(String title, Authorizations authorizations) {
        QueryResultsIterable<Vertex> existingVerticesIterable = this.graph.query(authorizations)
                .has(BcSchema.TITLE.getPropertyName(), Values.stringValue(title))
                .vertices();
        Iterator<Vertex> existingVertices = existingVerticesIterable.iterator();

        if (existingVertices.hasNext()) {
            safeClose(existingVerticesIterable);
            return existingVertices.next();
        }

        safeClose(existingVerticesIterable);
        return null;
    }

    private void safeClose(QueryResultsIterable iterable) {
        if (iterable == null) {
            return;
        }

        try {
            iterable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected String mapToConceptType(String type) {
        String conceptType = SchemaConstants.CONCEPT_TYPE_THING;
        ;
        if (SchemaConstants.CONCEPT_TYPE_LOCATION.equals(type)
                || SchemaConstants.CONCEPT_TYPE_ORGANIZATION.equals(type)
                || SchemaConstants.CONCEPT_TYPE_PERSON.equals(type)) {
            conceptType = type;
        }

        return conceptType;
    }
}
