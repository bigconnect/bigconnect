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

import com.google.common.base.Charsets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.security.BcVisibility;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ClientApiConverter;
import com.mware.ge.*;
import com.mware.ge.mutation.EdgeMutation;

import java.time.ZonedDateTime;
import java.util.Date;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TermMentionBuilder {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(TermMentionBuilder.class);
    private static final String TERM_MENTION_VERTEX_ID_PREFIX = "TM_";
    private Vertex outVertex;
    private String propertyKey;
    private String propertyName;
    private long start = -1;
    private long end = -1;
    private String title;
    private String conceptName;
    private VisibilityJson visibilityJson;
    private String process;
    private String resolvedToVertexId;
    private String resolvedEdgeId;
    private String snippet;
    private String resolvedFromTermMention;

    public TermMentionBuilder() {

    }

    /**
     * Copy an existing term mention.
     *
     * @param existingTermMention The term mention you would like to copy.
     * @param outVertex           The vertex that contains this term mention (ie Document, Html page, etc).
     */
    public TermMentionBuilder(Vertex existingTermMention, Vertex outVertex) {
        this.outVertex = outVertex;
        this.resolvedFromTermMention = existingTermMention.getId();
        this.propertyKey = BcSchema.TERM_MENTION_PROPERTY_KEY.getPropertyValue(existingTermMention);
        this.propertyName = BcSchema.TERM_MENTION_PROPERTY_NAME.getPropertyValue(existingTermMention);
        this.start = BcSchema.TERM_MENTION_START_OFFSET.getPropertyValue(existingTermMention, 0);
        this.end = BcSchema.TERM_MENTION_END_OFFSET.getPropertyValue(existingTermMention, 0);
        this.title = BcSchema.TERM_MENTION_TITLE.getPropertyValue(existingTermMention, "");
        this.snippet = BcSchema.TERM_MENTION_SNIPPET.getPropertyValue(existingTermMention);
        this.conceptName = BcSchema.TERM_MENTION_CONCEPT_TYPE.getPropertyValue(existingTermMention, "");
        this.visibilityJson = BcSchema.TERM_MENTION_VISIBILITY_JSON.getPropertyValue(existingTermMention, new VisibilityJson());
    }

    /**
     * The start offset within the property text that this term mention appears.
     */
    public TermMentionBuilder start(long start) {
        this.start = start;
        return this;
    }

    /**
     * The end offset within the property text that this term mention appears.
     */
    public TermMentionBuilder end(long end) {
        this.end = end;
        return this;
    }

    /**
     * The property key of the {@link BcSchema#TEXT} that this term mention references.
     */
    public TermMentionBuilder propertyKey(String propertyKey) {
        this.propertyKey = propertyKey;
        return this;
    }

    /**
     * The id of term mention that this resolved vertex is resolved
     */
    public TermMentionBuilder resolvedFromTermMention(String resolvedFromFromTermMention) {
        this.resolvedFromTermMention = resolvedFromFromTermMention;
        return this;
    }

    /**
     * The property name of the {@link BcSchema#TEXT} that this term mention references.
     */
    public TermMentionBuilder propertyName(String propertyName) {
        this.propertyName = propertyName;
        return this;
    }

    /**
     * Visibility JSON string. This will be applied to the newly created term.
     */
    public TermMentionBuilder visibilityJson(String visibilityJsonString) {
        return visibilityJson(visibilityJsonStringToJson(visibilityJsonString));
    }

    /**
     * Visibility JSON object. This will be applied to the newly created term.
     */
    public TermMentionBuilder visibilityJson(VisibilityJson visibilitySource) {
        this.visibilityJson = visibilitySource;
        return this;
    }

    private static VisibilityJson visibilityJsonStringToJson(String visibilityJsonString) {
        if (visibilityJsonString == null) {
            return new VisibilityJson();
        }
        if (visibilityJsonString.length() == 0) {
            return new VisibilityJson();
        }
        return ClientApiConverter.toClientApi(visibilityJsonString, VisibilityJson.class);
    }

    /**
     * If this is a resolved term mention. This allows setting that information.
     *
     * @param resolvedToVertex The vertex this term mention resolves to.
     * @param resolvedEdge     The edge that links the source vertex to the resolved vertex.
     */
    public TermMentionBuilder resolvedTo(Vertex resolvedToVertex, Edge resolvedEdge) {
        return resolvedTo(resolvedToVertex.getId(), resolvedEdge.getId());
    }

    /**
     * If this is a resolved term mention. This allows setting that information.
     *
     * @param resolvedToVertexId The vertex id this term mention resolves to.
     * @param resolvedEdgeId     The edge id that links the source vertex to the resolved vertex.
     */
    public TermMentionBuilder resolvedTo(String resolvedToVertexId, String resolvedEdgeId) {
        this.resolvedToVertexId = resolvedToVertexId;
        this.resolvedEdgeId = resolvedEdgeId;
        return this;
    }

    /**
     * The process that created this term mention.
     */
    public TermMentionBuilder process(String process) {
        this.process = process;
        return this;
    }

    /**
     * The vertex that contains this term mention (ie Document, Html page, etc).
     */
    public TermMentionBuilder outVertex(Vertex outVertex) {
        this.outVertex = outVertex;
        return this;
    }

    /**
     * The title/text of this term mention. (ie Job Ferner, Paris, etc).
     */
    public TermMentionBuilder title(String title) {
        this.title = title;
        return this;
    }

    public TermMentionBuilder snippet(String snippet) {
        this.snippet = snippet;
        return this;
    }

    /**
     * The concept type of this term mention.
     */
    public TermMentionBuilder conceptName(String conceptName) {
        this.conceptName = conceptName;
        return this;
    }

    /**
     * Saves the term mention to the graph.
     * <p></p>
     * The resulting graph for non-resolved terms will be:
     * <p></p>
     * Source  -- Has --&gt; Term
     * Vertex             Mention
     * <p></p>
     * The resulting graph for resolved terms will be:
     * <p></p>
     * Source  -- Has --&gt; Term    -- Resolved To --&gt; Resolved
     * Vertex             Mention                    Vertex
     */
    public Vertex save(Graph graph, VisibilityTranslator visibilityTranslator, User user, Authorizations authorizations) {
        checkNotNull(outVertex, "outVertex cannot be null");
        checkNotNull(propertyKey, "propertyKey cannot be null");
        checkNotNull(title, "title cannot be null");
        checkArgument(title.length() > 0, "title cannot be an empty string");
        checkNotNull(conceptName, "conceptName cannot be null");
        checkArgument(conceptName.length() > 0, "conceptName cannot be an empty string");
        checkNotNull(visibilityJson, "visibilityJson cannot be null");
        checkNotNull(process, "process cannot be null");
        checkArgument(process.length() > 0, "process cannot be an empty string");
        checkArgument(start >= 0, "start must be greater than or equal to 0");
        checkArgument(end >= 0, "start must be greater than or equal to 0");

        if (propertyName == null) {
            LOGGER.warn("Not setting a propertyName when building a term mention is deprecated");
        }

        ZonedDateTime now = ZonedDateTime.now();
        String vertexId = createVertexId();
        Visibility defaultVisibility = visibilityTranslator.getDefaultVisibility();
        Visibility visibility = BcVisibility.and(visibilityTranslator.toVisibility(this.visibilityJson).getVisibility(), TermMentionRepository.VISIBILITY_STRING);
        VertexBuilder vertexBuilder = graph.prepareVertex(vertexId, visibility, SchemaConstants.CONCEPT_TYPE_THING);
        BcSchema.TERM_MENTION_VISIBILITY_JSON.setProperty(vertexBuilder, this.visibilityJson, visibility);
        BcSchema.TERM_MENTION_CONCEPT_TYPE.setProperty(vertexBuilder, this.conceptName, visibility);
        BcSchema.TERM_MENTION_TITLE.setProperty(vertexBuilder, this.title, visibility);
        BcSchema.TERM_MENTION_START_OFFSET.setProperty(vertexBuilder, this.start, visibility);
        BcSchema.TERM_MENTION_END_OFFSET.setProperty(vertexBuilder, this.end, visibility);
        BcSchema.TERM_MENTION_PROCESS.setProperty(vertexBuilder, this.process, visibility);
        BcSchema.TERM_MENTION_PROPERTY_KEY.setProperty(vertexBuilder, this.propertyKey, visibility);
        if (this.propertyName != null) {
            BcSchema.TERM_MENTION_PROPERTY_NAME.setProperty(vertexBuilder, this.propertyName, visibility);
        }
        if (this.resolvedEdgeId != null) {
            BcSchema.TERM_MENTION_RESOLVED_EDGE_ID.setProperty(vertexBuilder, this.resolvedEdgeId, visibility);
        }
        if (this.snippet != null) {
            BcSchema.TERM_MENTION_SNIPPET.setProperty(vertexBuilder, this.snippet, visibility);
        }
        if (this.resolvedToVertexId != null) {
            BcSchema.TERM_MENTION_FOR_ELEMENT_ID.setProperty(vertexBuilder, resolvedToVertexId, visibility);
            BcSchema.TERM_MENTION_FOR_TYPE.setProperty(vertexBuilder, TermMentionFor.VERTEX, visibility);
        }

        Authorizations termMentionAuthorizations = graph.createAuthorizations(authorizations, TermMentionRepository.VISIBILITY_STRING);
        Vertex termMentionVertex = vertexBuilder.save(termMentionAuthorizations);

        String hasTermMentionId = vertexId + "_hasTermMention";
        EdgeBuilder termMentionEdgeBuilder = graph.prepareEdge(hasTermMentionId, this.outVertex, termMentionVertex, BcSchema.TERM_MENTION_LABEL_HAS_TERM_MENTION, visibility);
        BcSchema.TERM_MENTION_VISIBILITY_JSON.setProperty(termMentionEdgeBuilder, this.visibilityJson, visibility);
        BcSchema.MODIFIED_BY.setProperty(termMentionEdgeBuilder, user.getUserId(), defaultVisibility);
        BcSchema.MODIFIED_DATE.setProperty(termMentionEdgeBuilder, now, defaultVisibility);
        termMentionEdgeBuilder.save(authorizations);
        if (this.resolvedToVertexId != null) {
            String resolvedToId = vertexId + "_resolvedTo";
            EdgeMutation resolvedToEdgeBuilder = graph.prepareEdge(resolvedToId, termMentionVertex.getId(), resolvedToVertexId, BcSchema.TERM_MENTION_LABEL_RESOLVED_TO, visibility);
            BcSchema.TERM_MENTION_VISIBILITY_JSON.setProperty(resolvedToEdgeBuilder, this.visibilityJson, visibility);
            BcSchema.MODIFIED_BY.setProperty(resolvedToEdgeBuilder, user.getUserId(), defaultVisibility);
            BcSchema.MODIFIED_DATE.setProperty(resolvedToEdgeBuilder, now, defaultVisibility);
            resolvedToEdgeBuilder.save(authorizations);

            if (this.resolvedFromTermMention != null) {
                String resolvedFromId = vertexId + "_resolvedFrom";
                EdgeMutation resolvedFromEdgeBuilder = graph.prepareEdge(resolvedFromId, termMentionVertex.getId(), resolvedFromTermMention, BcSchema.TERM_MENTION_RESOLVED_FROM, visibility);
                BcSchema.TERM_MENTION_VISIBILITY_JSON.setProperty(resolvedFromEdgeBuilder, this.visibilityJson, visibility);
                BcSchema.MODIFIED_BY.setProperty(resolvedFromEdgeBuilder, user.getUserId(), defaultVisibility);
                BcSchema.MODIFIED_DATE.setProperty(resolvedFromEdgeBuilder, now, defaultVisibility);
                resolvedFromEdgeBuilder.save(authorizations);
            }
        }

        return termMentionVertex;
    }

    private String createVertexId() {
        Hasher id = Hashing.sha1().newHasher();

        id.putUnencodedChars(this.outVertex.getId());

        if (this.visibilityJson == null) {
            LOGGER.warn ("Visibility Json should not be null");
        } else if (this.visibilityJson.getSource() != null && this.visibilityJson.getSource().length() > 0) {
            id.putString(this.visibilityJson.getSource(), Charsets.UTF_8);
        }
        if (this.propertyName != null) {
            id.putUnencodedChars(this.propertyName);
        }
        if (this.propertyKey != null) {
            id.putUnencodedChars(this.propertyKey);
        }
        if (this.title != null) {
            id.putString(this.title, Charsets.UTF_8);
        }
        if (this.process != null) {
            id.putUnencodedChars(this.process);
        }

        return TERM_MENTION_VERTEX_ID_PREFIX
                + this.start
                + "-"
                + this.end
                + "-"
                + id.hash().toString();
    }
}
