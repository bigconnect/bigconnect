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
package com.mware.core.ingest.dataworker;

import com.google.common.base.Charsets;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.worker.InMemoryDataWorkerTestBase;
import com.mware.ge.*;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.IntValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.TextValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import static com.mware.ge.util.IterableUtils.toList;
import static org.junit.Assert.*;

public abstract class TermMentionGraphPropertyWorkerTestBase extends InMemoryDataWorkerTestBase {
    private static final String MULTI_VALUE_KEY = TermMentionGraphPropertyWorkerTestBase.class.getName();
    protected static final String CONCEPT_IRI = "test#regexGpwTest";

    public abstract DataWorker getGpw() throws Exception;

    @Override
    protected Graph getGraph() {
        Graph graph = super.getGraph();
        graph.defineProperty(BcSchema.TERM_MENTION_CONCEPT_TYPE.getPropertyName()).dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();
        graph.defineProperty(BcSchema.TERM_MENTION_VISIBILITY_JSON.getPropertyName()).dataType(TextValue.class).define();
        graph.defineProperty(BcSchema.TERM_MENTION_TITLE.getPropertyName()).dataType(TextValue.class).define();
        graph.defineProperty(BcSchema.TERM_MENTION_PROCESS.getPropertyName()).dataType(TextValue.class).define();
        graph.defineProperty(BcSchema.TERM_MENTION_PROPERTY_KEY.getPropertyName()).dataType(TextValue.class).define();
        graph.defineProperty(BcSchema.TERM_MENTION_PROPERTY_NAME.getPropertyName()).dataType(TextValue.class).define();
        graph.defineProperty(BcSchema.TERM_MENTION_START_OFFSET.getPropertyName()).dataType(IntValue.class).define();
        graph.defineProperty(BcSchema.TERM_MENTION_END_OFFSET.getPropertyName()).dataType(IntValue.class).define();
        return graph;
    }

    protected void doExtractionTest(String text, List<ExpectedTermMention> expectedTerms) throws Exception {
        VisibilityJson visibilityJson = new VisibilityJson("TermMentionGraphPropertyWorkerTestBase");
        Visibility visibility = getVisibilityTranslator().toVisibility(visibilityJson).getVisibility();
        Authorizations authorizations = getGraph().createAuthorizations("TermMentionGraphPropertyWorkerTestBase");
        Authorizations termMentionAuthorizations = getGraph().createAuthorizations(authorizations, TermMentionRepository.VISIBILITY_STRING);

        VertexBuilder vertexBuilder = getGraph().prepareVertex("v1", visibility, SchemaConstants.CONCEPT_TYPE_THING);

        Metadata textMetadata = Metadata.create();
        BcSchema.MIME_TYPE_METADATA.setMetadata(textMetadata, "text/plain", getVisibilityTranslator().getDefaultVisibility());
        StreamingPropertyValue textPropertyValue = new DefaultStreamingPropertyValue(asStream(text), TextValue.class);
        BcSchema.TEXT.addPropertyValue(vertexBuilder, MULTI_VALUE_KEY, textPropertyValue, textMetadata, visibility);

        BcSchema.VISIBILITY_JSON.setProperty(vertexBuilder, visibilityJson, getVisibilityTranslator().getDefaultVisibility());

        Vertex vertex = vertexBuilder.save(authorizations);
        Property property = vertex.getProperty(BcSchema.TEXT.getPropertyName());
        run(getGpw(), createWorkerPrepareData(), vertex, property, asStream(text));

        List<Vertex> termMentions = toList(vertex.getVertices(Direction.OUT,
                BcSchema.TERM_MENTION_LABEL_HAS_TERM_MENTION, termMentionAuthorizations));

        if (expectedTerms != null && !expectedTerms.isEmpty()) {
            assertEquals("Incorrect number of terms extracted", expectedTerms.size(), termMentions.size());

            String conceptTypes = termMentions.stream()
                    .map(BcSchema.TERM_MENTION_CONCEPT_TYPE::getPropertyValue)
                    .distinct()
                    .collect(Collectors.joining(", "));
            assertEquals("Incorrect concept types for term mentions", CONCEPT_IRI, conceptTypes);

            for (ExpectedTermMention expectedTerm : expectedTerms) {
                List<Vertex> matchingTermVertices = termMentions.stream()
                        .filter(termVertex -> expectedTerm.term.equals(BcSchema.TERM_MENTION_TITLE.getPropertyValue(termVertex)))
                        .filter(termVertex -> expectedTerm.startOffset.equals(BcSchema.TERM_MENTION_START_OFFSET.getPropertyValue(termVertex)))
                        .collect(Collectors.toList());

                if (matchingTermVertices.size() != 1) {
                    String foundTerms = termMentions.stream().map(termVertex ->
                            BcSchema.TERM_MENTION_TITLE.getPropertyValue(termVertex) + '[' +
                                    BcSchema.TERM_MENTION_START_OFFSET.getPropertyValue(termVertex) + ':' +
                                    BcSchema.TERM_MENTION_END_OFFSET.getPropertyValue(termVertex) + ']'
                    ).collect(Collectors.joining(", "));

                    fail("Unable to find expected term " + expectedTerm + ". Found: " + foundTerms);
                }
            }
        } else {
            assertTrue("Terms extracted when there were none expected", termMentions.isEmpty());
        }
    }

    private InputStream asStream(final String text) {
        return new ByteArrayInputStream(text.getBytes(Charsets.UTF_8));
    }

    public class ExpectedTermMention {
        String term;
        Long startOffset;
        Long endOffset;

        public ExpectedTermMention(String term, Long startOffset, Long endOffset) {
            this.term = term;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        @Override
        public String toString() {
            return term + '[' + startOffset + ':' + endOffset + ']';
        }
    }
}
