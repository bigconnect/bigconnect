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
import com.mware.core.GraphTestBase;
import com.mware.ge.*;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.inmemory.InMemoryGraphFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.List;
import java.util.stream.Collectors;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class TermMentionRepositoryTest extends GraphTestBase {
    private static final String WORKSPACE_ID = "WORKSPACE_1234";
    private Visibility visibility;
    private Visibility termMentionVisibility;
    private Authorizations authorizations;
    private TermMentionRepository termMentionRepository;

    @Before
    public void setUp() {
        visibility = new Visibility("");
        termMentionVisibility = new Visibility(TermMentionRepository.VISIBILITY_STRING);
        authorizations = getGraph().createAuthorizations(TermMentionRepository.VISIBILITY_STRING);

        termMentionRepository = getTermMentionRepository();
    }

    @Test
    public void testDelete() {
        Vertex v1 = getGraph().addVertex("v1", visibility, authorizations, CONCEPT_TYPE_THING);
        Vertex v1tm1 = getGraph().addVertex("v1tm1", termMentionVisibility, authorizations, CONCEPT_TYPE_THING);
        BcSchema.TERM_MENTION_RESOLVED_EDGE_ID.setProperty(v1tm1, "v1_to_v2", termMentionVisibility, authorizations);
        Vertex v2 = getGraph().addVertex("v2", visibility, authorizations, CONCEPT_TYPE_THING);
        getGraph().addEdge("v1_to_c1tm1", v1, v1tm1, BcSchema.TERM_MENTION_LABEL_HAS_TERM_MENTION, termMentionVisibility, authorizations);
        getGraph().addEdge("c1tm1_to_v2", v1tm1, v2, BcSchema.TERM_MENTION_LABEL_RESOLVED_TO, termMentionVisibility, authorizations);
        Edge e = getGraph().addEdge("v1_to_v2", v1, v2, "link", visibility, authorizations);
        VisibilityJson visibilityJson = new VisibilityJson();
        visibilityJson.addWorkspace(WORKSPACE_ID);
        BcSchema.VISIBILITY_JSON.setProperty(e, visibilityJson, new Visibility(""), authorizations);
        getGraph().flush();

        termMentionRepository.delete(v1tm1, authorizations);

        assertNull("term mention should not exist", getGraph().getVertex("v1tm1", authorizations));
        assertNull("term mention to v2 should not exist", getGraph().getEdge("c1tm1_to_v2", authorizations));
        assertNull("v1 to term mention should not exist", getGraph().getEdge("v1_to_c1tm1", authorizations));
    }

    @Test
    public void testFindResolvedToForRef() {
        Vertex v = getGraph().addVertex("v", visibility, authorizations, CONCEPT_TYPE_THING);
        VertexBuilder tmBuilder = getGraph().prepareVertex("tm", termMentionVisibility, CONCEPT_TYPE_THING);
        BcSchema.TERM_MENTION_REF_PROPERTY_KEY.setProperty(tmBuilder, "key", termMentionVisibility);
        BcSchema.TERM_MENTION_REF_PROPERTY_NAME.setProperty(tmBuilder, "name", termMentionVisibility);
        Vertex tm = tmBuilder.save(authorizations);
        getGraph().addEdge("tm_to_v", tm, v, BcSchema.TERM_MENTION_LABEL_RESOLVED_TO, termMentionVisibility, authorizations);
        getGraph().flush();

        List<Vertex> results = termMentionRepository.findResolvedToForRef(v.getId(), "key", "name", authorizations).collect(Collectors.toList());
        assertEquals(1, results.size());
        assertEquals("tm", results.get(0).getId());
    }

    @Test
    public void findResolvedToForRefElement() {
        Vertex v = getGraph().addVertex("v", visibility, authorizations, CONCEPT_TYPE_THING);
        VertexBuilder tmBuilder = getGraph().prepareVertex("tm", termMentionVisibility, CONCEPT_TYPE_THING);
        Vertex tm = tmBuilder.save(authorizations);
        getGraph().addEdge("tm_to_v", tm, v, BcSchema.TERM_MENTION_LABEL_RESOLVED_TO, termMentionVisibility, authorizations);
        getGraph().flush();

        List<Vertex> results = termMentionRepository.findResolvedToForRefElement(v.getId(), authorizations).collect(Collectors.toList());
        assertEquals(1, results.size());
        assertEquals("tm", results.get(0).getId());
    }

    @Override
    protected TestGraphFactory graphFactory() {
        return new InMemoryGraphFactory();
    }
}
