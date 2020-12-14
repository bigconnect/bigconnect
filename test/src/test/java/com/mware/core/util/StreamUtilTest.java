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
package com.mware.core.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.ge.*;
import com.mware.ge.inmemory.InMemoryGraph;
import com.mware.ge.query.Query;
import com.mware.ge.query.QueryResultsJoinIterable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StreamUtilTest {
    private static final Authorizations EMPTY_AUTHORIZATIONS = new Authorizations();

    @Mock
    private Query query1, query2;

    @Test
    public void streamWithEmptyIteratorsShouldReturnEmptyStream() {
        Stream<Object> stream = StreamUtil.stream(
                Collections.emptyList().iterator(), Collections.emptySet().iterator());

        assertThat(stream.count(), is(0L));
    }

    @Test
    public void streamWithOneIteratorsShouldReturnStreamWithSameContentsInOrder() {
        List<String> list = ImmutableList.of("A", "B");

        Stream<String> stream = StreamUtil.stream(list.iterator());

        assertThat(
                stream.collect(Collectors.toList()),
                is(list)
        );
    }

    @Test
    public void streamWithMultipleIteratorsShouldReturnStreamWithAllContentsConcatenatedInOrder() {
        Set<String> list1 = ImmutableSet.of("A1", "B1");
        Set<String> list2 = ImmutableSet.of("A2", "B2");
        Set<String> list3 = ImmutableSet.of("A3", "B3");

        Stream<String> stream = StreamUtil.stream(list1.iterator(), list2.iterator(), list3.iterator());

        assertThat(
                stream.collect(Collectors.toList()),
                is(ImmutableList.builder().addAll(list1).addAll(list2).addAll(list3).build())
        );
    }

    @Test
    public void streamWithEmptyIterablesShouldReturnEmptyStream() {
        Stream<Object> stream = StreamUtil.stream(Collections.emptyList(), Collections.emptySet());

        assertThat(stream.count(), is(0L));
    }

    @Test
    public void streamWithOneIterableShouldReturnStreamWithSameContentsInOrder() {
        List<String> list = ImmutableList.of("A", "B");

        Stream<String> stream = StreamUtil.stream(list);

        assertThat(
                stream.collect(Collectors.toList()),
                is(list)
        );
    }

    @Test
    public void streamWithMultipleIterablesShouldReturnStreamWithAllContentsConcatenatedInOrder() {
        Set<String> list1 = ImmutableSet.of("A1", "B1");
        Set<String> list2 = ImmutableSet.of("A2", "B2");
        Set<String> list3 = ImmutableSet.of("A3", "B3");

        Stream<String> stream = StreamUtil.stream(list1, list2, list3);

        assertThat(
                stream.collect(Collectors.toList()),
                is(ImmutableList.builder().addAll(list1).addAll(list2).addAll(list3).build())
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void streamWithEmptyQueriesShouldReturnEmptyStream() {
        when(query1.elements()).thenReturn(new QueryResultsJoinIterable<>());
        when(query2.elements()).thenReturn(new QueryResultsJoinIterable<>());

        Stream<Element> stream = StreamUtil.stream(query1, query2);

        assertThat(stream.count(), is(0L));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void streamWithOneQueryShouldReturnStreamWithResultsInOrder() {
        Graph graph = InMemoryGraph.create();
        Vertex v1 = graph.addVertex("v1", Visibility.EMPTY, EMPTY_AUTHORIZATIONS, SchemaConstants.CONCEPT_TYPE_THING);
        Vertex v2 = graph.addVertex("v2", Visibility.EMPTY, EMPTY_AUTHORIZATIONS, SchemaConstants.CONCEPT_TYPE_THING);
        List<Element> list1 = ImmutableList.of(v1, v2);

        when(query1.elements()).thenReturn(new QueryResultsJoinIterable<>(list1));

        Stream<Element> stream = StreamUtil.stream(query1);

        assertThat(
                stream.map(Element::getId).collect(Collectors.toList()),
                is(ImmutableList.of("v1", "v2"))
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void streamWithMultipleQueriesShouldReturnStreamWithAllResultsConcatenatedInOrder() {
        Graph graph = InMemoryGraph.create();
        Vertex v1 = graph.addVertex("v1", Visibility.EMPTY, EMPTY_AUTHORIZATIONS, SchemaConstants.CONCEPT_TYPE_THING);
        Vertex v2 = graph.addVertex("v2", Visibility.EMPTY, EMPTY_AUTHORIZATIONS, SchemaConstants.CONCEPT_TYPE_THING);
        Vertex v3 = graph.addVertex("v3", Visibility.EMPTY, EMPTY_AUTHORIZATIONS, SchemaConstants.CONCEPT_TYPE_THING);
        Vertex v4 = graph.addVertex("v4", Visibility.EMPTY, EMPTY_AUTHORIZATIONS, SchemaConstants.CONCEPT_TYPE_THING);
        List<Element> list1 = ImmutableList.of(v1, v2);
        List<Element> list2 = ImmutableList.of(v3, v4);

        when(query1.elements()).thenReturn(new QueryResultsJoinIterable<>(list1));
        when(query2.elements()).thenReturn(new QueryResultsJoinIterable<>(list2));

        Stream<Element> stream = StreamUtil.stream(query1, query2);

        assertThat(
                stream.map(Element::getId).collect(Collectors.toList()),
                is(ImmutableList.of("v1", "v2", "v3", "v4"))
        );
    }

    @Test
    public void testToImmutableList() {
        ImmutableList<String> list = Lists.newArrayList("a", "b", "c").stream()
                .collect(StreamUtil.toImmutableList());
        assertEquals(3, list.size());
        assertEquals("a", list.get(0));
        assertEquals("b", list.get(1));
        assertEquals("c", list.get(2));
    }

    @Test
    public void testToImmutableSet() {
        ImmutableSet<String> set = Lists.newArrayList("a", "b", "c").stream()
                .collect(StreamUtil.toImmutableSet());
        assertEquals(3, set.size());
        assertEquals("a", set.toArray()[0]);
        assertEquals("b", set.toArray()[1]);
        assertEquals("c", set.toArray()[2]);
    }

    @Test
    public void testToImmutableMap() {
        ImmutableMap<String, String> map = Lists.newArrayList("a", "b", "c").stream()
                .collect(StreamUtil.toImmutableMap(v -> "k" + v, v -> "v" + v));
        assertEquals(3, map.size());
        assertEquals("va", map.get("ka"));
        assertEquals("vb", map.get("kb"));
        assertEquals("vc", map.get("kc"));
    }
}
