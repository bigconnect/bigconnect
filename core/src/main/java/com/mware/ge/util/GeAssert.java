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
package com.mware.ge.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.mware.ge.*;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.metric.StackTraceTracker;
import com.mware.ge.query.IterableWithTotalHits;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.values.storable.Value;
import junit.framework.AssertionFailedError;
import org.junit.Assert;

import java.lang.reflect.Executable;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.mware.ge.util.GraphTestContext.eventsEnabled;
import static com.mware.ge.util.GraphTestContext.proxiesEnabled;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.util.StreamUtils.stream;

public class GeAssert {
    protected final static List<GraphEvent> graphEvents = new ArrayList<>();

    public static void assertEquals(Object o1, Object o2) {
        if (o1 == null || o2 == null) {
            Assert.assertEquals(o1, o2);
            return;
        }

        if (proxiesEnabled()) {
            if (Proxy.isProxyClass(o1.getClass()) && Proxy.isProxyClass(o2.getClass())) {
                if (o1 instanceof Element && o2 instanceof Element) {
                    Assert.assertEquals(((Element) o1).getId(), ((Element) o2).getId());
                    return;
                }

                throw new UnsupportedOperationException("The provided proxy is unrecognized.");
            }
        }

        Assert.assertEquals(o1, o2);
    }

    public static void assertNotEquals(Object o1, Object o2) {
        if (proxiesEnabled()) {
            if (Proxy.isProxyClass(o1.getClass()) && Proxy.isProxyClass(o2.getClass())) {
                if (o1 instanceof Element && o2 instanceof Element) {
                    Assert.assertNotEquals(((Element) o1).getId(), ((Element) o2).getId());
                    return;
                }

                throw new UnsupportedOperationException("The provided proxy is unrecognized.");
            }
        }

        Assert.assertNotEquals(o1, o2);
    }

    public static void assertEquals(double d1, double d2, double d3) {
        Assert.assertEquals(d1, d2, d3);
    }

    public static void assertEquals(long l1, long l2) {
        Assert.assertEquals(l1, l2);
    }

    public static void assertEquals(String msg, Object o1, Object o2) {
        Assert.assertEquals(msg, o1, o2);
    }

    public static void assertEquals(String msg, long o1, long o2) {
        Assert.assertEquals(msg, o1, o2);
    }

    public static void assertNotNull(Object o) {
        Assert.assertNotNull(o);
    }

    public static void assertNotNull(String msg, Object o) {
        Assert.assertNotNull(msg, o);
    }

    public static void assertNull(Object o) {
        Assert.assertNull(o);
    }

    public static void assertNull(String msg, Object o) {
        Assert.assertNull(msg, o);
    }

    public static void assertTrue(boolean o) {
        Assert.assertTrue(o);
    }

    public static void assertFalse(boolean o) {
        Assert.assertFalse(o);
    }

    public static void assertTrue(String msg, boolean o) {
        Assert.assertTrue(msg, o);
    }

    public static void assertFalse(String msg, boolean o) {
        Assert.assertFalse(msg, o);
    }

    public static void fail(String msg) {
        Assert.fail(msg);
    }

    public static void assertArrayEquals(String msg, Object[] expecteds,
                                         Object[] actuals) {
        Assert.assertArrayEquals(msg, expecteds, actuals);
    }

    public static void assertArrayEquals(Object[] expecteds,
                                         Object[] actuals) {
        Assert.assertArrayEquals(expecteds, actuals);
    }

    public static void assertIdsAnyOrder(Iterable<String> ids, String... expectedIds) {
        List<String> sortedIds = stream(ids).sorted().collect(Collectors.toList());
        Arrays.sort(expectedIds);

        String idsString = idsToString(sortedIds.toArray(new String[sortedIds.size()]));
        String expectedIdsString = idsToString(expectedIds);
        assertEquals("ids length mismatch found:[" + idsString + "] expected:[" + expectedIdsString + "]", expectedIds.length, sortedIds.size());
        for (int i = 0; i < expectedIds.length; i++) {
            assertEquals("at offset: " + i + " found:[" + idsString + "] expected:[" + expectedIdsString + "]", expectedIds[i], sortedIds.get(i));
        }
    }

    public static void assertVertexIdsAnyOrder(Iterable<Vertex> vertices, String... expectedIds) {
        if (vertices instanceof QueryResultsIterable) {
            assertEquals(expectedIds.length, ((QueryResultsIterable<Vertex>) vertices).getTotalHits());
        }
        assertElementIdsAnyOrder(vertices, expectedIds);
    }

    public static void assertVertexIds(Iterable<Vertex> vertices, String... expectedIds) {
        assertElementIds(vertices, expectedIds);
    }

    public static String idsToString(String[] ids) {
        return Joiner.on(", ").join(ids);
    }

    public static String idsToStringSorted(Iterable<String> ids) {
        ArrayList<String> idsList = Lists.newArrayList(ids);
        Collections.sort(idsList);
        return Joiner.on(", ").join(idsList);
    }

    public static void assertEvents(GraphEvent... expectedEvents) {
        if (eventsEnabled()) {
            Assert.assertEquals("Different number of events occurred than were asserted", expectedEvents.length, graphEvents.size());

            for (int i = 0; i < expectedEvents.length; i++) {
                Assert.assertEquals(expectedEvents[i], graphEvents.get(i));
            }
        }
    }

    public static void assertEdgeIdsAnyOrder(Iterable<Edge> edges, String... expectedIds) {
        assertElementIdsAnyOrder(edges, expectedIds);
    }

    public static void assertEdgeIds(Iterable<Edge> edges, String... ids) {
        assertElementIds(edges, ids);
    }

    public static void assertElementIdsAnyOrder(Iterable<? extends Element> elements, String... expectedIds) {
        List<Element> sortedElements = stream(elements)
                .sorted(Comparator.comparing(Element::getId))
                .collect(Collectors.toList());
        Arrays.sort(expectedIds);
        assertElementIds(sortedElements, expectedIds);
    }

    public static void assertElementIds(Iterable<? extends Element> elements, String... ids) {
        String found = stream(elements).map(Element::getId).collect(Collectors.joining(", "));
        String expected = Joiner.on(", ").join(ids);
        assertEquals(expected, found);
    }


    public static void assertResultsCount(int expectedCountAndTotalHits, QueryResultsIterable<? extends Element> results) {
        Assert.assertEquals(expectedCountAndTotalHits, results.getTotalHits());
        assertCount(expectedCountAndTotalHits, results);
    }

    public static void assertResultsCount(
            int expectedCount,
            int expectedTotalHits,
            IterableWithTotalHits<?> results
    ) {
        Assert.assertEquals(expectedTotalHits, results.getTotalHits());
        assertCount(expectedCount, results);
    }

    private static void assertCount(int expectedCount, Iterable<?> results) {
        int count = 0;
        Iterator<?> it = results.iterator();
        while (it.hasNext()) {
            count++;
            it.next();
        }
        Assert.assertEquals(expectedCount, count);
        Assert.assertFalse(it.hasNext());
        try {
            it.next();
            throw new GeException("Should throw NoSuchElementException: " + it.getClass().getName());
        } catch (NoSuchElementException ex) {
            // OK
        }
    }

    public static void addGraphEvent(GraphEvent graphEvent) {
        graphEvents.add(graphEvent);
    }

    public static void clearGraphEvents() {
        graphEvents.clear();
    }

    public static void assertRowIdsAnyOrder(Iterable<ExtendedDataRow> rows, String... expectedRowIds) {
        List<String> foundRowIds = getRowIds(rows);
        assertEquals(idsToStringSorted(Lists.newArrayList(expectedRowIds)), idsToStringSorted(foundRowIds));
        if (rows instanceof QueryResultsIterable) {
            assertEquals(
                    "search index total hits mismatch",
                    expectedRowIds.length,
                    ((QueryResultsIterable<ExtendedDataRow>) rows).getTotalHits()
            );
        }
    }

    public static void assertRowIdsAnyOrder(Iterable<String> expectedRowIds, Iterable<? extends GeObject> searchResults) {
        List<String> foundRowIds = getRowIds(searchResults);
        Assert.assertEquals(idsToStringSorted(expectedRowIds), idsToStringSorted(foundRowIds));
    }

    public static void assertRowIds(Iterable<String> expectedRowIds, Iterable<? extends GeObject> searchResults) {
        List<String> foundRowIds = getRowIds(searchResults);
        Assert.assertEquals(expectedRowIds, foundRowIds);
    }

    private static List<String> getRowIds(Iterable<? extends GeObject> searchResults) {
        return stream(searchResults)
                .filter((sr) -> sr instanceof ExtendedDataRow)
                .map((sr) -> ((ExtendedDataRow) sr).getId().getRowId())
                .collect(Collectors.toList());
    }

    public static void assertThrowsException(Runnable fn) {
        try {
            fn.run();
        } catch (Throwable ex) {
            return;
        }
        fail("Should have thrown an exception");
    }

    public static <T extends Throwable> T assertThrowsException(Class expectedType, Executable fn) {
        try {
            fn.execute();
        } catch (Throwable ex) {
            if (expectedType.isInstance(ex)) {
                return (T) ex;
            } else {
                fail("Unexpected exception type thrown");
            }
        }

        fail(String.format("Expected %s to be thrown, but nothing was thrown.", expectedType.getName()));
        throw new AssertionFailedError();
    }

    public interface Executable {
        void execute() throws Throwable;
    }

    public static void assertStackTraceTrackerCount(StackTraceTracker tracker, Consumer<List<StackTraceTracker.StackTraceItem>> validate) {
        assertStackTraceTrackerCount(tracker.getRoots(), new ArrayList<>(), validate);
    }

    public static void assertStackTraceTrackerCount(
            Set<StackTraceTracker.StackTraceItem> roots,
            List<StackTraceTracker.StackTraceItem> path,
            Consumer<List<StackTraceTracker.StackTraceItem>> validate
    ) {
        for (StackTraceTracker.StackTraceItem item : roots) {
            List<StackTraceTracker.StackTraceItem> newPath = new ArrayList<>(path);
            newPath.add(item);
            validate.accept(newPath);
            assertStackTraceTrackerCount(item.getChildren(), newPath, validate);
        }
    }
}
