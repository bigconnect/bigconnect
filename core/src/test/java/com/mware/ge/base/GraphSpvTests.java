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
package com.mware.ge.base;

import com.google.common.collect.Lists;
import com.mware.ge.*;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.event.GraphEventListener;
import com.mware.ge.query.Compare;
import com.mware.ge.query.TextPredicate;
import com.mware.ge.query.builder.GeQueryBuilders;
import com.mware.ge.util.IOUtils;
import com.mware.ge.util.LargeStringInputStream;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValueBase;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.query.builder.GeQueryBuilders.hasFilter;
import static com.mware.ge.util.GeAssert.*;
import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.values.storable.Values.stringValue;
import static org.junit.Assume.assumeTrue;

@RunWith(JUnit4.class)
public abstract class GraphSpvTests implements GraphTestSetup {
    protected Graph graph;

    @Before
    public void before() throws Exception {
        graph = graphFactory().createGraph();
        clearGraphEvents();
        getGraph().addGraphEventListener(new GraphEventListener() {
            @Override
            public void onGraphEvent(GraphEvent graphEvent) {
                addGraphEvent(graphEvent);
            }
        });
    }

    @After
    public void after() throws Exception {
        if (getGraph() != null) {
            getGraph().drop();
            getGraph().shutdown();
            graph = null;
        }
    }

    @Override
    public Graph getGraph() {
        return graph;
    }

    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
    @Test
    public void testAddStreamingPropertyValue() throws IOException {
        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        StreamingPropertyValueBase propSmall = StreamingPropertyValue.create(new ByteArrayInputStream("value1".getBytes()), TextValue.class, 6L);
        StreamingPropertyValueBase propLarge = StreamingPropertyValue.create(
                new ByteArrayInputStream(expectedLargeValue.getBytes()),
                TextValue.class,
                null
        );
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        Vertex v1 = getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Iterable<Value> propSmallValues = v1.getPropertyValues("propSmall");
        assertEquals(1, count(propSmallValues));
        Object propSmallValue = propSmallValues.iterator().next();
        assertTrue("propSmallValue was " + propSmallValue.getClass().getName(), propSmallValue instanceof StreamingPropertyValue);
        StreamingPropertyValue value = (StreamingPropertyValue) propSmallValue;
        assertEquals(TextValue.class, value.getValueType());
        assertEquals("value1".getBytes().length, (long) value.getLength());
        assertEquals("value1", IOUtils.toString(value.getInputStream()));

        Iterable<Value> propLargeValues = v1.getPropertyValues(largePropertyName);
        assertEquals(1, count(propLargeValues));
        Object propLargeValue = propLargeValues.iterator().next();
        assertTrue(largePropertyName + " was " + propLargeValue.getClass().getName(), propLargeValue instanceof StreamingPropertyValue);
        value = (StreamingPropertyValue) propLargeValue;
        assertEquals(TextValue.class, value.getValueType());
        assertEquals(expectedLargeValue.getBytes().length, (long) value.getLength());
        assertEquals(expectedLargeValue, IOUtils.toString(value.getInputStream()));
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        propSmallValues = v1.getPropertyValues("propSmall");
        assertEquals(1, count(propSmallValues));
        propSmallValue = propSmallValues.iterator().next();
        assertTrue("propSmallValue was " + propSmallValue.getClass().getName(), propSmallValue instanceof StreamingPropertyValue);
        value = (StreamingPropertyValue) propSmallValue;
        assertEquals(TextValue.class, value.getValueType());
        assertEquals("value1".getBytes().length, (long) value.getLength());
        assertEquals("value1", IOUtils.toString(value.getInputStream()));

        propLargeValues = v1.getPropertyValues(largePropertyName);
        assertEquals(1, count(propLargeValues));
        propLargeValue = propLargeValues.iterator().next();
        assertTrue(largePropertyName + " was " + propLargeValue.getClass().getName(), propLargeValue instanceof StreamingPropertyValue);
        value = (StreamingPropertyValue) propLargeValue;
        assertEquals(TextValue.class, value.getValueType());
        assertEquals(expectedLargeValue.getBytes().length, (long) value.getLength());
        assertEquals(expectedLargeValue, IOUtils.toString(value.getInputStream()));
    }

    @Test
    public void testStreamingPropertyValueLargeReads() throws IOException {
        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        byte[] expectedLargeValueBytes = expectedLargeValue.getBytes();
        StreamingPropertyValueBase propLarge = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), TextValue.class);
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("propLarge", propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        StreamingPropertyValue prop = (StreamingPropertyValue) v1.getPropertyValue("propLarge");
        byte[] buffer = new byte[LARGE_PROPERTY_VALUE_SIZE * 2];
        int leftToRead = expectedLargeValueBytes.length;
        InputStream in = prop.getInputStream();
        for (int expectedOffset = 0; expectedOffset < expectedLargeValueBytes.length; ) {
            int sizeRead = in.read(buffer);

            for (int j = 0; j < sizeRead; j++, expectedOffset++, leftToRead--) {
                assertEquals("invalid data at offset " + expectedOffset, expectedLargeValueBytes[expectedOffset], buffer[j]);
            }

            if (sizeRead == -1) {
                break;
            }

        }
        assertEquals(0, leftToRead);
        assertEquals(-1, in.read(buffer));
    }

    @Test
    public void testStreamingPropertyDecreasingSize() throws IOException {
        Metadata metadata = Metadata.create();
        Long timestamp = System.currentTimeMillis();
        String expectedValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        StreamingPropertyValueBase propLarge = StreamingPropertyValue.create(
                new ByteArrayInputStream(expectedValue.getBytes()),
                TextValue.class,
                (long) expectedValue.length()
        );
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "largeProp", propLarge, metadata, timestamp, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        StreamingPropertyValue spv = (StreamingPropertyValue) v1.getPropertyValue("key1", "largeProp");
        assertEquals(expectedValue, spv.readToString());

        // now save a smaller value, making sure it gets truncated
        expectedValue = "small";
        propLarge = StreamingPropertyValue.create(
                new ByteArrayInputStream(expectedValue.getBytes()),
                TextValue.class,
                (long) expectedValue.length()
        );
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .addPropertyValue("key1", "largeProp", propLarge, metadata, timestamp + 1, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        spv = (StreamingPropertyValue) v1.getPropertyValue("key1", "largeProp");
        assertEquals(expectedValue, spv.readToString());
    }

    @Test
    public void testStreamingPropertyValueMarkReset() throws IOException {
        assumeTrue("InputStream mark/reset is not supported", isInputStreamMarkResetSupported());

        String expectedLargeValue = "abcdefghijk";
        StreamingPropertyValueBase propLarge = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), TextValue.class);
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("propLarge", propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        StreamingPropertyValue prop = (StreamingPropertyValue) v1.getPropertyValue("propLarge");

        InputStream in = prop.getInputStream();

        byte[] buffer = new byte[15];
        int sizeRead = in.read(buffer);
        assertEquals(11, sizeRead);
        assertEquals("abcdefghijk", new String(buffer, 0, sizeRead));

        in.reset();
        buffer = new byte[3];
        sizeRead = in.read(buffer);
        assertEquals(3, sizeRead);
        assertEquals("abc", new String(buffer, 0, sizeRead));
        assertEquals('d', (char) in.read());
        assertEquals('e', (char) in.read());

        in.mark(32);
        buffer = new byte[5];
        sizeRead = in.read(buffer);
        assertEquals(5, sizeRead);
        assertEquals("fghij", new String(buffer, 0, sizeRead));

        in.reset();
        buffer = new byte[10];
        sizeRead = in.read(buffer);
        assertEquals(6, sizeRead);
        assertEquals("fghijk", new String(buffer, 0, sizeRead));

        assertEquals(-1, in.read(buffer));

        in.reset();
        buffer = new byte[10];
        sizeRead = in.read(buffer);
        assertEquals(6, sizeRead);
        assertEquals("fghijk", new String(buffer, 0, sizeRead));
    }

    @Test
    public void testStreamingPropertyValueMarkResetLargeReads() throws IOException {
        assumeTrue("InputStream mark/reset is not supported", isInputStreamMarkResetSupported());

        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        byte[] expectedLargeValueBytes = expectedLargeValue.getBytes();
        StreamingPropertyValueBase propLarge = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), TextValue.class);
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("propLarge", propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        StreamingPropertyValue prop = (StreamingPropertyValue) v1.getPropertyValue("propLarge");

        InputStream in = prop.getInputStream();
        int amountToRead = expectedLargeValueBytes.length - 8;
        byte[] buffer = null;
        while (amountToRead > 0) {
            buffer = new byte[amountToRead];
            amountToRead -= in.read(buffer);
        }

        assertEquals(expectedLargeValue.charAt(expectedLargeValue.length() - 9), (char) buffer[buffer.length - 1]);
        in.mark(32);
        buffer = new byte[2];
        int sizeRead = in.read(buffer);
        assertEquals(2, sizeRead);
        assertEquals(expectedLargeValue.charAt(expectedLargeValue.length() - 8), (char) buffer[0]);
        assertEquals(expectedLargeValue.charAt(expectedLargeValue.length() - 7), (char) buffer[1]);
        assertEquals(expectedLargeValue.charAt(expectedLargeValue.length() - 6), (char) in.read());

        in.reset();
        sizeRead = in.read(buffer);
        assertEquals(2, sizeRead);
        assertEquals(expectedLargeValue.charAt(expectedLargeValue.length() - 8), (char) buffer[0]);
    }

    @Test
    public void testStreamingPropertyValueResetMutlipleLargeReadsUntilEnd() throws IOException {
        assumeTrue("InputStream mark/reset is not supported", isInputStreamMarkResetSupported());

        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        byte[] expectedLargeValueBytes = expectedLargeValue.getBytes();
        StreamingPropertyValueBase propLarge = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), TextValue.class);
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("propLarge", propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        StreamingPropertyValue prop = (StreamingPropertyValue) v1.getPropertyValue("propLarge");

        InputStream in = prop.getInputStream();
        in.mark(2);
        for (int i = 0; i < 3; i++) {
            int totalBytesRead = 0;
            while (in.read() >= 0) {
                totalBytesRead++;
                assertTrue("Read past end of input stream", totalBytesRead <= expectedLargeValueBytes.length);
            }
            assertEquals("Read unexpected number of bytes on loop: " + i, expectedLargeValueBytes.length, totalBytesRead);
            assertEquals(-1, in.read());
            in.reset();
        }
    }

    @Test
    public void testTextIndexStreamingPropertyValue() throws Exception {
        getGraph().defineProperty("none").dataType(TextValue.class).textIndexHint(TextIndexHint.NONE).define();
        getGraph().defineProperty("both").dataType(TextValue.class).textIndexHint(TextIndexHint.ALL).define();
        getGraph().defineProperty("fullText").dataType(TextValue.class).textIndexHint(TextIndexHint.FULL_TEXT).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("none", StreamingPropertyValue.create("Test Value"), VISIBILITY_A)
                .setProperty("both", StreamingPropertyValue.create("Test Value"), VISIBILITY_A)
                .setProperty("fullText", StreamingPropertyValue.create("Test Value"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        assertEquals(1, count(getGraph().query(hasFilter("both", TextPredicate.CONTAINS, stringValue("Test")), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("fullText", TextPredicate.CONTAINS, stringValue("Test")), AUTHORIZATIONS_A).vertices()));
        assertEquals("un-indexed property shouldn't match partials", 0, count(getGraph().query(hasFilter("none", Compare.EQUAL, stringValue("Test")), AUTHORIZATIONS_A).vertices()));
        assertEquals(1, count(getGraph().query(hasFilter("none", TextPredicate.CONTAINS, stringValue("Test")), AUTHORIZATIONS_A).vertices()));
    }

    @Test
    public void testQueryingUpdatedStreamingPropertyValues() throws Exception {
        getGraph().defineProperty("fullText")
                .dataType(TextValue.class)
                .textIndexHint(TextIndexHint.FULL_TEXT)
                .define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("fullText", StreamingPropertyValue.create("Test Value"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Assert.assertEquals(
                1,
                count(getGraph().query(hasFilter("fullText", TextPredicate.CONTAINS, stringValue("Test")), AUTHORIZATIONS_A).vertices())
        );

        getGraph().getVertex("v1", AUTHORIZATIONS_A)
                .prepareMutation()
                .setProperty("fullText", StreamingPropertyValue.create("Updated Test Value 111"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        Assert.assertEquals(
                1,
                count(getGraph().query(hasFilter("fullText", TextPredicate.CONTAINS, stringValue("111")), AUTHORIZATIONS_A).vertices())
        );

        Vertex v = getGraph().getVertex("v1", AUTHORIZATIONS_A);
        v.prepareMutation()
                .setProperty("fullText", StreamingPropertyValue.create("Updated Test Value - existing mutation"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        Assert.assertEquals(
                1,
                count(getGraph().query(hasFilter("fullText", TextPredicate.CONTAINS, stringValue("mutation")), AUTHORIZATIONS_A).vertices())
        );
    }

    @Test
    public void testGetStreamingPropertyValueInputStreams() throws Exception {
        getGraph().defineProperty("a").dataType(TextValue.class).textIndexHint(TextIndexHint.FULL_TEXT).define();
        getGraph().defineProperty("b").dataType(TextValue.class).textIndexHint(TextIndexHint.FULL_TEXT).define();
        getGraph().defineProperty("c").dataType(TextValue.class).textIndexHint(TextIndexHint.FULL_TEXT).define();

        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("a", StreamingPropertyValue.create("Test Value A"), VISIBILITY_A)
                .setProperty("b", StreamingPropertyValue.create("Test Value B"), VISIBILITY_A)
                .setProperty("c", StreamingPropertyValue.create("Test Value C"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();

        Vertex v1 = getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B);
        StreamingPropertyValue spvA = (StreamingPropertyValue) v1.getPropertyValue("a");
        assertEquals(12L, (long) spvA.getLength());
        StreamingPropertyValue spvB = (StreamingPropertyValue) v1.getPropertyValue("b");
        assertEquals(12L, (long) spvA.getLength());
        StreamingPropertyValue spvC = (StreamingPropertyValue) v1.getPropertyValue("c");
        assertEquals(12L, (long) spvA.getLength());
        ArrayList<StreamingPropertyValue> spvs = Lists.newArrayList(spvA, spvB, spvC);
        List<InputStream> streams = getGraph().getStreamingPropertyValueInputStreams(spvs);
        assertEquals("Test Value A", IOUtils.toString(streams.get(0)));
        assertEquals("Test Value B", IOUtils.toString(streams.get(1)));
        assertEquals("Test Value C", IOUtils.toString(streams.get(2)));
    }

    @Test
    public void testChangeVisibilityOnStreamingProperty() throws IOException {
        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        StreamingPropertyValue propSmall = StreamingPropertyValue.create(new ByteArrayInputStream("value1".getBytes()), TextValue.class);
        StreamingPropertyValue propLarge = StreamingPropertyValue.create(new ByteArrayInputStream(expectedLargeValue.getBytes()), TextValue.class);
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        getGraph().prepareVertex("v1", VISIBILITY_A, CONCEPT_TYPE_THING)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(2, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A)
                .prepareMutation()
                .alterPropertyVisibility("propSmall", VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(1, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        getGraph().getVertex("v1", FetchHints.ALL, AUTHORIZATIONS_A)
                .prepareMutation()
                .alterPropertyVisibility(largePropertyName, VISIBILITY_B)
                .save(AUTHORIZATIONS_A_AND_B);
        getGraph().flush();
        assertEquals(0, count(getGraph().getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        assertEquals(2, count(getGraph().getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties()));
    }
}
