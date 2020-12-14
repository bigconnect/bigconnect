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
package com.mware.ge.values.utils;

import com.mware.ge.Authorizations;
import com.mware.ge.type.GeoPoint;
import com.mware.ge.values.AnyValueWriter;
import com.mware.ge.values.storable.TextArray;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.virtual.MapValue;
import com.mware.ge.values.virtual.NodeValue;
import com.mware.ge.values.virtual.RelationshipValue;

import java.time.*;
import java.util.ArrayDeque;
import java.util.Deque;

import static java.lang.String.format;

/**
 * Pretty printer for AnyValues.
 * <p>
 * Used to format AnyValue as a json-like string, as following:
 * <ul>
 * <li>nodes: <code>(id=42 :LABEL {prop1: ["a", 13]})</code></li>
 * <li>edges: <code>-[id=42 :TYPE {prop1: ["a", 13]}]-</code></li>
 * <li>paths: <code>(id=1 :L)-[id=42 :T {k: "v"}]->(id=2)-...</code></li>
 * <li>points are serialized to geojson</li>
 * <li>maps: <code>{foo: 42, bar: "baz"}</code></li>
 * <li>lists and arrays: <code>["aa", "bb", "cc"]</code></li>
 * <li>Numbers: <code>2.7182818285</code></li>
 * <li>Strings: <code>"this is a string"</code></li>
 * </ul>
 */
public class PrettyPrinter implements AnyValueWriter<RuntimeException> {
    private final Deque<Writer> stack = new ArrayDeque<>();
    private final String quoteMark;

    public PrettyPrinter() {
        this("\"");
    }

    public PrettyPrinter(String quoteMark) {
        this.quoteMark = quoteMark;
        stack.push(new ValueWriter());
    }

    @Override
    public void writeNodeReference(String nodeId) {
        append(format("(id=%s)", nodeId));
    }

    @Override
    public void writeNode(String nodeId, TextArray labels, MapValue properties, Authorizations authorizations) {
        append(format("(id=%s", nodeId));
        String sep = " ";
        for (int i = 0; i < labels.length(); i++) {
            append(sep);
            append(":" + labels.stringValue(i));
            sep = "";
        }
        if (properties.size() > 0) {
            append(" ");
            properties.writeTo(this, authorizations);
        }

        append(")");
    }

    @Override
    public void writeRelationshipReference(String relId) {
        append(format("-[id=%s]-", relId));
    }

    @Override
    public void writeRelationship(String relId, String startNodeId, String endNodeId, TextValue type, MapValue properties, Authorizations authorizations) {
        append(format("-[id=%s :%s", relId, type.stringValue()));
        if (properties.size() > 0) {
            append(" ");
            properties.writeTo(this, authorizations);
        }
        append("]-");
    }

    @Override
    public void beginMap(int size) {
        stack.push(new MapWriter());
    }

    @Override
    public void endMap() {
        assert !stack.isEmpty();
        append(stack.pop().done());
    }

    @Override
    public void beginList(int size) {
        stack.push(new ListWriter());
    }

    @Override
    public void endList() {
        assert !stack.isEmpty();
        append(stack.pop().done());
    }

    @Override
    public void writePath(NodeValue[] nodes, RelationshipValue[] relationships, Authorizations authorizations) {
        if (nodes.length == 0) {
            return;
        }
        //Path guarantees that nodes.length = edges.length = 1
        nodes[0].writeTo(this, authorizations);
        for (int i = 0; i < relationships.length; i++) {
            relationships[i].writeTo(this, authorizations);
            append(">");
            nodes[i + 1].writeTo(this, authorizations);
        }

    }

    @Override
    public void writeDuration(long months, long days, long seconds, int nanos) throws RuntimeException {
        append("{duration: {months: ");
        append(Long.toString(months));
        append(", days: ");
        append(Long.toString(days));
        append(", seconds: ");
        append(Long.toString(seconds));
        append(", nanos: ");
        append(Long.toString(nanos));
        append("}}");
    }

    @Override
    public void writeDate(LocalDate localDate) throws RuntimeException {
        append("{date: ");
        append(quote(localDate.toString()));
        append("}");
    }

    @Override
    public void writeLocalTime(LocalTime localTime) throws RuntimeException {
        append("{localTime: ");
        append(quote(localTime.toString()));
        append("}");
    }

    @Override
    public void writeTime(OffsetTime offsetTime) throws RuntimeException {
        append("{time: ");
        append(quote(offsetTime.toString()));
        append("}");
    }

    @Override
    public void writeLocalDateTime(LocalDateTime localDateTime) throws RuntimeException {
        append("{localDateTime: ");
        append(quote(localDateTime.toString()));
        append("}");
    }

    @Override
    public void writeDateTime(ZonedDateTime zonedDateTime) throws RuntimeException {
        append("{datetime: ");
        append(quote(zonedDateTime.toString()));
        append("}");
    }

    @Override
    public void writeGeoPoint(GeoPoint geoPoint) throws RuntimeException {
        append("{datetime: ");
        append(quote(geoPoint.toString()));
        append("}");
    }

    @Override
    public void writeNull() {
        append("<null>");
    }

    @Override
    public void writeBoolean(boolean value) {
        append(Boolean.toString(value));
    }

    @Override
    public void writeInteger(byte value) {
        append(Byte.toString(value));
    }

    @Override
    public void writeInteger(short value) {
        append(Short.toString(value));
    }

    @Override
    public void writeInteger(int value) {
        append(Integer.toString(value));
    }

    @Override
    public void writeInteger(long value) {
        append(Long.toString(value));
    }

    @Override
    public void writeFloatingPoint(float value) {
        append(Float.toString(value));
    }

    @Override
    public void writeFloatingPoint(double value) {
        append(Double.toString(value));
    }

    @Override
    public void writeString(String value) {
        append(quote(value));
    }

    @Override
    public void writeString(char value) {
        writeString(Character.toString(value));
    }

    @Override
    public void beginArray(int size, ArrayType arrayType) {
        stack.push(new ListWriter());
    }

    @Override
    public void endArray() {
        assert !stack.isEmpty();
        append(stack.pop().done());
    }

    @Override
    public void writeByteArray(byte[] value) {
        String sep = "";
        append("[");
        for (byte b : value) {
            append(sep);
            append(Byte.toString(b));
            sep = ", ";
        }
        append("]");
    }

    public String value() {
        assert stack.size() == 1;
        return stack.getLast().done();
    }

    private void append(String value) {
        assert !stack.isEmpty();
        Writer head = stack.peek();
        head.append(value);
    }

    private String quote(String value) {
        assert !stack.isEmpty();
        Writer head = stack.peek();
        return head.quote(value);
    }

    private interface Writer {
        void append(String value);

        String done();

        String quote(String in);
    }

    private abstract class BaseWriter implements Writer {
        protected final StringBuilder builder = new StringBuilder();

        @Override
        public String done() {
            return builder.toString();
        }

        @Override
        public String quote(String in) {
            return quoteMark + in + quoteMark;
        }
    }

    private class ValueWriter extends BaseWriter {
        @Override
        public void append(String value) {
            builder.append(value);
        }
    }

    private class MapWriter extends BaseWriter {
        private boolean writeKey = true;
        private String sep = "";

        MapWriter() {
            super();
            builder.append("{");
        }

        @Override
        public void append(String value) {
            if (writeKey) {
                builder.append(sep).append(value).append(": ");
            } else {
                builder.append(value);
            }
            writeKey = !writeKey;
            sep = ", ";
        }

        @Override
        public String done() {
            return builder.append("}").toString();
        }

        @Override
        public String quote(String in) {
            return writeKey ? in : super.quote(in);
        }
    }

    private class ListWriter extends BaseWriter {
        private String sep = "";

        ListWriter() {
            super();
            builder.append("[");
        }

        @Override
        public void append(String value) {
            builder.append(sep).append(value);
            sep = ", ";
        }

        @Override
        public String done() {
            return builder.append("]").toString();
        }
    }
}
