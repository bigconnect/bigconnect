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
package com.mware.ge.values.virtual;

import com.mware.ge.Authorizations;
import com.mware.ge.values.AnyValue;
import com.mware.ge.values.AnyValueWriter;
import com.mware.ge.values.ValueMapper;
import com.mware.ge.values.VirtualValue;

import java.util.Arrays;
import java.util.Comparator;

public abstract class PathValue extends VirtualValue {
    public abstract NodeValue startNode();

    public abstract NodeValue endNode();

    public abstract RelationshipValue lastRelationship();

    public abstract NodeValue[] nodes();

    public abstract RelationshipValue[] relationships();

    @Override
    public boolean equals(VirtualValue other) {
        if (other == null || !(other instanceof PathValue)) {
            return false;
        }
        PathValue that = (PathValue) other;
        return size() == that.size() &&
                Arrays.equals(nodes(), that.nodes()) &&
                Arrays.equals(relationships(), that.relationships());
    }

    @Override
    public int computeHash() {
        NodeValue[] nodes = nodes();
        RelationshipValue[] relationships = relationships();
        int result = nodes[0].hashCode();
        for (int i = 1; i < nodes.length; i++) {
            result += 31 * (result + relationships[i - 1].hashCode());
            result += 31 * (result + nodes[i].hashCode());
        }
        return result;
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer, Authorizations authorizations) throws E {
        writer.writePath(nodes(), relationships(), authorizations);
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapPath(this);
    }

    @Override
    public VirtualValueGroup valueGroup() {
        return VirtualValueGroup.PATH;
    }

    @Override
    public int compareTo(VirtualValue other, Comparator<AnyValue> comparator) {
        if (other == null || !(other instanceof PathValue)) {
            throw new IllegalArgumentException("Cannot compare different virtual values");
        }

        PathValue otherPath = (PathValue) other;
        NodeValue[] nodes = nodes();
        RelationshipValue[] relationships = relationships();
        NodeValue[] otherNodes = otherPath.nodes();
        RelationshipValue[] otherRelationships = otherPath.relationships();

        int x = nodes[0].compareTo(otherNodes[0], comparator);
        if (x == 0) {
            int i = 0;
            int length = Math.min(relationships.length, otherRelationships.length);

            while (x == 0 && i < length) {
                x = relationships[i].compareTo(otherRelationships[i], comparator);
                ++i;
            }

            if (x == 0) {
                x = Integer.compare(relationships.length, otherRelationships.length);
            }
        }

        return x;
    }

    @Override
    public String toString() {
        NodeValue[] nodes = nodes();
        RelationshipValue[] relationships = relationships();
        StringBuilder sb = new StringBuilder(getTypeName() + "{");
        int i = 0;
        for (; i < relationships.length; i++) {
            sb.append(nodes[i]);
            sb.append(relationships[i]);
        }
        sb.append(nodes[i]);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public String getTypeName() {
        return "Path";
    }

    public ListValue asList() {
        NodeValue[] nodes = nodes();
        RelationshipValue[] relationships = relationships();
        int size = nodes.length + relationships.length;
        AnyValue[] anyValues = new AnyValue[size];
        for (int i = 0; i < size; i++) {
            if (i % 2 == 0) {
                anyValues[i] = nodes[i / 2];
            } else {
                anyValues[i] = relationships[i / 2];
            }
        }
        return VirtualValues.list(anyValues);
    }

    public int size() {
        return relationships().length;
    }

    public static class DirectPathValue extends PathValue {
        private final NodeValue[] nodes;
        private final RelationshipValue[] edges;

        DirectPathValue(NodeValue[] nodes, RelationshipValue[] edges) {
            assert nodes != null;
            assert edges != null;
            assert nodes.length == edges.length + 1;

            this.nodes = nodes;
            this.edges = edges;
        }

        @Override
        public NodeValue startNode() {
            return nodes[0];
        }

        @Override
        public NodeValue endNode() {
            return nodes[nodes.length - 1];
        }

        @Override
        public RelationshipValue lastRelationship() {
            assert edges.length > 0;
            return edges[edges.length - 1];
        }

        @Override
        public NodeValue[] nodes() {
            return nodes;
        }

        @Override
        public RelationshipValue[] relationships() {
            return edges;
        }

        @Override
        public String toString() {
            return startNode() + "---" + endNode();
        }
    }
}
