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
package com.mware.ge;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;

public class EdgesSummary {
    private ImmutableMap<String, Integer> outEdgeCountsByLabels;
    private ImmutableMap<String, Integer> inEdgeCountsByLabels;

    public EdgesSummary(
            ImmutableMap<String, Integer> outEdgeCountsByLabels,
            ImmutableMap<String, Integer> inEdgeCountsByLabels
    ) {
        this.outEdgeCountsByLabels = outEdgeCountsByLabels;
        this.inEdgeCountsByLabels = inEdgeCountsByLabels;
    }

    public EdgesSummary(Map<String, Integer> outEdgeCountsByLabels, Map<String, Integer> inEdgeCountsByLabels) {
        this(
                ImmutableMap.copyOf(outEdgeCountsByLabels),
                ImmutableMap.copyOf(inEdgeCountsByLabels)
        );
    }

    public ImmutableMap<String, Integer> getOutEdgeCountsByLabels() {
        return outEdgeCountsByLabels;
    }

    public ImmutableMap<String, Integer> getInEdgeCountsByLabels() {
        return inEdgeCountsByLabels;
    }

    public ImmutableMap<String, Integer> getEdgeCountsByLabels() {
        Map<String, Integer> m = new HashMap<>(getOutEdgeCountsByLabels());
        for (Map.Entry<String, Integer> entry : getInEdgeCountsByLabels().entrySet()) {
            m.merge(entry.getKey(), entry.getValue(), (a, b) -> a + b);
        }
        return ImmutableMap.copyOf(m);
    }

    public ImmutableSet<String> getOutEdgeLabels() {
        return outEdgeCountsByLabels.keySet();
    }

    public ImmutableSet<String> getInEdgeLabels() {
        return inEdgeCountsByLabels.keySet();
    }

    public ImmutableSet<String> getEdgeLabels() {
        return ImmutableSet.<String>builder()
                .addAll(getOutEdgeLabels())
                .addAll(getInEdgeLabels())
                .build();
    }

    public ImmutableSet<String> getEdgeLabels(Direction direction) {
        if (direction == Direction.IN) {
            return getInEdgeLabels();
        }
        if (direction == Direction.OUT) {
            return getOutEdgeLabels();
        }
        if (direction == Direction.BOTH) {
            return getEdgeLabels();
        }
        throw new GeException("Unsupported direction: " + direction);
    }

    public int getCountOfOutEdges() {
        return outEdgeCountsByLabels.values().stream().mapToInt(l -> l).sum();
    }

    public int getCountOfInEdges() {
        return inEdgeCountsByLabels.values().stream().mapToInt(l -> l).sum();
    }

    public int getCountOfEdges() {
        return getCountOfOutEdges() + getCountOfInEdges();
    }

    public int getCountOfEdges(Direction direction) {
        if (direction == Direction.IN) {
            return getCountOfInEdges();
        }
        if (direction == Direction.OUT) {
            return getCountOfOutEdges();
        }
        if (direction == Direction.BOTH) {
            return getCountOfEdges();
        }
        throw new GeException("Unsupported direction: " + direction);
    }
}
