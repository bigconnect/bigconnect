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

import com.mware.ge.util.ArrayIterable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Path implements Iterable<String> {
    public static final Path EMPTY_PATH = new Path();

    private String crosspoint;
    private final String[] vertexIds;

    public Path(String... vertexIds) {
        this.vertexIds = vertexIds;
    }

    public Path(Path path, String vertexId) {
        this.vertexIds = Arrays.copyOf(path.vertexIds, path.vertexIds.length + 1);
        this.vertexIds[this.vertexIds.length - 1] = vertexId;
    }

    public void setCrosspoint(String crosspoint) {
        this.crosspoint = crosspoint;
    }

    public List<String> vertices() {
        return Arrays.asList(vertexIds);
    }

    public int length() {
        return this.vertexIds.length;
    }

    public String get(int i) {
        return this.vertexIds[i];
    }

    @Override
    public Iterator<String> iterator() {
        return new ArrayIterable<>(this.vertexIds).iterator();
    }

    @Override
    public String toString() {
        return Arrays.toString(vertexIds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Path pathOther = (Path) o;
        if (pathOther.vertexIds.length != vertexIds.length) {
            return false;
        }
        for (int i = 0; i < vertexIds.length; i++) {
            if (!pathOther.vertexIds[i].equals(vertexIds[i])) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(vertexIds);
    }
}
