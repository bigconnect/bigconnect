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
package com.mware.ge.metric;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class StackTraceTracker {
    private final Set<StackTraceItem> roots = Collections.synchronizedSet(new HashSet<>());

    public void addStackTrace() {
        addStackTrace(Thread.currentThread().getStackTrace());
    }

    public void addStackTrace(StackTraceElement[] stackTraceElements) {
        Set<StackTraceItem> parents = roots;
        for (int i = stackTraceElements.length - 1; i >= 0; i--) {
            StackTraceElement stackTraceElement = stackTraceElements[i];
            StackTraceItem item = addItem(parents, stackTraceElement);
            item.count++;
            parents = item.children;
        }
    }

    private StackTraceItem addItem(Set<StackTraceItem> parents, StackTraceElement stackTraceElement) {
        StackTraceItem item = getItem(parents, stackTraceElement);
        if (item == null) {
            item = new StackTraceItem(stackTraceElement);
            parents.add(item);
        }
        return item;
    }

    @SuppressWarnings("EqualsBetweenInconvertibleTypes")
    private StackTraceItem getItem(Set<StackTraceItem> parents, StackTraceElement stackTraceElement) {
        for (StackTraceItem item : parents) {
            if (item.equals(stackTraceElement)) {
                return item;
            }
        }
        return null;
    }

    public Set<StackTraceItem> getRoots() {
        return roots;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        toString(result, "", getRoots());
        return result.toString();
    }

    private void toString(StringBuilder result, String indent, Set<StackTraceItem> items) {
        for (StackTraceItem item : items) {
            result.append(indent);
            result.append(item.toString());
            result.append("\n");
            toString(result, indent + "  ", item.children);
        }
    }

    public void reset() {
        roots.clear();
    }

    public static class StackTraceItem {
        private final Set<StackTraceItem> children = new HashSet<>();
        private final StackTraceElement stackTraceElement;
        private int count;

        public StackTraceItem(StackTraceElement stackTraceElement) {
            this.stackTraceElement = stackTraceElement;
        }

        @Override
        public String toString() {
            return String.format("%s (count=%d)", stackTraceElement, count);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o instanceof StackTraceElement) {
                return stackTraceElement.equals(o);
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StackTraceItem that = (StackTraceItem) o;
            return stackTraceElement.equals(that.stackTraceElement);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stackTraceElement);
        }

        public Set<StackTraceItem> getChildren() {
            return children;
        }

        public StackTraceElement getStackTraceElement() {
            return stackTraceElement;
        }

        public int getCount() {
            return count;
        }
    }
}
