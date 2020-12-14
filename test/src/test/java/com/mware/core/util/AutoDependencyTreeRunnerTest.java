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

import com.google.common.base.Joiner;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AutoDependencyTreeRunnerTest {
    private List<String> foundOrder = new ArrayList<String>();
    private Runnable a = new FoundOrderRunnable(foundOrder, "a");
    private Runnable b = new FoundOrderRunnable(foundOrder, "b");
    private Runnable c = new FoundOrderRunnable(foundOrder, "c");
    private Runnable d = new FoundOrderRunnable(foundOrder, "d");
    private Runnable e = new FoundOrderRunnable(foundOrder, "e");

    @Test
    public void testInOrder() {
        foundOrder.clear();

        AutoDependencyTreeRunner tree = new AutoDependencyTreeRunner();
        tree.add(a, b, c);
        tree.add(c, d);
        tree.add(c, e);
        tree.run();

        assertEquals("a,b,c,d,e", Joiner.on(',').join(foundOrder));
    }

    @Test
    public void testOutOfOrder() {
        foundOrder.clear();

        AutoDependencyTreeRunner tree = new AutoDependencyTreeRunner();
        tree.add(c, d);
        tree.add(c, e);
        tree.add(a, b, c);
        tree.run();

        assertEquals("a,b,c,d,e", Joiner.on(',').join(foundOrder));
    }

    private static class FoundOrderRunnable implements Runnable {
        private final List<String> foundOrder;
        private final String name;

        public FoundOrderRunnable(List<String> foundOrder, String name) {
            this.foundOrder = foundOrder;
            this.name = name;
        }

        @Override
        public void run() {
            this.foundOrder.add(this.name);
        }
    }
}