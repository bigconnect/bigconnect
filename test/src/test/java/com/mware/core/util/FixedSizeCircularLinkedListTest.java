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

import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class FixedSizeCircularLinkedListTest {

    @Test
    public void setupAndToString() {
        int size = 5;
        FixedSizeCircularLinkedList<AtomicInteger> list = new FixedSizeCircularLinkedList<AtomicInteger>(size, AtomicInteger.class);
        assertEquals("0:0,1:0,2:0,3:0,4:0", list.toString());
    }

    @Test
    public void set() {
        int size = 5;
        FixedSizeCircularLinkedList<AtomicInteger> list = new FixedSizeCircularLinkedList<AtomicInteger>(size, AtomicInteger.class);
        for (int i = 0; i < size; i++) {
            list.head().set(i);
            list.rotateForward();
        }
        for (int i = 0; i < size; i++) {
            assertEquals(i, list.head().get());
            list.rotateForward();
        }
    }

    @Test
    public void increment() {
        int size = 5;
        FixedSizeCircularLinkedList<AtomicInteger> list = new FixedSizeCircularLinkedList<AtomicInteger>(size, AtomicInteger.class);
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < i; j++) {
                list.head().incrementAndGet();
            }
            list.rotateForward();
        }
        for (int i = 0; i < size; i++) {
            assertEquals(i, list.head().get());
            list.rotateForward();
        }
    }

    @Test
    public void circularity() {
        int size = 5;
        FixedSizeCircularLinkedList<AtomicInteger> list = new FixedSizeCircularLinkedList<AtomicInteger>(size, AtomicInteger.class);
        for (int i = 0; i < size; i++) {
            list.head().set(i);
            list.rotateForward();
        }
        for (int i = 0; i < size * 2; i++) {
            assertEquals(i % size, list.head().get());
            list.rotateForward();
        }
    }

    @Test
    public void readBackward() {
        int size = 16;
        FixedSizeCircularLinkedList<AtomicInteger> list = new FixedSizeCircularLinkedList<AtomicInteger>(size, AtomicInteger.class);
        for (int i = 0; i < size; i++) {
            list.head().set(i);
            list.rotateForward();
        }

        // one
        assertEquals(15, list.readBackward(1).get(0).get());

        // five
        List<AtomicInteger> five = list.readBackward(5);
        int sumOfFive = 0;
        for (int i = 0; i < 5; i++) {
            sumOfFive += five.get(i).get();
        }
        assertEquals(65, sumOfFive);

        // fifteen
        List<AtomicInteger> fifteen = list.readBackward(15);
        int sumOfFifteen = 0;
        for (int i = 0; i < 15; i++) {
            sumOfFifteen += fifteen.get(i).get();
        }
        assertEquals(120, sumOfFifteen);
    }
}
