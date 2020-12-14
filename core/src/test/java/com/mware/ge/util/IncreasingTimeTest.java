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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import com.mware.ge.util.IncreasingTime;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class IncreasingTimeTest {
    private static final int NUM_ITERATIONS = 1000;
    private static final int NUM_THREADS = 10;

    @Test
    public void currentTimeMillisReturnsEverIncreasingTime() {
        List<Long> times = new ArrayList<>(NUM_ITERATIONS);
        // iterate without sleeping to insure IncreasingTime will encounter duplicate system times.
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            times.add(IncreasingTime.currentTimeMillis());
        }
        assertIncreasingTimes(NUM_ITERATIONS, times);
    }

    @Test
    public void currentTimeMillisReturnsTimeGreaterThanSystemTime() {
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            long systemTime = System.currentTimeMillis();
            long increasedTime = IncreasingTime.currentTimeMillis();
            assertTrue(increasedTime > systemTime);
        }
    }

    @Test
    public void currentTimeMillisReturnsEverIncreasingTimeAcrossConcurrentThreads() throws Exception {
        final List<Long> times = Collections.synchronizedList(new ArrayList<Long>(NUM_ITERATIONS));
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    for (int i = 0; i < NUM_ITERATIONS; i++) {
                        times.add(IncreasingTime.currentTimeMillis());
                    }
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join(5000);
        }
        Collections.sort(times);
        assertIncreasingTimes(NUM_THREADS * NUM_ITERATIONS, times);
    }

    private static void assertIncreasingTimes(int expectedSize, List<Long> times) {
        assertEquals(expectedSize, new HashSet<>(times).size()); // the set removes duplicates
        long last = times.get(0) - 1;
        for (long time : times) {
            assertTrue(time == last + 1);
            last = time;
        }
    }
}
