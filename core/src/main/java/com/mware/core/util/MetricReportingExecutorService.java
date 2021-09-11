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

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricReportingExecutorService extends ThreadPoolExecutor {
    private BcLogger logger;
    private ScheduledExecutorService scheduledExecutorService;
    private FixedSizeCircularLinkedList<AtomicInteger> executionCount;
    private FixedSizeCircularLinkedList<AtomicInteger> maxActive;
    private FixedSizeCircularLinkedList<AtomicInteger> maxWaiting;

    public MetricReportingExecutorService(BcLogger logger, int nThreads) {
        super(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        this.logger = logger;

        executionCount = new FixedSizeCircularLinkedList<>(16, AtomicInteger.class);
        maxActive = new FixedSizeCircularLinkedList<>(16, AtomicInteger.class);
        maxWaiting = new FixedSizeCircularLinkedList<>(16, AtomicInteger.class);

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                tick();
            }
        }, 1, 1, TimeUnit.MINUTES);
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                report();
            }
        }, 1, 5, TimeUnit.MINUTES);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);

        executionCount.head().incrementAndGet();

        int active = getActiveCount();
        int currentMaxActive = maxActive.head().get();
        if (active > currentMaxActive) {
            maxActive.head().set(active);
        }

        int waiting = getQueue().size();
        int currentMaxWaiting = maxWaiting.head().get();
        if (waiting > currentMaxWaiting) {
            maxWaiting.head().set(waiting);
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
    }

    public void tick() {
        executionCount.rotateForward();
        executionCount.head().set(0);

        maxActive.rotateForward();
        maxActive.head().set(0);

        maxWaiting.rotateForward();
        maxWaiting.head().set(0);
    }

    public void report() {
        List<AtomicInteger> executionCountList = executionCount.readBackward(15);
        List<AtomicInteger> maxActiveList = maxActive.readBackward(15);
        List<AtomicInteger> maxWaitingList = maxWaiting.readBackward(15);
        report("executions: ", executionCountList);
        report("max active: ", maxActiveList);
        report("max waiting:", maxWaitingList);
    }

    private void report(String label, List<AtomicInteger> list) {
        int one = list.get(0).get();
        int five = 0;
        int fifteen = 0;
        for (int i = 0; i < 15; i++) {
            int value = list.get(i).get();
            if (i < 5) {
                five += value;
            }
            fifteen += value;
        }
        logger.debug("%s %3d / %6.2f / %6.2f", label, one, five / 5.0, fifteen / 15.0);
    }

    @Override
    public void shutdown() {
        scheduledExecutorService.shutdown();
        super.shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        scheduledExecutorService.awaitTermination(timeout, unit);
        return super.awaitTermination(timeout, unit);
    }

    @Override
    public List<Runnable> shutdownNow() {
        scheduledExecutorService.shutdownNow();
        return super.shutdownNow();
    }
}
