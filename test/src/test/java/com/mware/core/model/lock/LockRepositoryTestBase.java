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
package com.mware.core.model.lock;

import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class LockRepositoryTestBase {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(LockRepositoryTestBase.class);
    protected LockRepository lockRepository;

    protected abstract LockRepository createLockRepository();

    @Before
    public void before() throws Exception {
        lockRepository = createLockRepository();
    }


    protected Thread createLockExercisingThread(
            final LockRepository lockRepository,
            final String lockName,
            int threadIndex,
            final List<String> messages
    ) {
        Thread t = new Thread() {
            @Override
            public void run() {
                lockRepository.lock(lockName, new Runnable() {
                    @Override
                    public void run() {
                        String message = String.format(
                                "[thread: %s] run: %s",
                                Thread.currentThread().getName(),
                                lockName
                        );
                        LOGGER.debug(message);
                        messages.add(message);
                    }
                });
            }
        };
        t.setName("LockExercisingThread-" + threadIndex);
        t.setDaemon(true);
        return t;
    }

    protected Thread createLeaderElectingThread(
            final LockRepository lockRepository,
            final String lockName,
            int threadIndex,
            final List<String> messages
    ) {
        Thread t = new Thread() {
            @Override
            public void run() {
                LOGGER.debug("thread %s started", Thread.currentThread().getName());
                lockRepository.leaderElection(lockName, new LeaderListener() {
                    @Override
                    public void isLeader() throws InterruptedException {
                        String message = String.format(
                                "[thread: %s, threadIndex: %d] isLeader: %s",
                                Thread.currentThread().getName(),
                                threadIndex,
                                lockName
                        );
                        LOGGER.debug(message);
                        messages.add(message);
                        while (true) {
                            // spin like we are happily running as the leader
                            Thread.sleep(1000);
                        }
                    }

                    @Override
                    public void notLeader() {
                        String message = String.format(
                                "[thread: %s, threadIndex: %d] notLeader: %s",
                                Thread.currentThread().getName(),
                                threadIndex,
                                lockName
                        );
                        LOGGER.debug(message);
                    }
                });
            }
        };
        t.setName("LeaderElectingThread-" + threadIndex);
        t.setDaemon(true);
        return t;
    }

    protected void startThreadsWaitForMessagesThenStopThreads(
            List<Thread> threads,
            List<String> messages,
            int expectedMessageCount
    ) throws InterruptedException {
        for (Thread t : threads) {
            t.start();
        }
        for (int i = 0; i < 300 && messages.size() < expectedMessageCount; i++) {
            Thread.sleep(100);
        }
        assertEquals(expectedMessageCount, messages.size());
        for (Thread t : threads) {
            t.interrupt();
        }
    }

    protected void testCreateLock(LockRepository lockRepository) throws InterruptedException {
        List<String> messages = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            threads.add(createLockExercisingThread(lockRepository, "lockOne", i, messages));
        }
        for (int i = 5; i < 10; i++) {
            threads.add(createLockExercisingThread(lockRepository, "lockTwo", i, messages));
        }
        for (Thread t : threads) {
            t.start();
            t.join();
        }
        if (threads.size() != messages.size()) {
            throw new RuntimeException("Expected " + threads.size() + " found " + messages.size());
        }
    }
}
