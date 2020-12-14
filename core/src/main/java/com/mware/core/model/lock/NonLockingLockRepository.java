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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.lifecycle.LifeSupportService;

import java.util.WeakHashMap;
import java.util.concurrent.Callable;

@Singleton
public class NonLockingLockRepository extends LockRepository {
    private WeakHashMap<Long, Thread> threads = new WeakHashMap<>();

    // available for testing when you don't need perfect shutdown behavior
    @VisibleForTesting
    public NonLockingLockRepository() {

    }

    @Inject
    public NonLockingLockRepository(LifeSupportService lifeSupportService) {
        lifeSupportService.add(this);
    }

    @Override
    public Lock createLock(String lockName) {
        return new Lock(lockName) {
            @Override
            public <T> T run(Callable<T> callable) {
                try {
                    return callable.call();
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to run in lock", ex);
                }
            }
        };
    }

    @Override
    public void leaderElection(String lockName, final LeaderListener listener) {
        Thread t = new Thread(() -> {
            try {
                listener.isLeader();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t.setName(NonLockingLockRepository.class.getSimpleName() + "-LeaderElection-" + lockName);
        t.setDaemon(true);
        t.start();
        threads.put(t.getId(), t);
    }

    @Override
    public void shutdown() {
        for (Thread thread : threads.values()) {
            thread.interrupt();
        }
    }
}
