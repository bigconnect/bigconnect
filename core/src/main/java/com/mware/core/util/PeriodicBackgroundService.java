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

import com.mware.core.lifecycle.LifecycleAdapter;
import org.apache.commons.lang.time.DateUtils;
import com.mware.core.exception.BcException;
import com.mware.core.model.lock.LeaderListener;
import com.mware.core.model.lock.LockRepository;

import java.util.Date;

public abstract class PeriodicBackgroundService extends LifecycleAdapter {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(PeriodicBackgroundService.class);
    private final LockRepository lockRepository;
    private volatile boolean enabled;

    protected PeriodicBackgroundService(LockRepository lockRepository) {
        this.lockRepository = lockRepository;
    }

    public void start() {
        if (getCheckIntervalSeconds() > 0) {
            startBackgroundThread();
        }
    }

    @SuppressWarnings("InfiniteLoopStatement")
    private void startBackgroundThread() {
        Thread t = new Thread(() -> {
            enabled = false;
            lockRepository.leaderElection(getLockName(), new LeaderListener() {
                @Override
                public void isLeader() {
                    LOGGER.debug("using successfully acquired lock (%s)", Thread.currentThread().getName());
                    enabled = true;
                }

                @Override
                public void notLeader() {
                    LOGGER.debug("lost leadership (%s)", Thread.currentThread().getName());
                    disable();
                }
            });

            while (true) {
                try {
                    Thread.sleep(10 * 1000); // wait for enabled to change
                } catch (InterruptedException e) {
                    LOGGER.error("Failed to sleep", e);
                    throw new BcException("Failed to sleep", e);
                }
                runPeriodically();
            }
        });
        t.setDaemon(true);
        t.setName(getThreadName());
        t.start();
    }

    private void runPeriodically() {
        try {
            while (enabled) {
                LOGGER.debug("running periodically");
                Date now = new Date();
                Date nowPlusOneMinute = DateUtils.addSeconds(now, getCheckIntervalSeconds());
                run();
                try {
                    long remainingMilliseconds = nowPlusOneMinute.getTime() - System.currentTimeMillis();
                    if (remainingMilliseconds > 0) {
                        Thread.sleep(remainingMilliseconds);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        } catch (Throwable ex) {
            LOGGER.error("runPeriodically error", ex);
            throw ex;
        }
    }

    protected String getThreadName() {
        return "bc-periodic-" + this.getClass().getSimpleName();
    }

    protected String getLockName() {
        return this.getClass().getName();
    }

    protected abstract void run();

    protected abstract int getCheckIntervalSeconds();

    public void disable() {
        enabled = false;
    }
}
