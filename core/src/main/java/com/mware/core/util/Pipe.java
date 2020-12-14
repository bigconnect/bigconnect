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

import com.mware.core.exception.BcException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class Pipe {
    private static ExecutorService executor = Executors.newFixedThreadPool(5);
    private Semaphore completionSemaphore = new Semaphore(1);

    public Pipe pipe(final InputStream in, final OutputStream out, final StatusHandler statusHandler) {
        final boolean[] threadStarted = new boolean[1];
        threadStarted[0] = false;

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    int read;
                    byte[] buffer = new byte[1 * 1024 * 1024];
                    completionSemaphore.acquire();
                    threadStarted[0] = true;
                    while ((read = in.read(buffer)) > 0) {
                        out.write(buffer, 0, read);
                    }
                } catch (IOException e) {
                    statusHandler.handleException(e);
                } catch (InterruptedException e) {
                    statusHandler.handleException(e);
                } finally {
                    threadStarted[0] = true; // if something fails before we get a chance to set this to true make sure it happens
                    statusHandler.handleComplete();
                    completionSemaphore.release();
                }
            }
        });

        // It could be possible for the process to exit before this thread gets started resulting in this thread
        //   to not read the output stream. This waiting is to make sure the thread is started before we return.
        //   http://stackoverflow.com/questions/2150723/process-waitfor-threads-and-inputstreams (see resolution)
        while (!threadStarted[0]) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new BcException("Could not sleep", e);
            }
        }
        return this;
    }

    public void waitForCompletion(long timeout, TimeUnit units) throws InterruptedException {
        completionSemaphore.tryAcquire(timeout, units);
    }

    public static class StatusHandler {
        public void handleException(Exception e) {

        }

        public void handleComplete() {

        }
    }
}
