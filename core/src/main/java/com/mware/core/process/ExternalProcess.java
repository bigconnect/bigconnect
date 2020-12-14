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
package com.mware.core.process;

import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ProcessUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ExternalProcess {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ExternalProcess.class);

    private final ProcessBuilder def;
    private final File runDir;
    private final Thread waitingThread;
    private final User user;
    private Process p;
    private int pid = 0;
    private int returnCode = -1;

    public ExternalProcess(ProcessBuilder def, File runDir, User user) {
        this.def = def;
        this.runDir = runDir;
        this.user = user;
        this.waitingThread = new Thread(() -> {
            try {
                Thread.currentThread().setName("RegularExec-" + Thread.currentThread().getId());
                ExternalProcess.this.returnCode = ExternalProcess.this.p.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.error("Unexpected interruption!", e);
            }
        });
    }

    public void start() throws IOException {
        this.def.directory(this.runDir);
        LOGGER.info("Starting process (regular)");
        this.p = this.def.start();
        this.pid = ProcessUtil.getPid(this.p);
        LOGGER.info("Process started with pid=" + this.pid);
        this.waitingThread.start();
    }

    public int getWorkingPid() {
        return this.pid;
    }

    public void niceKill() {
        int pid = ProcessUtil.getPid(this.p);
        try {
            Runtime.getRuntime().exec("kill -SIGINT " + pid);
        } catch (IOException e) {
            LOGGER.error("Nice kill failed (pid=" + pid + ")", e);
        }
    }

    public void evilKill() {
        int pid = ProcessUtil.getPid(this.p);
        LOGGER.info(String.format("Killing process PID: %d", pid));
        try {
            this.p.destroy();
            Runtime.getRuntime().exec("kill -SIGKILL " + pid);
        } catch (IOException e) {
            LOGGER.error("Evil kill failed (pid=" + pid + ")", e);
        }
    }

    public void niceThenEvilKill() {
        if (this.isRunning()) {
            LOGGER.info("Process is still running, sending SIGINT...");
            this.niceKill();
            try {
                this.waitFor(15000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.error("Unexpected interruption", e);
            }
            if (this.isRunning()) {
                LOGGER.info("Process is still running after 15000ms, sending SIGKILL...");
                this.evilKill();
                try {
                    this.waitFor(5000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.error("Unexpected interruption", e);
                }
                if (this.isRunning()) {
                    LOGGER.info("Process is still alive after SIGINT and SIGKILL. Too strong for us...");
                } else {
                    LOGGER.info("Process has been abruptly killed. Cleanup may be incomplete.");
                }
            } else {
                LOGGER.info("Process has been nicely interrupted!");
            }
        }
    }

    public boolean isRunning() {
        return this.waitingThread.isAlive();
    }

    private int waitFor(long timeout) throws InterruptedException {
        this.waitingThread.join(timeout);
        return this.returnCode;
    }

    public int waitFor() throws InterruptedException {
        this.waitingThread.join();
        return this.returnCode;
    }

    public OutputStream getOutputStream() {
        return this.p.getOutputStream();
    }

    public InputStream getInputStream() {
        return this.p.getInputStream();
    }

    public InputStream getErrorStream() {
        return this.p.getErrorStream();
    }
}
