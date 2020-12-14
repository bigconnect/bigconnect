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
import com.google.common.collect.Lists;
import com.mware.core.exception.BcException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class ProcessRunner {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ProcessRunner.class);

    public String executeToString(final String programName, final String[] programArgs) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            execute(programName, programArgs, out, "executeToString");
            return new String(out.toByteArray());
        } catch (Exception ex) {
            throw new BcException("Could not run " + programName + " " + Joiner.on(" ").join(programArgs), ex);
        }
    }

    public Process execute(final String programName, final String[] programArgs, OutputStream out, final String logPrefix) throws IOException, InterruptedException {
        return execute(null, programName, programArgs, out, logPrefix);
    }

    public Process execute(
            File workingDirectory,
            final String programName,
            final String[] programArgs,
            OutputStream out,
            final String logPrefix
    ) throws IOException, InterruptedException {
        final List<String> arguments = Lists.newArrayList(programName);
        for (String programArg : programArgs) {
            if (programArg == null) {
                throw new NullPointerException("Argument was null in argument list [ " + Joiner.on(", ").useForNull("null").join(programArgs) + " ]");
            }
            arguments.add(programArg);
        }

        final ProcessBuilder procBuilder = new ProcessBuilder(arguments);
        if (workingDirectory != null) {
            procBuilder.directory(workingDirectory);
        }
        final Map<String, String> sortedEnv = new TreeMap<>(procBuilder.environment());

        LOGGER.info("%s Running: %s", logPrefix, arrayToString(arguments));

        if (!sortedEnv.isEmpty()) {
            LOGGER.info("%s Spawned program environment: ", logPrefix);
            for (final Map.Entry<String, String> entry : sortedEnv.entrySet()) {
                LOGGER.info("%s %s:%s", logPrefix, entry.getKey(), entry.getValue());
            }
        } else {
            LOGGER.info("%s Running program environment is empty", logPrefix);
        }

        final Process proc = procBuilder.start();

        LoggingThread stderrThread = new LoggingThread(proc.getErrorStream(), LOGGER, logPrefix + programName + "(stderr): ");
        stderrThread.start();

        final Exception[] pipeException = new Exception[1];
        Pipe pipe = null;
        LoggingThread stdoutThread = null;
        if (out == null) {
            stdoutThread = new LoggingThread(proc.getInputStream(), LOGGER, logPrefix + programName + "(stdout): ");
            stdoutThread.start();
        } else {
            Pipe.StatusHandler statusHandler = new Pipe.StatusHandler() {
                @Override
                public void handleException(Exception e) {
                    pipeException[0] = e;
                }

            };
            pipe = new Pipe().pipe(proc.getInputStream(), out, statusHandler);
        }

        // Pipe will ensure to some degree that we have started reading but if the process exits at
        //  just the right time (after threadStarted = true but before the in.read occurs) we could miss the output
        //  this sleep should be enough to prevent this happening.
        Thread.sleep(100);

        proc.waitFor();

        stderrThread.join(10000);

        if (stdoutThread != null) {
            stdoutThread.join(10000);
        }

        if (pipe != null) {
            pipe.waitForCompletion(10000, TimeUnit.MILLISECONDS);
        }

        proc.getOutputStream().close(); // stdin
        proc.getInputStream().close(); // stdout
        proc.getErrorStream().close();

        LOGGER.info(logPrefix + programName + "(returncode): " + proc.exitValue());

        if (proc.exitValue() != 0) {
            throw new BcException("unexpected return code: " + proc.exitValue() + " for command " + arrayToString(arguments));
        }
        if (pipeException[0] != null) {
            throw new BcException("pipe exception", pipeException[0]);
        }

        return proc;
    }

    private static String arrayToString(List<String> arr) {
        StringBuilder result = new StringBuilder();
        for (String s : arr) {
            result.append(s).append(' ');
        }
        return result.toString();
    }
}

