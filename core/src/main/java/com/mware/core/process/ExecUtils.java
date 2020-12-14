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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ProcessUtil;
import org.apache.log4j.Level;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ExecUtils {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ExecUtils.class);

    public static void unsafeSleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

    }

    public static byte[] execAndGetOutput(ProcessBuilder pb) throws IOException, InterruptedException {
        ByteCollectingSubscription outputCollector = new ByteCollectingSubscription();
        new ExecBuilder()
                .withProcessBuilder(pb)
                .withOutputConsumer(outputCollector)
                .withErrorConsumer(new LoggingLineSubscription(Level.INFO))
                .withCompletionHandler(new SimpleExceptionExecCompletionHandler(pb.command()))
                .exec();

        return outputCollector.getCollected();
    }

    public static byte[] execAndLogAndGetOutput(ProcessBuilder pb, File log) throws IOException, InterruptedException {
        try (FileOutputStream os = new FileOutputStream(log, true);){
            ByteCollectingSubscription outputCollector = new ByteCollectingSubscription();
            OutputStreamSubscription osSubscription = new OutputStreamSubscription(os, false);
            new ExecBuilder()
                    .withProcessBuilder(pb)
                    .withOutputConsumer(outputCollector)
                    .withOutputConsumer(osSubscription)
                    .withErrorConsumer(new LoggingLineSubscription(Level.INFO))
                    .withErrorConsumer(osSubscription)
                    .withCompletionHandler(new SimpleExceptionExecCompletionHandler(pb.command()))
                    .exec();
            return outputCollector.getCollected();
        }
    }

    public static ExecutionResults execAndGetOutputAndErrors(ProcessBuilder pb) throws InterruptedException, IOException {
        ByteCollectingSubscription outputCollector = new ByteCollectingSubscription();
        ByteCollectingSubscription errorCollector = new ByteCollectingSubscription();
        ExecutionResults er = new ExecutionResults();
        er.rv = new ExecBuilder()
                .withProcessBuilder(pb)
                .withOutputConsumer(outputCollector)
                .withErrorConsumer(errorCollector)
                .exec();
        er.out = new String(outputCollector.getCollected(), StandardCharsets.UTF_8);
        er.err = new String(errorCollector.getCollected(), StandardCharsets.UTF_8);
        return er;
    }

    public static ExecutionResults execAndGetOutputAndErrors(String[] args, Map<String, String> env) throws InterruptedException, IOException {
        ByteCollectingSubscription outputCollector = new ByteCollectingSubscription();
        ByteCollectingSubscription errorCollector = new ByteCollectingSubscription();
        ExecutionResults er = new ExecutionResults();
        er.rv = new ExecBuilder()
                .withArgs(args)
                .withEnv(env)
                .withOutputConsumer(outputCollector)
                .withErrorConsumer(errorCollector)
                .exec();
        er.out = new String(outputCollector.getCollected(), StandardCharsets.UTF_8);
        er.err = new String(errorCollector.getCollected(), StandardCharsets.UTF_8);
        return er;
    }

    public static class ExecBuilder {
        private String threadsBaseName;
        private List<ExecSubscription> outputConsumers = Lists.newArrayList();
        private List<ExecSubscription> errorConsumers = Lists.newArrayList();
        private List<ExecCleanuper> cleanupers = Lists.newArrayList();
        private ProcessBuilder pb;
        private String[] args;
        private Map<String, String> env;
        private File cwd;
        private String input;
        private ExecCompletionHandler completionHandler;
        private LazyInitExecKiller killer;

        public ExecBuilder withThreadsBaseName(String threadsBaseName) {
            this.threadsBaseName = threadsBaseName;
            return this;
        }

        public ExecBuilder withCompletionHandler(ExecCompletionHandler completionHandler) {
            this.completionHandler = completionHandler;
            completionHandler.init(this);
            return this;
        }

        public ExecBuilder withOutputConsumer(ExecSubscription consumer) {
            this.outputConsumers.add(consumer);
            return this;
        }

        public ExecBuilder withErrorConsumer(ExecSubscription consumer) {
            this.errorConsumers.add(consumer);
            return this;
        }

        public ExecBuilder withOutputConsumers(List<ExecSubscription> consumers) {
            this.outputConsumers.addAll(consumers);
            return this;
        }

        public ExecBuilder withErrorConsumers(List<ExecSubscription> consumers) {
            this.errorConsumers.addAll(consumers);
            return this;
        }

        public ExecBuilder withCleanuper(ExecCleanuper cleanuper) {
            this.cleanupers.add(cleanuper);
            return this;
        }

        public ExecBuilder withCleanupers(List<ExecCleanuper> cleanupers) {
            this.cleanupers.addAll(cleanupers);
            return this;
        }

        public ExecBuilder withProcessBuilder(ProcessBuilder pb) {
            if (this.args != null) {
                throw new IllegalArgumentException("Cannot set process builder after args");
            }
            this.pb = pb;
            return this;
        }

        public ExecBuilder withArgs(List<String> args) {
            return this.withArgs(args.toArray(new String[0]));
        }

        public ExecBuilder withArgs(String[] args) {
            if (this.pb != null) {
                throw new IllegalArgumentException("Cannot set args after process builder");
            }
            this.args = args;
            return this;
        }

        public ExecBuilder withEnv(Map<String, String> env) {
            if (this.env == null) {
                this.env = env;
            } else {
                this.env.putAll(env);
            }
            return this;
        }

        public ExecBuilder withEnv(String key, String value) {
            if (this.env == null) {
                this.env = Maps.newHashMap();
            }
            this.env.put(key, value);
            return this;
        }

        public ExecBuilder withCwd(File cwd) {
            this.cwd = cwd;
            return this;
        }

        public ExecBuilder withInput(String input) {
            this.input = input;
            return this;
        }

        public ExecBuilder withKiller(LazyInitExecKiller killer) {
            this.killer = killer;
            return this;
        }

        public int exec() throws IOException, InterruptedException {
            if (this.args != null) {
                this.pb = new ProcessBuilder(this.args);
            }

            if (this.env != null) {
                this.pb.environment().putAll(this.env);
            }

            if (this.cwd != null) {
                this.pb = this.pb.directory(this.cwd);
            }

            final Process p = this.pb.start();
            ExecOutputConsumer consumer = new ExecOutputConsumer()
                    .withInput(this.input)
                    .withOutputConsumers(this.outputConsumers)
                    .withErrorConsumers(this.errorConsumers)
                    .withThreadsBaseName(this.threadsBaseName);
            consumer.start(p.getInputStream(), p.getErrorStream(), p.getOutputStream());
            ExecWaitingThead waitingThread = new ExecWaitingThead(p);
            waitingThread.start();
            if (this.killer != null) {
                this.killer.setWaitingThread(waitingThread);
            }

            try {
                int returnCode = waitingThread.waitFor();
                if (this.completionHandler != null) {
                    this.completionHandler.handle(returnCode);
                }
                return returnCode;
            } finally {
                consumer.finish();
                for(ExecCleanuper cleanuper : this.cleanupers) {
                    cleanuper.cleanup();
                }
            }
        }
    }

    private static class ExecWaitingThead extends Thread {
        private int rv;
        private final Process p;

        private ExecWaitingThead(Process p) {
            this.p = p;
        }

        @Override
        public void run() {
            try {
                this.rv = this.p.waitFor();
                LOGGER.info("Done waiting for return value,  got " + this.rv);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.error("Exec wait interrupted", e);
            }
        }

        public boolean isRunning() {
            return this.isAlive();
        }

        public int waitFor() throws InterruptedException {
            this.join();
            return this.rv;
        }

        public int waitFor(long timeout) throws InterruptedException {
            this.join(timeout);
            return this.rv;
        }

        public void niceKill() {
            int pid = ProcessUtil.getPid(this.p);
            try {
                Runtime.getRuntime().exec("kill -SIGINT " + pid);
            }
            catch (IOException e) {
                LOGGER.error("Nice kill failed (pid=" + pid + ")", e);
            }
        }

        public void evilKill() {
            int pid = ProcessUtil.getPid(this.p);
            try {
                this.p.destroy();
                Runtime.getRuntime().exec("kill -SIGKILL " + pid);
            }
            catch (IOException e) {
                LOGGER.error("Evil kill failed (pid=" + pid + ")", e);
            }
        }
    }

    public interface ExecSubscription {
        void close() throws IOException;
    }

    public interface BytesSubscription extends ExecSubscription {
        void handle(byte[] buffer, int count) throws IOException;
    }

    public interface LineSubscription extends ExecSubscription {
        void handle(String line, boolean replace) throws IOException;
    }

    public interface ExecCleanuper {
        void cleanup();
    }

    public interface ExecCompletionHandler {
        void init(ExecBuilder builder);
        void handle(int returnCode) throws IOException;
    }

    public interface LazyInitExecKiller {
        void kill();
        void setWaitingThread(ExecWaitingThead t);
    }

    public static class ExecOutputConsumer {
        private String threadsBaseName;
        private List<ExecSubscription> outputConsumers = Lists.newArrayList();
        private List<ExecSubscription> errorConsumers = Lists.newArrayList();
        private String input;
        private List<Thread> outputConsumingThreads;
        private List<Thread> errorConsumingThreads;

        public ExecOutputConsumer withThreadsBaseName(String threadsBaseName) {
            this.threadsBaseName = threadsBaseName;
            return this;
        }

        public ExecOutputConsumer withOutputConsumer(ExecSubscription consumer) {
            this.outputConsumers.add(consumer);
            return this;
        }

        public ExecOutputConsumer withErrorConsumer(ExecSubscription consumer) {
            this.errorConsumers.add(consumer);
            return this;
        }

        public ExecOutputConsumer withOutputConsumers(List<ExecSubscription> consumers) {
            this.outputConsumers.addAll(consumers);
            return this;
        }

        public ExecOutputConsumer withErrorConsumers(List<ExecSubscription> consumers) {
            this.errorConsumers.addAll(consumers);
            return this;
        }

        public ExecOutputConsumer withInput(String input) {
            this.input = input;
            return this;
        }

        public void start(InputStream processOutput, InputStream processError, OutputStream processInput) throws IOException {
            this.outputConsumingThreads = this.build(processOutput, this.threadsBaseName + "-out", this.outputConsumers);
            this.errorConsumingThreads = this.build(processError, this.threadsBaseName + "-err", this.errorConsumers);
            for (Thread t : this.outputConsumingThreads) {
                t.start();
            }
            for (Thread t : this.errorConsumingThreads) {
                t.start();
            }
            if (this.input != null) {
                PrintStream stream = new PrintStream(processInput);
                stream.append(this.input);
                stream.close();
                processInput.close();
            }
        }

        public void finish() throws InterruptedException {
            for (Thread t : this.outputConsumingThreads) {
                t.join();
            }
            for (Thread t : this.errorConsumingThreads) {
                t.join();
            }
        }

        private List<Thread> build(InputStream is, String threadBaseName, List<ExecSubscription> consumers) throws IOException {
            ArrayList bytesSubscription = Lists.newArrayList();
            ArrayList lineSubscriptions = Lists.newArrayList();
            for (ExecSubscription consumer : consumers) {
                if (consumer instanceof BytesSubscription) {
                    bytesSubscription.add(consumer);
                    continue;
                }
                if (consumer instanceof LineSubscription) {
                    lineSubscriptions.add(consumer);
                    continue;
                }
                throw new IllegalArgumentException("Unhandled process output stream consumer of type " + consumer.getClass().getCanonicalName());
            }
            ArrayList threads = Lists.newArrayList();
            if (bytesSubscription.isEmpty() && lineSubscriptions.isEmpty()) {
                threads.add(new Blackhole(is));
            } else if (bytesSubscription.isEmpty()) {
                threads.add(new StreamToLine(threadBaseName, is, lineSubscriptions));
            } else if (!lineSubscriptions.isEmpty()) {
                PipedOutputStream pos = new PipedOutputStream();
                PipedInputStream pis = new PipedInputStream(pos);
                bytesSubscription.add(new OutputStreamSubscription(pos, true));
                threads.add(new StreamDuplicator(is, bytesSubscription));
                threads.add(new StreamToLine(threadBaseName, pis, lineSubscriptions));
            } else {
                threads.add(new StreamDuplicator(is, bytesSubscription));
            }
            return threads;
        }
    }

    private static class Blackhole extends Thread {
        private final InputStream src;
        private static BcLogger _logger = BcLoggerFactory.getLogger(Blackhole.class);

        Blackhole(InputStream src) {
            this.src = src;
        }

        @Override
        public void run() {
            try {
                long total = 0L;
                byte[] buffer = new byte[4096];
                int read = this.src.read(buffer);
                while (read >= 0) {
                    total += (long)read;
                    read = this.src.read(buffer);
                }
                _logger.info("Read and ignored " + total + " bytes");
            }
            catch (IOException e) {
                _logger.error("Failed to dev/null stream", e);
            }
        }
    }

    private static class StreamToLine extends Thread {
        private static BcLogger _logger = BcLoggerFactory.getLogger(StreamToLine.class);

        private final InputStream src;
        private final List<LineSubscription> dsts;
        private final String threadBaseName;

        StreamToLine(String threadBaseName, InputStream src, List<LineSubscription> dsts) {
            this.threadBaseName = threadBaseName;
            this.src = src;
            this.dsts = dsts;
        }

        /*
         * WARNING - Removed try catching itself - possible behaviour change.
         */
        @Override
        public void run() {
            Thread.currentThread().setName(this.threadBaseName + "-" + Thread.currentThread().getId());
            try {
                try (CRLineReader reader = new CRLineReader(new InputStreamReader(this.src, StandardCharsets.UTF_8));){
                    String line = reader.readLine();
                    while (line != null) {
                        Iterator<LineSubscription> i$ = this.dsts.iterator();
                        while (i$.hasNext()) {
                            LineSubscription dst;
                            LineSubscription lineSubscription = dst = i$.next();
                            synchronized (lineSubscription) {
                                dst.handle(line, reader.startsWithCR());
                            }
                        }
                        line = reader.readLine();
                    }
                }
            }
            catch (EOFException e) {
                _logger.debug("StreamToLine: EOF");
            }
            catch (IOException e) {
                if ("Stream closed".equals(e.getMessage())) {
                    _logger.debug("StreamToLine: EOF (stream closed)");
                }
                _logger.error("Failed to duplicate stream", e);
            }
            finally {
                for (LineSubscription dst : this.dsts) {
                    try {
                        dst.close();
                    }
                    catch (IOException e) {
                        _logger.error("Failed to shutdown output log line handler", e);
                    }
                }
            }
        }
    }


    public static class OutputStreamSubscription implements BytesSubscription {
        private final OutputStream os;
        private final boolean doCloseStream;

        public OutputStreamSubscription(OutputStream os, boolean doCloseStream) {
            this.os = os;
            this.doCloseStream = doCloseStream;
        }

        @Override
        public void handle(byte[] buffer, int count) throws IOException {
            this.os.write(buffer, 0, count);
        }

        @Override
        public void close() throws IOException {
            this.os.flush();
            if (this.doCloseStream) {
                this.os.close();
            }
        }
    }


    private static class StreamDuplicator extends Thread {
        private static BcLogger _logger = BcLoggerFactory.getLogger(StreamDuplicator.class);

        private final InputStream src;
        private final List<BytesSubscription> dsts;

        StreamDuplicator(InputStream src, List<BytesSubscription> dsts) {
            this.src = src;
            this.dsts = dsts;
        }

        @Override
        public void run() {
            try {
                byte[] buffer = new byte[4096];
                int read = this.src.read(buffer);
                while (read >= 0) {
                    for(BytesSubscription dst : this.dsts) {
                        synchronized (dst) {
                            dst.handle(buffer, read);
                        }
                    }

                    read = this.src.read(buffer);
                }
            } catch (IOException e) {
                _logger.error("Failed to duplicate stream", e);
            } finally {
                for (BytesSubscription dst : this.dsts) {
                    try {
                        dst.close();
                    }
                    catch (IOException e) {
                        _logger.error("Failed to shutdown output log line handler", e);
                    }
                }
            }
        }
    }

    private static class CRLineReader extends BufferedReader {
        private static final char LF = '\n';
        private static final char CR = '\r';
        private boolean startsWithCR = false;

        public CRLineReader(Reader reader) {
            super(reader);
        }

        @Override
        public String readLine() throws IOException {
            StringBuilder sb = new StringBuilder();
            this.startsWithCR = false;
            synchronized (this.lock) {
                int intch;
                boolean first = true;
                while ((intch = super.read()) != -1) {
                    if (intch != CR && intch != LF) {
                        sb.append((char)intch);
                        break;
                    }
                    this.startsWithCR = first && intch == CR;
                    first &= this.startsWithCR;
                }
                while ((intch = this.markAndRead(1)) != -1) {
                    if (intch == CR || intch == LF) {
                        super.reset();
                        break;
                    }
                    sb.append((char)intch);
                }
            }
            return sb.length() == 0 ? null : sb.toString();
        }

        private int markAndRead(int readAheadLimit) throws IOException {
            super.mark(readAheadLimit);
            return super.read();
        }

        public boolean startsWithCR() {
            return this.startsWithCR;
        }
    }

    public static class ByteCollectingSubscription implements BytesSubscription {
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        public byte[] getCollected() {
            return this.baos.toByteArray();
        }

        @Override
        public void handle(byte[] buffer, int count) throws IOException {
            this.baos.write(buffer, 0, count);
        }

        @Override
        public void close() throws IOException {
            this.baos.flush();
        }
    }

    public static class OutputWriterSubscription implements LineSubscription {
        private final Writer wr;
        private final boolean doCloseWriter;

        public OutputWriterSubscription(Writer wr, boolean doCloseWriter) {
            this.wr = wr;
            this.doCloseWriter = doCloseWriter;
        }

        @Override
        public void handle(String line, boolean replace) throws IOException {
            this.wr.write(line);
            this.wr.write("\n");
            this.wr.flush();
        }

        @Override
        public void close() throws IOException {
            this.wr.flush();
            if (this.doCloseWriter) {
                this.wr.close();
            }
        }
    }

    public static class LoggingLineSubscription implements LineSubscription {
        private final Level level;

        public LoggingLineSubscription(Level level) {
            this.level = level;
        }

        @Override
        public void handle(String line, boolean replace) {
            if(level.equals(Level.INFO) || level.equals(Level.ALL))
                LOGGER.info(line);
            else if(level.equals(Level.WARN))
                LOGGER.warn(line);
            else if(level.equals(Level.ERROR) || level.equals(Level.FATAL))
                LOGGER.error(line);
            else if(level.equals(Level.DEBUG))
                LOGGER.debug(line);
            else if(level.equals(Level.TRACE))
                LOGGER.trace(line);
        }

        @Override
        public void close() throws IOException {
        }
    }

    public static class SimpleExceptionExecCompletionHandler implements ExecCompletionHandler {
        private String commandName;
        private String message;

        public SimpleExceptionExecCompletionHandler() {
        }

        public SimpleExceptionExecCompletionHandler(String message) {
            this.message = message;
        }

        public SimpleExceptionExecCompletionHandler(List<String> command) {
            if (!command.isEmpty() && command.get(0).length() < 100) {
                this.commandName = command.get(0);
            }
        }

        @Override
        public void init(ExecBuilder builder) {
        }

        @Override
        public void handle(int rv) throws IOException {
            if (rv != 0) {
                throw ProcessDiedException.getExceptionOnProcessDeath(
                        this.commandName + " failed",
                        null,
                        false,
                        rv
                );
            }
        }
    }

    public static class ExecutionResults {
        public String out;
        public String err;
        public int rv;
    }
}
