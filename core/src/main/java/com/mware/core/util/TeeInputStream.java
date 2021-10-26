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

import com.mware.ge.metric.PausableTimerContext;
import com.mware.ge.metric.PausableTimerContextAware;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

public class TeeInputStream {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(TeeInputStream.class);
    private static final int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;
    public static final int LOOP_REPORT_INTERVAL = 10 * 1000; // report to the user every 10 seconds that a queue is waiting
    private final InputStream source;
    private final MyInputStream[] tees;
    private final byte[] cyclicBuffer;
    private int cyclicBufferOffsetIndex; /* Index into the buffer for which cyclicBufferOffset represents */
    private long cyclicBufferOffset; /* Offset of the source input stream that begins the cyclic buffer */
    private int cyclicBufferValidSize; /* number of bytes in the cyclicBuffer which are valid */
    private final Object cyclicBufferLock = new Object();
    private boolean sourceComplete;

    public TeeInputStream(InputStream source, String[] splitNames) {
        this(source, splitNames, DEFAULT_BUFFER_SIZE);
    }

    public TeeInputStream(InputStream source, int splits) {
        this(source, new String[splits], DEFAULT_BUFFER_SIZE);
    }

    public TeeInputStream(InputStream source, int splits, int bufferSize) {
        this(source, new String[splits], bufferSize);
    }

    public TeeInputStream(InputStream source, String[] splitNames, int bufferSize) {
        this.source = source;
        cyclicBuffer = new byte[bufferSize];
        cyclicBufferOffsetIndex = 0;
        cyclicBufferOffset = 0;
        cyclicBufferValidSize = 0;
        sourceComplete = false;
        tees = new MyInputStream[splitNames.length];
        for (int i = 0; i < tees.length; i++) {
            tees[i] = new MyInputStream(splitNames[i]);
        }
    }

    public InputStream[] getTees() {
        return tees;
    }

    private boolean isClosed(int idx) {
        return tees[idx].isClosed();
    }

    public void close() throws IOException {
        for (InputStream tee : tees) {
            tee.close();
        }
    }

    public void loopUntilTeesAreClosed() throws Exception {
        boolean allClosed = false;
        long lastReport = new Date().getTime();
        while (!allClosed) {
            allClosed = true;
            for (int i = 0; i < tees.length; i++) {
                if (!isClosed(i)) {
                    allClosed = false;
                    if (LOGGER.isDebugEnabled() && new Date().getTime() > lastReport + LOOP_REPORT_INTERVAL) {
                        MyInputStream teeWithLowestOffset = findTeeWithLowestTeeOffset();
                        if (teeWithLowestOffset == null) {
                            LOGGER.debug("All tees are complete");
                        } else {
                            LOGGER.debug("Waiting for tee: %s (offset: %d)", teeWithLowestOffset.splitName, teeWithLowestOffset.offset);
                        }
                        lastReport = new Date().getTime();
                    }
                    break;
                }
            }
            loop();
        }
    }

    protected void loop() throws Exception {
        synchronized (cyclicBufferLock) {

            // TODO: shouldn't need to do this each loop. Should really only be done if a read occurs.
            updateOffsets();

            if (!sourceComplete && cyclicBufferValidSize < cyclicBuffer.length) {
                int readOffset = cyclicBufferOffsetIndex + cyclicBufferValidSize;
                int readLen = cyclicBuffer.length - cyclicBufferValidSize;

                // read from readOffset to end of buffer
                int partialRedLen = Math.min(cyclicBuffer.length - readOffset, readLen);
                if (partialRedLen > 0) {
                    int read = source.read(cyclicBuffer, readOffset, partialRedLen);
                    if (read == -1) {
                        sourceComplete = true;
                    } else {
                        cyclicBufferValidSize += read;
                        readLen -= read;
                        readOffset += read;
                    }
                }

                // wrap and read from the beginning of the buffer
                if (!sourceComplete && readLen > 0 && readOffset >= cyclicBuffer.length) {
                    readOffset = readOffset % cyclicBuffer.length;
                    int read = source.read(cyclicBuffer, readOffset, readLen);
                    if (read == -1) {
                        sourceComplete = true;
                    } else {
                        cyclicBufferValidSize += read;
                    }
                }

                cyclicBufferLock.notifyAll();
            } else {
                cyclicBufferLock.wait(100);
            }
        }
    }

    private void updateOffsets() {
        synchronized (cyclicBufferLock) {
            long lowestOffset = findLowestTeeOffset();
            if (lowestOffset > cyclicBufferOffset) {
                int delta = (int) (lowestOffset - cyclicBufferOffset);
                cyclicBufferOffset += delta;
                cyclicBufferOffsetIndex += delta;
                cyclicBufferOffsetIndex = cyclicBufferOffsetIndex % cyclicBuffer.length;
                cyclicBufferValidSize -= delta;
            }
        }
    }

    private long findLowestTeeOffset() {
        synchronized (cyclicBufferLock) {
            long lowestOffset = Long.MAX_VALUE;
            for (MyInputStream tee : tees) {
                if (!tee.isClosed() && tee.offset < lowestOffset) {
                    lowestOffset = tee.offset;
                }
            }
            return lowestOffset;
        }
    }

    private MyInputStream findTeeWithLowestTeeOffset() {
        synchronized (cyclicBufferLock) {
            MyInputStream teeWithLowestOffset = null;
            for (MyInputStream tee : tees) {
                if (!tee.isClosed() && (teeWithLowestOffset == null || tee.offset < teeWithLowestOffset.offset)) {
                    teeWithLowestOffset = tee;
                }
            }
            return teeWithLowestOffset;
        }
    }

    public int getMaxNonblockingReadLength(int teeIndex) {
        return tees[teeIndex].getMaxNonblockingReadLength();
    }

    private class MyInputStream extends InputStream implements PausableTimerContextAware {
        private final String splitName;
        private boolean closed;
        private long offset;
        private PausableTimerContext pausableTimerContext;

        public MyInputStream(String splitName) {
            closed = false;
            offset = 0;
            this.splitName = splitName;
        }

        @Override
        public int read() throws IOException {
            pauseTimer();
            try {
                synchronized (cyclicBufferLock) {
                    if (closed) {
                        return -1;
                    }

                    int result = readInternal();
                    if (result != -1) {
                        offset++;
                    }
                    cyclicBufferLock.notifyAll();
                    return result;
                }
            } finally {
                resumeTimer();
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            pauseTimer();
            try {
                synchronized (cyclicBufferLock) {
                    if (closed) {
                        return -1;
                    }
                    if (b.length == 0 || len == 0) {
                        return 0;
                    }

                    int readLength = readInternal(b, off, len);
                    if (readLength != -1) {
                        offset += readLength;
                    }
                    cyclicBufferLock.notifyAll();
                    return readLength;
                }
            } finally {
                resumeTimer();
            }
        }

        private int readInternal() throws IOException {
            synchronized (cyclicBufferLock) {
                if (offset < cyclicBufferOffset) {
                    throw new IOException("attempting to read previous data is not permitted. offset: " + offset + ", cyclicBufferOffset: " + cyclicBufferOffset);
                }
                while (getMaxNonblockingReadLength() <= 0) {
                    if (sourceComplete) {
                        return -1;
                    }
                    try {
                        cyclicBufferLock.wait();
                    } catch (InterruptedException e) {
                        throw new IOException("Cyclic buffer wait failed", e);
                    }
                }
                int readOffset = (int) (offset - cyclicBufferOffset + cyclicBufferOffsetIndex) % cyclicBuffer.length;
                return cyclicBuffer[readOffset];
            }
        }

        private int readInternal(byte[] b, int off, int len) throws IOException {
            synchronized (cyclicBufferLock) {
                if (offset < cyclicBufferOffset) {
                    throw new IOException("attempting to read previous data is not permitted. offset: " + offset + ", cyclicBufferOffset: " + cyclicBufferOffset);
                }
                while (getMaxNonblockingReadLength() <= 0) {
                    if (sourceComplete) {
                        return -1;
                    }
                    try {
                        cyclicBufferLock.wait();
                    } catch (InterruptedException e) {
                        throw new IOException("Cyclic buffer wait failed", e);
                    }
                }
                int readOffset = (int) (offset - cyclicBufferOffset + cyclicBufferOffsetIndex) % cyclicBuffer.length;
                int readLen = Math.min(len, getMaxNonblockingReadLength());
                int bytesRead = 0;

                // read from readOffset to end of buffer
                int partialReadLen = Math.min(cyclicBuffer.length - readOffset, readLen);
                if (partialReadLen > 0) {
                    System.arraycopy(cyclicBuffer, readOffset, b, off, partialReadLen);
                    readLen -= partialReadLen;
                    off += partialReadLen;
                    readOffset += partialReadLen;
                    bytesRead += partialReadLen;
                }

                // read from start of buffer to readLen
                if (readLen > 0) {
                    readOffset = readOffset % cyclicBuffer.length;
                    System.arraycopy(cyclicBuffer, readOffset, b, off, readLen);
                    bytesRead += readLen;
                }

                return bytesRead;
            }
        }

        @Override
        public void close() throws IOException {
            LOGGER.debug("Closing tee: " + splitName);
            try {
                super.close();
            } finally {
                synchronized (cyclicBufferLock) {
                    closed = true;
                    offset = Long.MAX_VALUE;
                    cyclicBufferLock.notifyAll();
                }
            }
        }

        public boolean isClosed() {
            return closed;
        }

        public int getMaxNonblockingReadLength() {
            synchronized (cyclicBufferLock) {
                return (int) (cyclicBufferValidSize - (offset - cyclicBufferOffset));
            }
        }

        @Override
        public void setPausableTimerContext(PausableTimerContext pausableTimerContext) {
            this.pausableTimerContext = pausableTimerContext;
        }

        private void resumeTimer() {
            if (this.pausableTimerContext != null) {
                this.pausableTimerContext.resume();
            }
        }

        private void pauseTimer() {
            if (this.pausableTimerContext != null) {
                this.pausableTimerContext.pause();
            }
        }
    }
}
