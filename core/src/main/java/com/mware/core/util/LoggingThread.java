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

import java.io.*;

import static com.mware.ge.util.CloseableUtils.closeQuietly;

/**
 * This class is designed to log {@link Process} output on a thread.
 */
public class LoggingThread extends Thread {
    private InputStream inputStream;
    private OutputStream outputStream;
    private StringBuffer contentBuffer;
    private String prefix;
    private PrintWriter writer;
    private BcLogger logger;

    public LoggingThread(InputStream inputStream, BcLogger logger, String prefix) {
        this(inputStream, null, logger, null, prefix);
    }

    /**
     * Creates a new stream helper and immediately starts capturing output from
     * the given stream. Output will be captured to the given buffer and also
     * redirected to the provided output stream.
     *
     * @param inputStream   the input stream to read from
     * @param redirect      a stream to also redirect the captured output to
     * @param logger        the logger to append to
     * @param contentBuffer the buffer to write the captured output to
     */
    public LoggingThread(InputStream inputStream, OutputStream redirect,
                         BcLogger logger, StringBuffer contentBuffer, String prefix) {
        this.inputStream = inputStream;
        this.outputStream = redirect;
        this.logger = logger;
        this.contentBuffer = contentBuffer;
        this.prefix = prefix;
    }

    @Override
    public void run() {
        BufferedReader reader = null;
        InputStreamReader isreader = null;
        try {
            if (outputStream != null) {
                writer = new PrintWriter(outputStream);
            }
            isreader = new InputStreamReader(inputStream);
            reader = new BufferedReader(isreader);
            String line;
            while ((line = reader.readLine()) != null) {
                if (prefix != null) {
                    line = prefix + line;
                }
                append(line);
                log(line);
            }
            if (writer != null)
                writer.flush();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            closeQuietly(reader);
            closeQuietly(isreader);
            closeQuietly(writer);
        }
    }

    /**
     * This method will write any output from the stream to the the content buffer
     * and the logger.
     *
     * @param output the stream output
     */
    protected void append(String output) {
        // Process stream redirects
        if (writer != null) {
            writer.println(output);
        }

        // Fill the content buffer, if one has been assigned
        if (contentBuffer != null) {
            contentBuffer.append(output.trim());
            contentBuffer.append('\n');
        }

        // Append output to logger?
    }

    /**
     * If a logger has been specified, the output is written to the logger using
     * the defined log level.
     *
     * @param output the stream output
     */
    protected void log(String output) {
        if (logger != null) {
            logger.info("%s", output);
        }
    }
}
