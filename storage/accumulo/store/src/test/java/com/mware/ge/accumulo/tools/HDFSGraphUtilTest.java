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
package com.mware.ge.accumulo.tools;

import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;

/**
 * Meant to be run locally, need to set correct URI and credential for this to work.
 */
@Ignore
public class HDFSGraphUtilTest {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(HDFSGraphUtilTest.class);

    private static final String HDFS = "hdfs://test:8020";
    private static final String HDFS_TEST_DIR = "/bigconnect/backup";
    private static final String HDFS_USER = "hadoop";
    private static final boolean USE_DATANODE_HOSTNAME = true;

    @Test
    public void createFile() throws InterruptedException, IOException, URISyntaxException {
        FileSystem fileSystem = HDFSGraphUtil.getHdfsFileSystem(HDFS, USE_DATANODE_HOSTNAME, HDFS_USER);
        HDFSGraphUtil.createFile(fileSystem, HDFS_TEST_DIR, "test.ge");
    }

    @Test
    public void listFiles() throws InterruptedException, IOException, URISyntaxException {
        FileSystem fileSystem = HDFSGraphUtil.getHdfsFileSystem(HDFS, USE_DATANODE_HOSTNAME, HDFS_USER);
        List<String> filenames = HDFSGraphUtil.listFiles(fileSystem, HDFS_TEST_DIR, Optional.of("vertex"));
        LOGGER.info("Found files: %s", filenames);
    }
}