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
package com.mware.core.model.file;

import com.google.inject.Inject;
import com.mware.core.config.BcHadoopConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class HdfsFileSystemRepository extends FileSystemRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(HdfsFileSystemRepository.class);
    private static final String CONFIG_PREFIX = HdfsFileSystemRepository.class.getName() + ".prefix";
    private static final String CONFIG_PREFIX_DEFAULT = "/bigconnect/config/";
    private final FileSystem hdfsFileSystem;
    private final String prefix;

    @Inject
    public HdfsFileSystemRepository(Configuration configuration) {
        hdfsFileSystem = getFileSystem(configuration);
        prefix = configuration.get(CONFIG_PREFIX, CONFIG_PREFIX_DEFAULT);
    }

    public static FileSystem getFileSystem(Configuration configuration) {
        String hdfsUserName = configuration.get(BcHadoopConfiguration.HDFS_USER_NAME, BcHadoopConfiguration.HDFS_USER_NAME_DEFAULT);
        String fsDefaultFS = configuration.get("fs.defaultFS", null);
        try {
            return FileSystem.get(new URI(fsDefaultFS), BcHadoopConfiguration.getHadoopConfiguration(configuration), hdfsUserName);
        } catch (Exception e) {
            throw new BcException("Could not open hdfs filesystem: " + fsDefaultFS + " (user: " + hdfsUserName + ")", e);
        }
    }

    @Override
    public File getLocalFileFor(String path) {
        try {
            Path filePath = getHdfsPath(path);
            String fileName = filePath.getName();
            File tempFile = new File(System.getProperty("java.io.tmpdir"), fileName);
            LOGGER.debug("Copying %s to %s", filePath, tempFile.getAbsolutePath());
            try (InputStream shapeInputStream = hdfsFileSystem.open(filePath)) {
                FileUtils.copyInputStreamToFile(shapeInputStream, tempFile);
            }
            return tempFile;
        } catch (IOException ex) {
            throw new BcException("Could not copy file: " + path, ex);
        }
    }

    private Path getHdfsPath(String path) {
        try {
            Path filePath = new Path(prefix + path);
            if (!hdfsFileSystem.exists(filePath)) {
                throw new BcException("Could not find file: " + hdfsFileSystem.getUri() + filePath);
            }
            return filePath;
        } catch (IOException ex) {
            throw new BcException("Could not get path for: " + path, ex);
        }
    }

    @Override
    public InputStream getInputStream(String path) {
        try {
            return hdfsFileSystem.open(getHdfsPath(path));
        } catch (IOException ex) {
            throw new BcException("Could not open file: " + path, ex);
        }
    }

    @Override
    public Iterable<String> list(String path) {
        List<String> results = new ArrayList<>();
        try {
            FileStatus[] statuses = hdfsFileSystem.listStatus(getHdfsPath(path));
            for (FileStatus status : statuses) {
                results.add(status.getPath().getName());
            }
        } catch (IOException e) {
            throw new BcException("Could not get files for: " + path);
        }
        return results;
    }

    public FileSystem getHdfsFileSystem() {
        return hdfsFileSystem;
    }
}
