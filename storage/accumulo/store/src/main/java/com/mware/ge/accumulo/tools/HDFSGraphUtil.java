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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class HDFSGraphUtil {

    static FileSystem getHdfsFileSystem(String fs, boolean useDatanodeHostname, String hadoopUsername) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        if (useDatanodeHostname) {
            conf.set("dfs.client.use.datanode.hostname", "true");
        }
        if (hadoopUsername != null) {
            return FileSystem.get(new URI(fs), conf, hadoopUsername);
        }
        return FileSystem.get(new URI(fs), conf);
    }

    static Path getPath(FileSystem fileSystem, String hdfsDirectory, String fileName) {
        if (!hdfsDirectory.startsWith("hdfs")) {
            hdfsDirectory = fileSystem.getUri() + hdfsDirectory;
        }
        return new Path(hdfsDirectory, fileName);
    }

    static Path getPath(FileSystem fileSystem, String hdfsDirectory) {
        if (!hdfsDirectory.startsWith("hdfs")) {
            hdfsDirectory = fileSystem.getUri() + hdfsDirectory;
        }
        return new Path(hdfsDirectory);
    }

    static Path createFile(FileSystem fileSystem, String dir, String filename) throws IOException {
        Path rootDirPath = HDFSGraphUtil.getPath(fileSystem, dir);
        if (!fileSystem.exists(rootDirPath)) {
            fileSystem.mkdirs(rootDirPath);
        }
        Path backupFilePath = HDFSGraphUtil.getPath(fileSystem, dir, filename);
        if (!fileSystem.exists(backupFilePath)) {
            fileSystem.createNewFile(backupFilePath);
        }
        return backupFilePath;
    }

    static List<String> listFiles(FileSystem fileSystem, String dir, Optional<String> filenamePrefix) throws IOException {
        Path backupFolderPath = getPath(fileSystem, dir);
        RemoteIterator<LocatedFileStatus> fileIterator = fileSystem.listFiles(backupFolderPath, false);
        List<String> filesThatMatch =  new ArrayList<>();
        while (fileIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileIterator.next();
            if (filenamePrefix.isPresent()) {
                if (fileStatus.getPath().getName().startsWith(filenamePrefix.get())) {
                    filesThatMatch.add(fileStatus.getPath().getName());
                }
            } else {
                filesThatMatch.add(fileStatus.getPath().getName());
            }
        }
        return filesThatMatch;
    }
}
