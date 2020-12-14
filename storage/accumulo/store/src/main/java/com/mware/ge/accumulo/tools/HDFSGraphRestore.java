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

import com.mware.ge.tools.GraphRestore;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public class HDFSGraphRestore extends GraphRestore {
    private final String hadoopFs;
    private final boolean useDatanodeName;
    private final String hadoopUsername;

    public HDFSGraphRestore(String hadoopFs, String rootDir,
                            boolean useDatanodeName, String hadoopUsername) {
        super(rootDir);
        this.hadoopFs = hadoopFs;
        this.useDatanodeName = useDatanodeName;
        this.hadoopUsername = hadoopUsername;
    }

    public Optional<String> getLastBackupFile(String backupFilePrefix) {
        try {
            FileSystem fileSystem = HDFSGraphUtil.getHdfsFileSystem(hadoopFs, useDatanodeName, hadoopUsername);
            List<String> filesThatMatch = HDFSGraphUtil.listFiles(fileSystem, rootDir, Optional.of(backupFilePrefix));
            return filesThatMatch.stream()
                    .sorted((f1Name, f2Name) -> {
                        String date1Value = f1Name.replace(backupFilePrefix, "").replace(DEFAULT_GRAPH_BACKUP_EXT, "");
                        LocalDateTime date1 = LocalDateTime.parse(date1Value, BACKUP_DATETIME_FORMATTER);
                        String date2Value = f2Name.replace(backupFilePrefix, "").replace(DEFAULT_GRAPH_BACKUP_EXT, "");
                        LocalDateTime date2 = LocalDateTime.parse(date2Value, BACKUP_DATETIME_FORMATTER);
                        return date2.compareTo(date1);
                    })
                    .findFirst();
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    public InputStream createInputStream(String fileName) throws FileNotFoundException {
        try {
            FileSystem fileSystem = HDFSGraphUtil.getHdfsFileSystem(hadoopFs, useDatanodeName, hadoopUsername);
            FSDataInputStream in = fileSystem.open(HDFSGraphUtil.getPath(fileSystem, rootDir, fileName));
            return in;
        } catch (Exception e) {
            e.printStackTrace();
            throw new FileNotFoundException(e.getMessage());
        }
    }

    @Override
    public String getAbsoluteFilePath(String filename) {
        return hadoopFs + rootDir + "/" + filename;
    }

}