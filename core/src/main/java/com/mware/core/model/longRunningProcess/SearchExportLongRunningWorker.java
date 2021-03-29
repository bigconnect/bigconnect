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
 *
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
package com.mware.core.model.longRunningProcess;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.exception.BcException;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.search.*;
import com.mware.core.model.user.UserRepository;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ClientApiConverter;
import com.mware.ge.*;
import com.mware.ge.tools.GraphBackup;
import com.mware.ge.tools.GraphToolBase;
import com.mware.ge.values.storable.StreamingPropertyValue;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Name("Export Raw Search")
@Description("Export archive with raw elements from search results")
@Singleton
public class SearchExportLongRunningWorker extends LongRunningProcessWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(SearchExportLongRunningWorker.class);

    public static final String TYPE = "export-raw-search";

    protected final LongRunningProcessRepository longRunningProcessRepository;
    protected final UserRepository userRepository;
    protected final GraphBaseWithSearchIndex graph;
    protected final SearchRepository searchRepository;

    @Inject
    public SearchExportLongRunningWorker(
            LongRunningProcessRepository longRunningProcessRepository,
            UserRepository userRepository,
            Graph graph,
            SearchRepository searchRepository) {
        this.longRunningProcessRepository = longRunningProcessRepository;
        this.userRepository = userRepository;
        this.graph = (GraphBaseWithSearchIndex) graph;
        this.searchRepository = searchRepository;
    }

    @Override
    public boolean isHandled(JSONObject longRunningProcessQueueItem) {
        return TYPE.equals(longRunningProcessQueueItem.getString("type"));
    }

    @Override
    public void processInternal(final JSONObject config) {
        SearchExportLRPQueueItem queueItem = ClientApiConverter
                .toClientApi(config.toString(), SearchExportLRPQueueItem.class);

        SearchOptions searchOptions = new SearchOptions(queueItem.getParameters(), queueItem.getWorkspaceId());
        GeObjectSearchRunnerBase searchRunner =
                (GeObjectSearchRunnerBase) searchRepository.findSearchRunnerByUri(VertexSearchRunner.URI);

        User user = userRepository.findById(queueItem.getUserId());
        if (user == null) {
            LOGGER.error(String.format("User with id %s not found.", queueItem.getUserId()));
            return;
        }

        LOGGER.info("Start long running export raw search for user: %s, workspaceId: %s",
                user.getDisplayName(), queueItem.getWorkspaceId());
        config.put("backupFile", "Running...");
        try (QueryResultsIterableSearchResults searchResults =
                     searchRunner.run(searchOptions, user, new Authorizations(queueItem.getAuthorizations()))) {
            long size = searchResults.getQueryResultsIterable().getTotalHits();
            LOGGER.info("Found %s element(s) for export", size);

            GraphBackup backupTool = this.graph.getBackupTool(null);
            JSONArray export = new JSONArray();

            double progress = 0;
            if (size > 0) {
                progress += 0.3;
                longRunningProcessRepository.reportProgress(config, progress,
                        String.format("Finished running search, found %d item(s).", size));
            } else {
                LOGGER.error("Search for raw export returned no items.");
                throw new BcException("Search for raw export returned no items.");
            }

            long idx = 0;
            File outputDirectory = new File(this.graph.getBackupDir(), archiveFolderName());
            File outputFile;
            for (GeObject geObject : searchResults.getQueryResultsIterable()) {
                if (geObject instanceof Vertex) {
                    final Vertex _vertex = (Vertex)geObject;
                    export.put(backupTool.vertexToJson(_vertex, false));

                    // Write raw file if that's the case
                    final Property fileName = _vertex.getProperty("fileName");
                    final Property raw = _vertex.getProperty("raw");
                    if (fileName != null && raw != null) {
                        outputFile = new File(outputDirectory, fileName.getValue().asObjectCopy().toString());
                        StreamingPropertyValue spv = (StreamingPropertyValue)raw.getValue();
                        FileUtils.copyInputStreamToFile(spv.getInputStream(), outputFile);
                    }
                } else {
                    LOGGER.warn("Element of class %s was ignored from export", geObject.getClass().getName());
                }

                progress += 0.7 / size;
                idx += 1;
                longRunningProcessRepository.reportProgress(config, progress,
                        String.format("Exported %d of %d item(s).", idx, size));
            }

            // Write meta file
            File metaFile = new File(outputDirectory, getBackupFileName());
            FileUtils.write(metaFile, export.toString(4));

            // Create zip archive
            final String zipPath = createZip(this.graph.getBackupDir(), outputDirectory);
            deleteFolder(outputDirectory);

            searchResults.getQueryResultsIterable().close();
            config.put("filePath", zipPath);
            config.put("backupFile", new File(zipPath).getName());
            config.put("resultsCount", size);
            longRunningProcessRepository.reportProgress(config, 1.0,
                        String.format("Finished running export raw search for %d item(s).", size));
        } catch (Exception e) {
            LOGGER.error("Export RAW Search failed with message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String createZip(String path, File directory) {
        try {
            final String zipPath = path + File.separator + archiveFolderName() + ".zip";
            FileOutputStream fos = new FileOutputStream(zipPath);
            ZipOutputStream zipOut = new ZipOutputStream(fos);
            zipFile(directory, directory.getName(), zipOut);
            zipOut.close();
            fos.close();
            return zipPath;
        } catch (IOException e) {
            LOGGER.error("Could not create zip file. Error message: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private void zipFile(File fileToZip, String fileName, ZipOutputStream zipOut) throws IOException {
        if (fileToZip.isHidden()) {
            return;
        }
        if (fileToZip.isDirectory()) {
            if (fileName.endsWith("/")) {
                zipOut.putNextEntry(new ZipEntry(fileName));
                zipOut.closeEntry();
            } else {
                zipOut.putNextEntry(new ZipEntry(fileName + "/"));
                zipOut.closeEntry();
            }
            File[] children = fileToZip.listFiles();
            if (children != null) {
                for (File childFile : children) {
                    zipFile(childFile, fileName + "/" + childFile.getName(), zipOut);
                }
            }
            return;
        }
        FileInputStream fis = new FileInputStream(fileToZip);
        ZipEntry zipEntry = new ZipEntry(fileName);
        zipOut.putNextEntry(zipEntry);
        byte[] bytes = new byte[1024];
        int length;
        while ((length = fis.read(bytes)) >= 0) {
            zipOut.write(bytes, 0, length);
        }
        fis.close();
    }

    private void deleteFolder(File directory) {
        String[] entries = directory.list();
        if (entries != null) {
            for (String s: entries){
                File currentFile = new File(directory.getPath(), s);
                int retries = 3;
                while (!currentFile.delete() && retries > 0) {
                    retries--;
                }
            }
        }
        int retries = 3;
        while (!directory.delete() && retries > 0) {
            retries--;
        }
    }

    private String getBackupFileName() {
        return "search_export" + GraphToolBase.DEFAULT_GRAPH_BACKUP_EXT;
    }

    private String archiveFolderName() {
        return LocalDateTime.now().format(GraphToolBase.BACKUP_DATETIME_FORMATTER);
    }
}
