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
package com.mware.ge.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import com.mware.core.config.Configuration;
import com.mware.ge.Authorizations;
import com.mware.ge.GeException;
import com.mware.ge.Graph;
import com.mware.ge.GraphFactory;
import com.mware.ge.util.ConfigurationUtils;

import java.io.File;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Parameters(separators = "=")
public abstract class GraphToolBase {
    public static final DateTimeFormatter BACKUP_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss");
    public static final String DEFAULT_GRAPH_BACKUP_EXT = ".ge";
    @Parameter(names = {"-c", "--config"}, description = "Configuration file name")
    private List<String> configFileNames = new ArrayList<>();

    @Parameter(names = {"-a", "--auth"}, description = "Comma separated string of Authorizations")
    private String authString = "administrator";

    @Parameter(names = {"-cd"}, description = "Configuration directories (all files ending in .properties)")
    private List<String> configDirectories = new ArrayList<>();

    @Parameter(names = {"-cp"}, description = "Configuration property prefix")
    private String configPropertyPrefix = null;

    private Graph graph;
    protected String rootDir = System.getProperty("user.dir");

    public GraphToolBase() {
    }

    public GraphToolBase(String rootDir) {
        this.rootDir = rootDir;
    }

    protected void run(String[] args) throws Exception {
        new JCommander(this).parse(args);
        addConfigDirectoriesToConfigFileNames(configDirectories, configFileNames);
        Map<String, Object> config = ConfigurationUtils.loadConfig(configFileNames, configPropertyPrefix);
        graph = new GraphFactory().createGraph(new Configuration(config));
    }

    private void addConfigDirectoriesToConfigFileNames(List<String> configDirectories, List<String> configFileNames) {
        for (String configDirectory : configDirectories) {
            addConfigDirectoryToConfigFileNames(configDirectory, configFileNames);
        }
    }

    private void addConfigDirectoryToConfigFileNames(String configDirectory, List<String> configFileNames) {
        File dir = new File(configDirectory);
        if (!dir.exists()) {
            throw new GeException("Directory does not exist: " + dir.getAbsolutePath());
        }
        List<String> files = Lists.newArrayList(dir.listFiles()).stream()
                .filter(File::isFile)
                .map(File::getName)
                .filter(f -> f.endsWith(".properties"))
                .collect(Collectors.toList());
        Collections.sort(files);
        files = files.stream()
                .map(f -> new File(dir, f).getAbsolutePath())
                .collect(Collectors.toList());
        configFileNames.addAll(files);
    }

    protected Authorizations getAuthorizations() {
        // TODO change this to be configurable
        String[] split = authString.split(",");
        if (split.length == 1 && split[0].length() == 0) {
            split = new String[0];
        }
        return new Authorizations(split);
    }

    protected Graph getGraph() {
        return graph;
    }

    public String getAbsoluteFilePath(String filename) {
        return new File(rootDir, filename).getAbsolutePath();
    }

}
