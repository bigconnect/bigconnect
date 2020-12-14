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
package com.mware.core.config;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import com.mware.core.exception.BcException;
import com.mware.core.exception.BcResourceNotFoundException;
import com.mware.core.util.ProcessUtil;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * By default searches for bigCONNECT configuration directories in this order:
 * - Location specified by system property BIGCONNECT_DIR
 * - Location specified by environment variable BIGCONNECT_DIR
 * - ${user.home}/.bigconnect
 * - ${appdata}/bigconnect
 * - /opt/bigconnect/ or c:/opt/bigconnect/
 * <p/>
 * You can override the default search order using a system property or environment property BC_CONFIGURATION_LOADER_SEARCH_LOCATIONS.
 * The default is: systemProperty,env,userHome,appdata,defaultDir
 */
public class FileConfigurationLoader extends ConfigurationLoader {
    /**
     * !!! DO NOT DEFINE A LOGGER here. This class get loaded very early in the process and we don't want to the logger to be initialized yet **
     */
    public static final String ENV_BC_DIR = "BIGCONNECT_DIR";
    public static final String DEFAULT_UNIX_LOCATION = "/opt/bigconnect/";
    public static final String DEFAULT_WINDOWS_LOCATION = "/opt/bigconnect/";

    public static final String ENV_SEARCH_LOCATIONS = "BC_CONFIGURATION_LOADER_SEARCH_LOCATIONS";
    public static final String ENV_SEARCH_LOCATIONS_DEFAULT = Joiner.on(",").join(new String[]{
            SearchLocation.SystemProperty.getValue(),
            SearchLocation.EnvironmentVariable.getValue(),
            SearchLocation.UserHome.getValue(),
            SearchLocation.AppData.getValue(),
            SearchLocation.BcDefaultDirectory.getValue()
    });

    public FileConfigurationLoader(Map initParameters) {
        super(initParameters);
    }

    public Configuration createConfiguration() {
        final Map<String, String> properties = getDefaultProperties();
        List<File> configDirectories = getBcDirectoriesFromLeastPriority("config");
        if (configDirectories.size() == 0) {
            throw new BcException("Could not find any valid config directories.");
        }
        List<String> loadedFiles = new ArrayList<>();
        for (File directory : configDirectories) {
            Map<String, String> directoryProperties = loadDirectory(directory, loadedFiles);
            properties.putAll(directoryProperties);
        }
        setConfigurationInfo("loadedFiles", loadedFiles);
        return new Configuration(this, properties);
    }

    private Map<String, String> getDefaultProperties() {
        Map<String, String> defaultProperties = new HashMap<>(getInitParameters());

        List<File> configDirs = getBcDirectoriesFromMostPriority("config");
        if (configDirs.size() > 0) {
            String bcDir = configDirs.get(0).getParentFile().getAbsolutePath();
            defaultProperties.put(ENV_BC_DIR, bcDir);
        }

        defaultProperties.putAll(System.getenv());

        return defaultProperties;
    }

    public static List<File> getBcDirectoriesFromMostPriority(String subDirectory) {
        List<File> results = new ArrayList<>();

        List<SearchLocation> searchLocations = getSearchLocations();
        for (SearchLocation searchLocation : searchLocations) {
            switch (searchLocation) {
                case AppData:
                    String appData = System.getProperty("appdata");
                    if (appData != null && appData.length() > 0) {
                        addBcSubDirectory(
                                results,
                                new File(new File(appData), "bigconnect").getAbsolutePath(),
                                subDirectory
                        );
                    }
                    break;

                case EnvironmentVariable:
                    addBcSubDirectory(results, System.getenv(ENV_BC_DIR), subDirectory);
                    break;

                case SystemProperty:
                    addBcSubDirectory(results, System.getProperty(ENV_BC_DIR, null), subDirectory);
                    break;

                case UserHome:
                    String userHome = System.getProperty("user.home");
                    if (userHome != null && userHome.length() > 0) {
                        addBcSubDirectory(
                                results,
                                new File(new File(userHome), ".bigconnect").getAbsolutePath(),
                                subDirectory
                        );
                    }
                    break;

                case BcDefaultDirectory:
                    String defaultBcDir = getDefaultBcDir();
                    addBcSubDirectory(results, defaultBcDir, subDirectory);
                    break;

                default:
                    throw new BcException("Unhandled search type: " + searchLocation);
            }
        }

        return ImmutableList.copyOf(results);
    }

    public static List<File> getBcDirectoriesFromLeastPriority(String subDirectory) {
        return Lists.reverse(getBcDirectoriesFromMostPriority(subDirectory));
    }

    private static List<SearchLocation> getSearchLocations() {
        String locationsString = System.getProperty(ENV_SEARCH_LOCATIONS);
        if (locationsString == null) {
            locationsString = System.getenv(ENV_SEARCH_LOCATIONS);
            if (locationsString == null) {
                locationsString = ENV_SEARCH_LOCATIONS_DEFAULT;
            }
        }

        String[] locationItems = locationsString.split(",");
        List<SearchLocation> searchLocations = new ArrayList<>();
        for (String locationItem : locationItems) {
            searchLocations.add(SearchLocation.parse(locationItem));
        }
        return searchLocations;
    }

    public static String getDefaultBcDir() {
        String _location = null;
        if (ProcessUtil.isWindows()) {
            _location = DEFAULT_WINDOWS_LOCATION;
        } else {
            _location = DEFAULT_UNIX_LOCATION;
        }
        File f = new File(_location);
        if (!f.exists()) {
            _location = System.getProperty("catalina.base") + "\\bc-conf";
        }

        return _location;
    }

    private static void addBcSubDirectory(List<File> results, String location, String subDirectory) {
        if (location == null || location.trim().length() == 0) {
            return;
        }

        location = location.trim();
        if (location.startsWith("file://")) {
            location = location.substring("file://".length());
        }

        File dir = new File(new File(location), subDirectory);
        if (!dir.exists()) {
            return;
        }

        results.add(dir);
    }

    private static Map<String, String> loadDirectory(File configDirectory, List<String> loadedFiles) {
        BcLogger LOGGER = BcLoggerFactory.getLogger(FileConfigurationLoader.class);

        LOGGER.debug("Attempting to load configuration from directory: %s", configDirectory);
        if (!configDirectory.exists()) {
            throw new BcException("Could not find config directory: " + configDirectory);
        }

        File[] files = configDirectory.listFiles();
        if (files == null) {
            throw new BcException("Could not parse directory name: " + configDirectory);
        }
        // sort similar to IntelliJ, bc.properties should come before bc-*.properties
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return getComparableFileName(o1).compareTo(getComparableFileName(o2));
            }

            private String getComparableFileName(File o1) {
                return FilenameUtils.getBaseName(o1.getName()).toLowerCase();
            }
        });
        Map<String, String> properties = new HashMap<>();
        for (File f : files) {
            if (!f.getAbsolutePath().endsWith(".properties")) {
                continue;
            }
            try {
                Map<String, String> fileProperties = loadFile(f.getAbsolutePath(), loadedFiles);
                for (Map.Entry<String, String> filePropertyEntry : fileProperties.entrySet()) {
                    properties.put(filePropertyEntry.getKey(), filePropertyEntry.getValue());
                }
            } catch (IOException ex) {
                throw new BcException("Could not load config file: " + f.getAbsolutePath(), ex);
            }
        }

        return properties;
    }

    private static Map<String, String> loadFile(final String fileName, List<String> loadedFiles) throws IOException {
        BcLogger LOGGER = BcLoggerFactory.getLogger(FileConfigurationLoader.class);

        Map<String, String> results = new HashMap<>();
        LOGGER.info("Loading config file: %s", fileName);
        try (FileInputStream in = new FileInputStream(fileName)) {
            Properties properties = new Properties();
            properties.load(in);
            for (Map.Entry<Object, Object> prop : properties.entrySet()) {
                String key = prop.getKey().toString();
                String value = prop.getValue().toString();
                results.put(key, value);
            }
            loadedFiles.add(fileName);
        } catch (Exception e) {
            LOGGER.info("Could not load configuration file: %s", fileName);
        }
        return results;
    }

    @Override
    public File resolveFileName(String fileName) {
        return resolveLocalFileName(fileName);
    }

    public static File resolveLocalFileName(String fileName) {
        List<File> configDirectories = getBcDirectoriesFromMostPriority("config");
        if (configDirectories.size() == 0) {
            throw new BcResourceNotFoundException("Could not find any valid config directories.");
        }
        for (File directory : configDirectories) {
            File f = new File(directory, fileName);
            if (f.exists()) {
                return f;
            }
        }
        throw new BcResourceNotFoundException("Could not find file: " + fileName);
    }

    public enum SearchLocation {
        BcDefaultDirectory("defaultDir"),
        AppData("appdata"),
        UserHome("userHome"),
        EnvironmentVariable("env"),
        SystemProperty("systemProperty");

        private final String value;

        SearchLocation(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return getValue();
        }

        public static SearchLocation parse(String searchType) {
            for (SearchLocation type : SearchLocation.values()) {
                if (type.name().equalsIgnoreCase(searchType) || type.getValue().equalsIgnoreCase(searchType)) {
                    return type;
                }
            }
            throw new BcException("Could not parse search type: " + searchType);
        }
    }
}
