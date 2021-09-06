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

import com.google.common.base.Throwables;
import com.mware.core.exception.BcException;
import com.mware.core.exception.BcResourceNotFoundException;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import org.apache.log4j.xml.DOMConfigurator;
import org.json.JSONObject;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public abstract class ConfigurationLoader {
    public static final String ENV_CONFIGURATION_LOADER = "BC_CONFIGURATION_LOADER";
    private static Configuration configuration;
    private final Map initParameters;
    private JSONObject configurationInfo = new JSONObject();

    protected ConfigurationLoader(Map initParameters) {
        this.initParameters = initParameters;
    }

    public static void configureLog4j() {
        ConfigurationLoader configurationLoader = createConfigurationLoader();
        configurationLoader.doConfigureLog4j();
    }

    public static Configuration load() {
        return load(new HashMap());
    }

    public static Configuration load(Map p) {
        return load(getConfigurationLoaderClass(), p);
    }

    public static Class getConfigurationLoaderClass() {
        String configLoaderName = System.getenv(ENV_CONFIGURATION_LOADER);
        if (configLoaderName == null) {
            configLoaderName = System.getProperty(ENV_CONFIGURATION_LOADER);
        }
        if (configLoaderName != null) {
            return getConfigurationLoaderByName(configLoaderName);
        }

        return FileConfigurationLoader.class;
    }

    public static Configuration load(String configLoaderName, Map<String, String> initParameters) {
        Class configLoader;
        if (configLoaderName == null) {
            configLoader = getConfigurationLoaderClass();
        } else {
            configLoader = getConfigurationLoaderByName(configLoaderName);
        }
        return load(configLoader, initParameters);
    }

    public static Class getConfigurationLoaderByName(String configLoaderName) {
        Class configLoader;
        try {
            configLoader = Class.forName(configLoaderName);
        } catch (ClassNotFoundException e) {
            throw new BcException("Could not load class " + configLoaderName, e);
        }
        return configLoader;
    }

    public static Configuration load(Class configLoader, Map initParameters) {
        ConfigurationLoader configurationLoader = createConfigurationLoader(configLoader, initParameters);
        if (configuration == null) {
            configuration = configurationLoader.createConfiguration();
        }

        // This load method overload is at the bottom of the call hierarchy and is the only place guaranteed
        // to get called while loading configuration. It is also early enough in the startup process (ie before
        // SSL connection to databases or data stores are made) to set system properties and have them take effect.
        setSystemProperties(configuration);

        return configuration;
    }

    private static void setSystemProperties(Configuration configuration) {
        Map<String, String> systemProperties = configuration.getSubset("systemProperty");
        for (Map.Entry<String, String> systemProperty : systemProperties.entrySet()) {
            System.setProperty(systemProperty.getKey(), systemProperty.getValue());
        }
    }

    private static ConfigurationLoader createConfigurationLoader() {
        return createConfigurationLoader(null, null);
    }

    private static ConfigurationLoader createConfigurationLoader(Class configLoaderClass, Map initParameters) {
        if (configLoaderClass == null) {
            configLoaderClass = getConfigurationLoaderClass();
        }
        if (initParameters == null) {
            initParameters = new HashMap<>();
        }

        try {
            @SuppressWarnings("unchecked") Constructor constructor = configLoaderClass.getConstructor(Map.class);
            return (ConfigurationLoader) constructor.newInstance(initParameters);
        } catch (Exception e) {
            throw new BcException("Could not load configuration class: " + configLoaderClass.getName(), e);
        }
    }

    public abstract Configuration createConfiguration();

    protected void doConfigureLog4j() {
        File log4jFile = null;
        String log4jLocation = null;
        try {
            log4jFile = resolveFileName("log4j.xml");
        } catch (BcResourceNotFoundException e) {
            // OK, try classpath
        }
        if (log4jFile == null || !log4jFile.exists()) {
            try {
                String fileName = System.getProperty("logQuiet") == null ? "log4j.xml" : "log4j-quiet.xml";
                URL log4jResource = getClass().getResource(fileName);
                System.err.println("Could not resolve log4j.xml, using the fallback: " + log4jResource);
                if (log4jResource != null) {
                    DOMConfigurator.configure(log4jResource);
                    log4jLocation = log4jResource.toExternalForm();
                } else {
                    throw new BcResourceNotFoundException("Could not find log4j.xml on the classpath");
                }
            } catch (RuntimeException e) {
                Throwables.propagate(e);
            }
        } else {
            log4jLocation = log4jFile.getAbsolutePath();
            DOMConfigurator.configure(log4jFile.getAbsolutePath());
        }
        BcLogger logger = BcLoggerFactory.getLogger(BcLoggerFactory.class);
        logger.debug("Using ConfigurationLoader: %s", this.getClass().getName());
        logger.info("log4j.xml config file: %s", log4jLocation);
    }

    public abstract File resolveFileName(String fileName);

    protected Map getInitParameters() {
        return initParameters;
    }

    protected void setConfigurationInfo(String key, Object object) {
        configurationInfo.put(key, object);
    }

    public JSONObject getConfigurationInfo() {
        return configurationInfo;
    }
}
