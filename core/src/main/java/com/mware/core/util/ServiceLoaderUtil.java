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

import com.google.common.collect.Iterables;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.*;

/**
 * This class exists to provide much deeper and extensive debugging and logging as
 * opposed to (@see java.util.ServiceLoader)
 */
public class ServiceLoaderUtil {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ServiceLoaderUtil.class);
    private static final String PREFIX = "META-INF/services/";
    public static final String CONFIG_DISABLE_PREFIX = "disable.";

    public static <T> Iterable<T> load(Class<T> clazz, Configuration configuration) {
        Iterable<Class<? extends T>> classes = loadClasses(clazz, configuration);
        return Iterables.transform(classes, serviceClass -> {
            try {
                return InjectHelper.getInstance(serviceClass);
            } catch (Exception ex) {
                String errorMessage = String.format("Failed to load %s", serviceClass.getName());
                LOGGER.error("%s", errorMessage, ex);
                throw new BcException(errorMessage, ex);
            }
        });
    }

    public static <T> Iterable<T> loadWithoutInjecting(Class<T> clazz, Configuration configuration) {
        Iterable<Class<? extends T>> classes = loadClasses(clazz, configuration);
        return Iterables.transform(classes, serviceClass -> {
            try {
                Constructor<? extends T> constructor = serviceClass.getConstructor();
                return constructor.newInstance();
            } catch (Exception ex) {
                String errorMessage = String.format("Failed to load %s", serviceClass.getName());
                LOGGER.error("%s", errorMessage, ex);
                throw new BcException(errorMessage, ex);
            }
        });
    }

    public static <T> Iterable<Class<? extends T>> loadClasses(Class<T> clazz, Configuration configuration) {
        Set<Class<? extends T>> services = new HashSet<>();
        String fullName = PREFIX + clazz.getName();
        LOGGER.debug("loading services for class %s", fullName);
        try {
            Enumeration<URL> serviceFiles = Thread.currentThread().getContextClassLoader().getResources(fullName);
            if (!serviceFiles.hasMoreElements()) {
                LOGGER.debug("Could not find any services for %s", fullName);
            } else {
                Set<URL> serviceFilesSet = new HashSet<>();
                while (serviceFiles.hasMoreElements()) {
                    URL serviceFile = serviceFiles.nextElement();
                    serviceFilesSet.add(serviceFile);
                }

                Map<String, URL> loadedClassNames = new HashMap<>();
                for (URL serviceFile : serviceFilesSet) {
                    List<String> fileClassNames = loadFile(serviceFile);
                    for (String className : fileClassNames) {
                        if (configuration.getBoolean(CONFIG_DISABLE_PREFIX + className, false)) {
                            LOGGER.info("ignoring class %s because it is disabled in configuration", className);
                            continue;
                        }
                        if (loadedClassNames.containsKey(className)) {
                            LOGGER.warn("ignoring class '%s' because it is already loaded from '%s'", className, loadedClassNames.get(className));
                            continue;
                        }
                        Class<? extends T> loadedClass = ServiceLoaderUtil.<T>loadClass(serviceFile, className);

                        BcPlugin bcPlugin = loadedClass.getAnnotation(BcPlugin.class);
                        if (bcPlugin != null) {
                            if (configuration.getBoolean(CONFIG_DISABLE_PREFIX + className, bcPlugin.disabledByDefault())) {
                                LOGGER.debug("ignoring class %s because it is disabled by default", className);
                                continue;
                            }
                        }

                        services.add(loadedClass);
                        loadedClassNames.put(className, serviceFile);
                    }
                }
            }

            return services;
        } catch (IOException e) {
            throw new BcException("Could not load services for class: " + clazz.getName(), e);
        }
    }

    private static List<String> loadFile(URL serviceFile) throws IOException {
        List<String> results = new ArrayList<>();
        LOGGER.debug("loadFile(%s)", serviceFile);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(serviceFile.openStream()))) {
            String className;
            while ((className = reader.readLine()) != null) {
                className = className.trim();
                if (className.length() == 0) {
                    continue;
                }
                results.add(className);
            }
        }
        return results;
    }

    private static <T> Class<? extends T> loadClass(URL config, String className) {
        try {
            LOGGER.debug("Loading %s from %s", className, config.toString());
            return ClassUtil.forName(className);
        } catch (Throwable t) {
            String errorMessage = String.format("Failed to load %s from %s", className, config.toString());
            LOGGER.error("%s", errorMessage, t);
            throw new BcException(errorMessage, t);
        }
    }
}
