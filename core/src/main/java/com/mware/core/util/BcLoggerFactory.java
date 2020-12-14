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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.slf4j.LoggerFactory;
import com.mware.core.config.ConfigurationLoader;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class BcLoggerFactory {
    private static final Map<String, BcLogger> logMap = new HashMap<>();
    private static boolean initialized = false;
    private static boolean initializing = false;

    public static BcLogger getLogger(Class clazz, String processType) {
        if (processType != null) {
            setProcessType(processType);
        } else {
            setProcessType(clazz.getSimpleName());
        }
        ensureInitialized();
        return getLogger(clazz.getName());
    }

    public static BcLogger getLogger(Class clazz) {
        return getLogger(clazz, null);
    }

    private static void ensureInitialized() {
        synchronized (logMap) {
            if (!initialized && !initializing) {
                initializing = true;
                if (System.getProperty("logFileSuffix") == null) {
                    String hostname = null;
                    try {
                        hostname = InetAddress.getLocalHost().getHostName();
                    } catch (UnknownHostException e) {
                        System.err.println("Could not get host name: " + e.getMessage());
                    }
                    String logFileSuffix = "-" + Joiner.on("-").skipNulls().join(getProcessType(), hostname, ProcessUtil.getPid());
                    System.setProperty("logFileSuffix", logFileSuffix);
                }
                ConfigurationLoader.configureLog4j();
                initialized = true;
                initializing = false;
                logSystem();
            }
        }
    }

    private static void logSystem() {
        BcLogger logger = getLogger(BcLoggerFactory.class);
        if(logger.isDebugEnabled()) {
            logEnv(logger);
            logSystemProperties(logger);
            logJvmInputArguments(logger);
        }
    }

    private static void logJvmInputArguments(BcLogger logger) {
        logger.info("jvm input arguments:");
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> arguments = runtimeMxBean.getInputArguments();
        for (String arg : arguments) {
            logger.info("  %s", arg);
        }
    }

    private static void logSystemProperties(BcLogger logger) {
        logger.info("system properties:");
        ArrayList<Map.Entry<Object, Object>> properties = Lists.newArrayList(System.getProperties().entrySet());
        Collections.sort(properties, new Comparator<Map.Entry<Object, Object>>() {
            @Override
            public int compare(Map.Entry<Object, Object> o1, Map.Entry<Object, Object> o2) {
                return o1.getKey().toString().compareTo(o2.getKey().toString());
            }
        });
        for (final Map.Entry<Object, Object> entry : properties) {
            logger.info("  %s: %s", entry.getKey(), entry.getValue());
        }
    }

    private static void logEnv(BcLogger logger) {
        logger.info("environment:");
        ArrayList<Map.Entry<String, String>> entries = Lists.newArrayList(System.getenv().entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, String>>() {
            @Override
            public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        for (final Map.Entry<String, String> entry : entries) {
            logger.info("  %s: %s", entry.getKey(), entry.getValue());
        }
    }

    public static void setProcessType(String processType) {
        if (getProcessType() == null) {
            if (initializing) {
                System.err.println("setProcessType called too late");
            } else if (initialized) {
                getLogger(BcLoggerFactory.class).warn("setProcessType called too late");
            }
            System.setProperty("bc.processType", processType);
        }
    }

    private static String getProcessType() {
        return System.getProperty("bc.processType");
    }

    public static BcLogger getLogger(String name) {
        ensureInitialized();
        synchronized (logMap) {
            BcLogger logger = logMap.get(name);
            if (logger != null) {
                return logger;
            }
            logger = new BcLogger(LoggerFactory.getLogger(name));
            logMap.put(name, logger);
            return logger;
        }
    }
}
