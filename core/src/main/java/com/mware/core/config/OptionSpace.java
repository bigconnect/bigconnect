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

import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class OptionSpace {
    private static final GeLogger LOG = GeLoggerFactory.getLogger(OptionSpace.class);

    private static final Map<String, Class<? extends OptionHolder>> HOLDERS;
    private static final Map<String, TypedOption<?, ?>> OPTIONS;
    private static final String INSTANCE_METHOD = "instance";

    static {
        HOLDERS = new ConcurrentHashMap<>();
        OPTIONS = new ConcurrentHashMap<>();
    }

    public static void register(String module, String holder) {
        ClassLoader classLoader = OptionSpace.class.getClassLoader();
        Class<?> clazz;
        try {
            clazz = classLoader.loadClass(holder);
        } catch (ClassNotFoundException e) {
            throw new ConfigException(
                    "Failed to load class of option holder '%s'", e, holder);
        }

        // Check subclass
        if (!OptionHolder.class.isAssignableFrom(clazz)) {
            throw new ConfigException(
                    "Class '%s' is not a subclass of OptionHolder", holder);
        }

        OptionHolder instance = null;
        Exception exception = null;
        try {
            Method method = clazz.getMethod(INSTANCE_METHOD);
            if (!Modifier.isStatic(method.getModifiers())) {
                throw new NoSuchMethodException(INSTANCE_METHOD);
            }
            instance = (OptionHolder) method.invoke(null);
            if (instance == null) {
                exception = new ConfigException(
                        "Returned null from %s() method",
                        INSTANCE_METHOD);
            }
        } catch (NoSuchMethodException e) {
            LOG.warn("Class {} does not has static method {}.",
                    holder, INSTANCE_METHOD);
            exception = e;
        } catch (InvocationTargetException e) {
            LOG.warn("Can't call static method {} from class {}.",
                    INSTANCE_METHOD, holder);
            exception = e;
        } catch (IllegalAccessException e) {
            LOG.warn("Illegal access while calling method {} from class {}.",
                    INSTANCE_METHOD, holder);
            exception = e;
        }

        if (exception != null) {
            throw new ConfigException("Failed to instantiate option holder: %s",
                    exception, holder);
        }

        register(module, instance);
    }

    public static void register(String module, OptionHolder holder) {
        // Check exists
        if (HOLDERS.containsKey(module)) {
            LOG.warn("Already registered option holder: %s (%s)", module, HOLDERS.get(module));
        }
        Preconditions.checkArgument(holder != null, "OptionHolder can't be null");
        HOLDERS.put(module, holder.getClass());
        OPTIONS.putAll(holder.options());
        LOG.debug("Registered options for OptionHolder: {}",
                holder.getClass().getSimpleName());
    }

    public static Set<String> keys() {
        return Collections.unmodifiableSet(OPTIONS.keySet());
    }

    public static boolean containKey(String key) {
        return OPTIONS.containsKey(key);
    }

    public static TypedOption<?, ?> get(String key) {
        return OPTIONS.get(key);
    }
}
