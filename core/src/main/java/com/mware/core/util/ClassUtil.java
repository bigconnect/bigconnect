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

import com.mware.core.exception.BcException;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class ClassUtil {
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    @SuppressWarnings("unchecked")
    public static <T> Class<? extends T> forName(String className) {
        try {
            return (Class<? extends T>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new BcException("Could not load class " + className, e);
        }
    }

    public static Method findMethod(Class clazz, String methodName, Class[] parameters) {
        while (clazz != null) {
            try {
                return getDeclaredMethod(clazz, methodName, parameters);
            } catch (NoSuchMethodException e) {
                clazz = clazz.getSuperclass();
            }
        }
        return null;
    }

    public static void logClasspath(ClassLoader classLoader) {
        logClasspath(classLoader, System.out);
    }

    public static void logClasspath(ClassLoader classLoader, PrintStream printStream) {
        outputClasspath(classLoader, printStream);
    }

    public static void logClasspath(ClassLoader classLoader, BcLogger logger) {
        outputClasspath(classLoader, logger);
    }

    private static void outputClasspath(ClassLoader classLoader, Object outputObject) {
        StringBuilder sb = new StringBuilder();

        if (classLoader instanceof URLClassLoader) {
            sb.append(classLoader.getClass().getName());
            for (URL url : ((URLClassLoader) classLoader).getURLs()) {
                sb.append(LINE_SEPARATOR).append(url.toString());
            }
        } else {
            sb.append("unable to enumerate entries for ").append(classLoader.getClass().getName());
        }

        if (outputObject instanceof PrintStream) {
            ((PrintStream) outputObject).println(sb.toString());
        } else if (outputObject instanceof BcLogger) {
            ((BcLogger) outputObject).debug(sb.toString());
        } else {
            throw new BcException("unexpected outputObject");
        }

        ClassLoader parentClassLoader = classLoader.getParent();
        if (parentClassLoader != null) {
            outputClasspath(parentClassLoader, outputObject);
        }
    }

    @SuppressWarnings("unchecked")
    private static Method getDeclaredMethod(Class clazz, String methodName, Class[] parameters) throws NoSuchMethodException {
        return clazz.getDeclaredMethod(methodName, parameters);
    }
}
