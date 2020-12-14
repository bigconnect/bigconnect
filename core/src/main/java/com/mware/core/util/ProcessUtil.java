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

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;

public class ProcessUtil {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ProcessUtil.class);

    public static String getPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        int i = name.indexOf('@');
        if (i > 0) {
            name = name.substring(0, i);
        }
        return name;
    }

    public static boolean isWindows() {
        String os = System.getProperty("os.name");
        if (os == null) {
            return false;
        }
        if (os.toLowerCase().startsWith("windows")) {
            return true;
        }
        return false;
    }

    public static int getPid(Process p) {
        int pid = 0;
        if (p.getClass().getName().equals("java.lang.UNIXProcess") || p.getClass().getName().equals("java.lang.ProcessImpl")) {
            try {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                pid = f.getInt(p);
            } catch (Throwable e) {
                LOGGER.error("Failure trying to get pid from " + p, e);
            }
        }
        return pid;
    }
}
