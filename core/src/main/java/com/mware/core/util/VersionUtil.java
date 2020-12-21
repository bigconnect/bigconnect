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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public class VersionUtil {
    public static void printVersion() {
        try {
            List<VersionData> versionDatas = new ArrayList<>();

            Enumeration<URL> resEnum = Thread.currentThread().getContextClassLoader().getResources(JarFile.MANIFEST_NAME);
            while (resEnum.hasMoreElements()) {
                URL url = resEnum.nextElement();
                try {
                    InputStream is = url.openStream();
                    if (is != null) {
                        Manifest manifest = new Manifest(is);
                        Attributes mainAttributes = manifest.getMainAttributes();
                        String builtOnUnix = mainAttributes.getValue("Built-On-Unix");
                        if (builtOnUnix != null) {
                            try {
                                Date buildOnDate = new Date(Long.parseLong(builtOnUnix));
                                String path = url.toString();
                                path = path.replace("!/META-INF/MANIFEST.MF", "");
                                path = path.replace("/META-INF/MANIFEST.MF", "");
                                path = path.replace("jar:", "");
                                path = path.replace("file:", "");
                                versionDatas.add(new VersionData(path, buildOnDate));
                            } catch (Exception ex) {
                                System.out.println("Could not parse Built-On-Unix: " + builtOnUnix + ": " + ex.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Could not find version information in: " + url);
                }
            }
            if (versionDatas.size() == 0) {
                System.err.println("Could not find version information");
            } else {
                int maxPathWidth = 0;
                for (VersionData versionData : versionDatas) {
                    maxPathWidth = Math.max(maxPathWidth, versionData.getPath().length());
                }
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                for (VersionData versionData : versionDatas) {
                    String buildOnDateString = sdf.format(versionData.getBuildOnDate());
                    System.out.println(String.format("%-" + maxPathWidth + "s: %s", versionData.getPath(), buildOnDateString));
                }
            }
        } catch (IOException ex) {
            System.err.println("could not get version information: " + ex.getMessage());
        }
    }

    private static class VersionData {
        private final String path;
        private final Date buildOnDate;

        public VersionData(String path, Date buildOnDate) {
            this.path = path;
            this.buildOnDate = buildOnDate;
        }

        public String getPath() {
            return path;
        }

        public Date getBuildOnDate() {
            return buildOnDate;
        }
    }
}
