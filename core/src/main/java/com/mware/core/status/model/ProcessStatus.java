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
package com.mware.core.status.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ProcessStatus extends Status {
    private String type;
    private Date startTime;
    private String hostname;
    private String osUser;
    private Map<String, String> env = new HashMap<>();
    private Jvm jvm = new Jvm();
    private Object configuration;

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getHostname() {
        return hostname;
    }

    public void setOsUser(String osUser) {
        this.osUser = osUser;
    }

    public String getOsUser() {
        return osUser;
    }

    public Map<String, String> getEnv() {
        return env;
    }

    public Jvm getJvm() {
        return jvm;
    }

    public void setConfiguration(Object configuration) {
        this.configuration = configuration;
    }

    public Object getConfiguration() {
        return configuration;
    }

    public static class Jvm {

        private String classpath;

        private String heapUsage;

        public void setClasspath(String classpath) {
            this.classpath = classpath;
        }

        public String getClasspath() {
            return classpath;
        }

        public String getHeapUsage() {
            return heapUsage;
        }

        public void setHeapUsage(String heapUsage) {
            this.heapUsage = heapUsage;
        }
    }
}
