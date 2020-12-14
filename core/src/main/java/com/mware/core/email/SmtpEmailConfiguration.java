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
package com.mware.core.email;

import com.mware.core.config.Configurable;
import com.mware.core.config.Configuration;
import com.mware.core.config.PostConfigurationValidator;

public class SmtpEmailConfiguration {
    public static final String CONFIGURATION_PREFIX = Configuration.EMAIL_REPOSITORY + ".smtp";

    private String serverHostname;
    private int serverPort;
    private String serverUsername;
    private String serverPassword;
    private ServerAuthentication serverAuthentication;

    @Configurable(name = "serverHostname")
    public void setServerHostname(String serverHostname) {
        this.serverHostname = serverHostname;
    }

    @Configurable(name = "serverPort")
    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    @Configurable(name = "serverUsername", required = false)
    public void setServerUsername(String serverUsername) {
        this.serverUsername = serverUsername;
    }

    @Configurable(name = "serverPassword", required = false)
    public void setServerPassword(String serverPassword) {
        this.serverPassword = serverPassword;
    }

    @Configurable(name = "serverAuthentication", defaultValue = "NONE")
    public void setServerAuthentication(String serverAuthentication) {
        this.serverAuthentication = ServerAuthentication.valueOf(serverAuthentication);
    }

    @PostConfigurationValidator(description = "serverUsername and serverPassword settings are required for the configured serverAuthentication value")
    public boolean validateAuthentication() {
        return ServerAuthentication.NONE.equals(serverAuthentication) || (isNotNullOrBlank(serverUsername) && isNotNullOrBlank(serverPassword));
    }

    public String getServerHostname() {
        return serverHostname;
    }

    public int getServerPort() {
        return serverPort;
    }

    public String getServerPassword() {
        return serverPassword;
    }

    public String getServerUsername() {
        return serverUsername;
    }

    public ServerAuthentication getServerAuthentication() {
        return serverAuthentication;
    }

    private boolean isNotNullOrBlank(String s) {
        return s != null && s.trim().length() > 0;
    }

    public enum ServerAuthentication {
        NONE,
        TLS,
        SSL
    }
}
