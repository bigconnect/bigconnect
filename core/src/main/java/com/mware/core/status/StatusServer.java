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
package com.mware.core.status;

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.github.jknack.handlebars.io.StringTemplateSource;
import com.mware.core.model.clientapi.util.IOUtils;
import com.mware.core.status.model.ProcessStatus;
import com.mware.core.status.model.Status;
import com.mware.core.util.JSONUtil;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONObject;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.util.ClientApiConverter;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.*;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public abstract class StatusServer {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(StatusServer.class);
    private final Configuration configuration;
    private final HttpServer httpServer;
    private final Date startTime;
    private final String type;
    private final Class sourceClass;
    private final StatusRepository.StatusHandle statusHandle;
    private final StatusRepository statusRepository;
    private final Template hbsTemplate;

    public StatusServer(Configuration configuration, StatusRepository statusRepository, String type, Class sourceClass) {
        this.statusRepository = statusRepository;
        this.sourceClass = sourceClass;
        this.type = type;
        this.configuration = configuration;
        this.startTime = new Date();

        String portRange = configuration.get(Configuration.STATUS_PORT_RANGE, Configuration.DEFAULT_STATUS_PORT_RANGE);
        httpServer = startHttpServer(portRange);

        String url = getUrl();
        LOGGER.debug("Using url: " + url);
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            String hostAddress = InetAddress.getLocalHost().getHostAddress();
            StatusData statusData = new StatusData(url, hostname, hostAddress);
            this.statusHandle = statusRepository.saveStatus(type, UUID.randomUUID().toString(), statusData);
        } catch (UnknownHostException e) {
            throw new BcException("Could not get local host address", e);
        }

        try {
            Handlebars handlebars = new Handlebars();
            ByteArrayOutputStream templateStream = new ByteArrayOutputStream();
            IOUtils.copy(StatusServer.class.getResourceAsStream("status.hbs"), templateStream);
            hbsTemplate = handlebars.compileInline(templateStream.toString("UTF-8"));
        } catch (Exception ex) {
            throw new BcException("Could not load StatusServer hbs template");
        }
    }

    private String getUrl() {
        try {
            String hostname = InetAddress.getLocalHost().getHostAddress();
            return String.format("http://%s:%d/", hostname, httpServer.getAddress().getPort());
        } catch (UnknownHostException ex) {
            throw new BcException("Could not create url", ex);
        }
    }

    private HttpServer startHttpServer(String portRange) {
        String[] parts = portRange.split("-");
        if (parts.length != 2) {
            throw new BcException("Invalid port range: " + portRange);
        }
        int startPort = Integer.parseInt(parts[0]);
        int endPort = Integer.parseInt(parts[1]);
        return startHttpServer(startPort, endPort);
    }

    private HttpServer startHttpServer(int startPort, int endPort) {
        for (int i = startPort; i < endPort; i++) {
            try {
                HttpServer httpServer = HttpServer.create(new InetSocketAddress(i), 0);
                httpServer.createContext("/", new StatusHandler());
                httpServer.setExecutor(null);
                httpServer.start();
                LOGGER.info("Started status HTTP server on port: %s", i);
                return httpServer;
            } catch (BindException ex) {
                LOGGER.debug("Could not start HTTP server on port %d", i);
            } catch (Throwable ex) {
                LOGGER.debug("Could not start HTTP server on port %d", i, ex);
            }
        }
        throw new BcException("Could not start HTTP status server");
    }

    public void shutdown() {
        try {
            httpServer.stop(0);
        } catch (Throwable ex) {
            LOGGER.error("Could not stop http server", ex);
        }

        statusRepository.deleteStatus(this.statusHandle);
    }

    private class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            try {
                Status status = getStatus();
                String responseString = hbsTemplate.apply(status);
                Headers responseHeaders = t.getResponseHeaders();
                responseHeaders.set("Content-Type", "text/html");
                t.sendResponseHeaders(200, 0);

                OutputStream os = t.getResponseBody();
                os.write(responseString.getBytes());
                os.close();
            } catch (Throwable ex) {
                LOGGER.error("Could not process request", ex);
            }
        }
    }

    private Status getStatus() {
        ProcessStatus status = createStatus();
        getGeneralInfo(status, this.sourceClass);
        status.setType(type);
        status.setStartTime(startTime);
        try {
            status.setHostname(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            LOGGER.error("Could not get hostname");
        }
        status.setOsUser(System.getProperty("user.name"));
        for (Map.Entry<String, String> envEntry : System.getenv().entrySet()) {
            status.getEnv().put(envEntry.getKey(), envEntry.getValue());
        }
        status.setConfiguration(ClientApiConverter.toClientApiValue(getConfigurationJson()));
        status.getJvm().setClasspath(System.getProperty("java.class.path"));
        return status;
    }

    protected abstract ProcessStatus createStatus();

    private JSONObject getConfigurationJson() {
        JSONObject json = new JSONObject();
        json.put("properties", configuration.getJsonProperties());
        json.put("configurationInfo", configuration.getConfigurationInfo());
        return json;
    }

    private static Manifest getManifest(Class clazz) {
        try {
            String className = clazz.getSimpleName() + ".class";
            URL resource = clazz.getResource(className);
            if (resource == null) {
                LOGGER.error("Could not get class manifest: " + clazz.getName() + ", could not find resource: " + className);
                return null;
            }
            String classPath = resource.toString();
            if (!classPath.startsWith("jar")) {
                return null; // Class not from JAR
            }
            String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF";
            return new Manifest(new URL(manifestPath).openStream());
        } catch (Exception ex) {
            LOGGER.error("Could not get class manifest: " + clazz.getName(), ex);
            return null;
        }
    }

    public static void getGeneralInfo(JSONObject json, Class clazz) {
        json.put("className", clazz.getName());

        Name nameAnnotation = (Name) clazz.getAnnotation(Name.class);
        if (nameAnnotation != null) {
            json.put("name", nameAnnotation.value());
        }

        Description descriptionAnnotation = (Description) clazz.getAnnotation(Description.class);
        if (descriptionAnnotation != null) {
            json.put("description", descriptionAnnotation.value());
        }

        Manifest manifest = getManifest(clazz);
        if (manifest != null) {
            Attributes mainAttributes = manifest.getMainAttributes();
            json.put("projectVersion", mainAttributes.getValue("Project-Version"));
            json.put("gitRevision", mainAttributes.getValue("Git-Revision"));
            json.put("builtBy", mainAttributes.getValue("Built-By"));
            String value = mainAttributes.getValue("Built-On-Unix");
            if (value != null) {
                json.put("builtOn", Long.parseLong(value));
            }
        }
    }

    public static void getGeneralInfo(Status generalStatus, Class clazz) {
        generalStatus.setClassName(clazz.getName());

        Name nameAnnotation = (Name) clazz.getAnnotation(Name.class);
        if (nameAnnotation != null) {
            generalStatus.setName(nameAnnotation.value());
        }

        Description descriptionAnnotation = (Description) clazz.getAnnotation(Description.class);
        if (descriptionAnnotation != null) {
            generalStatus.setDescription(descriptionAnnotation.value());
        }

        Manifest manifest = getManifest(clazz);
        if (manifest != null) {
            Attributes mainAttributes = manifest.getMainAttributes();
            generalStatus.setProjectVersion(mainAttributes.getValue("Project-Version"));
            generalStatus.setGitRevision(mainAttributes.getValue("Git-Revision"));
            generalStatus.setBuiltBy(mainAttributes.getValue("Built-By"));
            String value = mainAttributes.getValue("Built-On-Unix");
            if (value != null) {
                generalStatus.setBuiltOn(new Date(Long.parseLong(value)));
            }
        }
    }
}
