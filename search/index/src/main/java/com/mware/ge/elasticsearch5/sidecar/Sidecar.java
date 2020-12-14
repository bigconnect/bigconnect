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
package com.mware.ge.elasticsearch5.sidecar;

import com.mware.ge.elasticsearch5.ElasticsearchSearchIndexConfiguration;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.LogConfigurator;

import java.io.IOException;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

public class Sidecar {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(Sidecar.class);

    private ElasticsearchSearchIndexConfiguration configuration;
    private ElasticsearchClusterRunner runner;
    private Client client;

    public Sidecar(ElasticsearchSearchIndexConfiguration configuration) {
        this.configuration = configuration;

        Preconditions.checkState(configuration.sidecarEnabled());
        Preconditions.checkState(!StringUtils.isEmpty(configuration.getSidecarPath()));

        LogConfigurator.registerErrorListener();

        runner = new ElasticsearchClusterRunner();
        runner.onBuild((i, builder) ->
                builder.put("cluster.name", configuration.getClusterName())
                        .put("http.type", "netty4")
                        .put("transport.type", "netty4")
        ).build(newConfigs()
                .basePath(configuration.getSidecarPath())
                .numOfNode(1)
        );

        this.client = runner.getNode(0).client();
    }

    public Client getClient() {
        return client;
    }

    public void stop() {
        try {
            runner.close();
        } catch (IOException e) {
            LOGGER.warn(e.getMessage());
        }
    }
}
