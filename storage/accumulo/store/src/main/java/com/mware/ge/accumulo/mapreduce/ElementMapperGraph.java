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
package com.mware.ge.accumulo.mapreduce;

import com.mware.ge.*;
import com.mware.ge.accumulo.AccumuloGraphConfiguration;
import com.mware.ge.accumulo.util.GeTabletServerBatchReader;
import com.mware.ge.id.IdGenerator;
import com.mware.ge.metric.NullMetricRegistry;
import com.mware.ge.query.GraphQuery;
import com.mware.ge.query.MultiVertexQuery;
import com.mware.ge.query.builder.GeQueryBuilder;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;

import java.util.Collection;

public class ElementMapperGraph extends GraphBase {
    private static final boolean STRICT_TYPING = false;
    private ElementMapper elementMapper;
    private AccumuloGraphConfiguration config;

    public ElementMapperGraph(ElementMapper elementMapper, AccumuloGraphConfiguration config) {
        super(STRICT_TYPING, new NullMetricRegistry());
        this.elementMapper = elementMapper;
        this.config = config;
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility, String conceptType) {
        return this.elementMapper.prepareVertex(vertexId, timestamp, visibility, conceptType);
    }

    @Override
    public Iterable<String> getVertexIds(Authorizations authorizations) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Vertex> getVertices(FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public void deleteVertex(Vertex vertex, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public void softDeleteEdge(Edge edge, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility) {
        return this.elementMapper.prepareEdge(edgeId, outVertex, inVertex, label, timestamp, visibility);
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility) {
        return this.elementMapper.prepareEdge(edgeId, outVertexId, inVertexId, label, timestamp, visibility);
    }

    @Override
    public Iterable<Edge> getEdges(FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public void deleteEdge(Edge edge, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public void deleteExtendedDataRow(ExtendedDataRowId id, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    protected GraphMetadataStore getGraphMetadataStore() {
        throw new GeException("Not supported");
    }

    @Override
    public GraphQuery query(Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public GraphQuery query(String queryString, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public MultiVertexQuery query(GeQueryBuilder queryBuilder, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public void reindex(Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public void flush() {
        throw new GeException("Not supported");
    }

    @Override
    public void shutdown() {
        throw new GeException("Not supported");
    }

    @Override
    public IdGenerator getIdGenerator() {
        return this.elementMapper.getIdGenerator();
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public DefinePropertyBuilder defineProperty(String propertyName) {
        throw new GeException("Not supported");
    }

    @Override
    public boolean isPropertyDefined(String propertyName) {
        throw new GeException("Not supported");
    }

    @Override
    public boolean isFieldBoostSupported() {
        throw new GeException("Not supported");
    }

    @Override
    public void truncate() {
        throw new GeException("Not supported");
    }

    @Override
    public void drop() {
        throw new GeException("Not supported");
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        throw new GeException("Not supported");
    }

    @Override
    public void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations) {
        throw new GeException("Not supported");
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        return new Authorizations(auths);
    }

    @Override
    public FetchHints getDefaultFetchHints() {
        throw new GeException("Not supported");
    }

    public AccumuloGraphConfiguration getConfig() {
        return config;
    }

    private Scanner createScanner(
            String tableName,
            org.apache.accumulo.core.data.Range range,
            org.apache.accumulo.core.security.Authorizations accumuloAuthorizations
    ) throws TableNotFoundException {
        Connector connector = config.createConnector();
        Scanner scanner = connector.createScanner(tableName, accumuloAuthorizations);
        if (range != null) {
            scanner.setRange(range);
        }
        return scanner;
    }

    public GeTabletServerBatchReader createBatchScanner(String tableName, Collection<Range> ranges, org.apache.accumulo.core.security.Authorizations accumuloAuthorizations) throws TableNotFoundException {
        Connector connector = config.createConnector();
        GeTabletServerBatchReader scanner = new GeTabletServerBatchReader(
                connector,
                tableName,
                accumuloAuthorizations,
                config.getNumberOfQueryThreads()
        );
        scanner.setRanges(ranges);
        return scanner;
    }
}
