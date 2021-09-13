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
package com.mware.ge;

import com.mware.core.config.options.GraphOptions;
import com.mware.ge.id.IdGenerator;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.query.Query;
import com.mware.ge.query.builder.GeQueryBuilder;
import com.mware.ge.search.IndexHint;
import com.mware.ge.search.SearchIndex;
import com.mware.ge.search.SearchIndexWithVertexPropertyCountByValue;
import com.mware.ge.tools.GraphBackup;
import com.mware.ge.tools.GraphRestore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class GraphBaseWithSearchIndex extends GraphBase implements Graph, GraphWithSearchIndex {
    public static final String METADATA_ID_GENERATOR_CLASSNAME = "idGenerator.classname";
    private final GraphConfiguration configuration;
    private final IdGenerator idGenerator;
    private final FetchHints defaultFetchHints;
    private SearchIndex searchIndex;
    private boolean foundIdGeneratorClassnameInMetadata;

    protected GraphBaseWithSearchIndex(GraphConfiguration configuration) {
        super(configuration.isStrictTyping(), configuration.createMetricsRegistry());
        this.configuration = configuration;
        this.searchIndex = configuration.createSearchIndex(this);
        this.idGenerator = configuration.createIdGenerator(this);
        this.defaultFetchHints = FetchHints.ALL;
    }

    protected GraphBaseWithSearchIndex(GraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        super(configuration.isStrictTyping(), configuration.createMetricsRegistry());
        this.configuration = configuration;
        this.searchIndex = searchIndex;
        this.idGenerator = idGenerator;
        this.defaultFetchHints = FetchHints.ALL;
    }

    protected void setup() {
        setupGraphMetadata();
    }

    protected void setupGraphMetadata() {
        foundIdGeneratorClassnameInMetadata = false;
        for (GraphMetadataEntry graphMetadataEntry : getMetadata()) {
            setupGraphMetadata(graphMetadataEntry);
        }
        if (!foundIdGeneratorClassnameInMetadata) {
            setMetadata(METADATA_ID_GENERATOR_CLASSNAME, this.idGenerator.getClass().getName());
        }
    }

    protected void setupGraphMetadata(GraphMetadataEntry graphMetadataEntry) {
        if (graphMetadataEntry.getKey().startsWith(METADATA_DEFINE_PROPERTY_PREFIX)) {
            Object value = graphMetadataEntry.getValue();
            if (value == null) {
                return;
            } else if (value instanceof PropertyDefinition) {
                addToPropertyDefinitionCache((PropertyDefinition) graphMetadataEntry.getValue());
            } else {
                throw new GeException("Invalid property definition metadata: " + graphMetadataEntry.getKey() + " expected " + PropertyDefinition.class.getName() + " found " + graphMetadataEntry.getValue().getClass().getName());
            }
        } else if (graphMetadataEntry.getKey().equals(METADATA_ID_GENERATOR_CLASSNAME)) {
            if (graphMetadataEntry.getValue() instanceof String) {
                String idGeneratorClassname = (String) graphMetadataEntry.getValue();
                if (idGeneratorClassname.equals(idGenerator.getClass().getName())) {
                    foundIdGeneratorClassnameInMetadata = true;
                }
            } else {
                throw new GeException("Invalid " + METADATA_ID_GENERATOR_CLASSNAME + " expected String found " + graphMetadataEntry.getValue().getClass().getName());
            }
        }
    }

    @Override
    public Query query(GeQueryBuilder queryBuilder, Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, queryBuilder, authorizations);
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public GraphConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public SearchIndex getSearchIndex() {
        return searchIndex;
    }

    @Override
    public void reindex(Authorizations authorizations) {
        reindexVertices(authorizations);
        reindexEdges(authorizations);
    }

    protected void reindexVertices(Authorizations authorizations) {
        this.searchIndex.addElements(this, getVertices(authorizations), authorizations);
    }

    private void reindexEdges(Authorizations authorizations) {
        this.searchIndex.addElements(this, getEdges(authorizations), authorizations);
    }

    @Override
    public void flush() {
        flushStackTraceTracker.addStackTrace();
        if (getSearchIndex() != null) {
            this.searchIndex.flush(this);
        }
    }

    @Override
    public void shutdown() {
        flush();
        if (getSearchIndex() != null) {
            this.searchIndex.shutdown();
            this.searchIndex = null;
        }
        getMetricsRegistry().shutdown();
    }

    @Override
    public abstract void drop();

    @Override
    public boolean isFieldBoostSupported() {
        return getSearchIndex().isFieldBoostSupported();
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return getSearchIndex().getSearchIndexSecurityGranularity();
    }

    @Override
    public Map<Object, Long> getVertexPropertyCountByValue(String propertyName, Authorizations authorizations) {
        if (getSearchIndex() instanceof SearchIndexWithVertexPropertyCountByValue) {
            return ((SearchIndexWithVertexPropertyCountByValue) getSearchIndex()).getVertexPropertyCountByValue(this, propertyName, authorizations);
        }
        return super.getVertexPropertyCountByValue(propertyName, authorizations);
    }

    @Override
    public Iterable<Element> saveElementMutations(
            Iterable<ElementMutation<? extends Element>> mutations,
            Authorizations authorizations
    ) {
        List<Element> elements = new ArrayList<>();

        for (ElementMutation<? extends Element> m : orderMutations(mutations)) {
            if (m instanceof ExistingElementMutation && !m.hasChanges()) {
                elements.add(((ExistingElementMutation) m).getElement());
                continue;
            }

            Element element = m.save(authorizations);
            elements.add(element);
        }

        for (ElementMutation<? extends Element> m : mutations) {
            if (m.getIndexHint() == IndexHint.INDEX) {
                getSearchIndex().addElementExtendedData(
                        this,
                        m,
                        m.getExtendedData(),
                        authorizations
                );
            }
        }
        return elements;
    }

    @Override
    public abstract VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility, String conceptType);

    @Override
    public abstract Iterable<Vertex> getVertices(FetchHints fetchHints, Long endTime, Authorizations authorizations);

    @Override
    public abstract EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility);

    @Override
    public VertexBuilder prepareVertex(Long timestamp, Visibility visibility, String conceptType) {
        return super.prepareVertex(timestamp, visibility, conceptType);
    }

    @Override
    public abstract EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility);

    @Override
    public abstract void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations);

    @Override
    public abstract void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations);

    @Override
    public abstract Iterable<Edge> getEdges(FetchHints fetchHints, Long endTime, Authorizations authorizations);

    @Override
    protected abstract GraphMetadataStore getGraphMetadataStore();

    @Override
    public abstract void deleteVertex(Vertex vertex, Authorizations authorizations);

    @Override
    public abstract void deleteEdge(Edge edge, Authorizations authorizations);

    @Override
    public abstract boolean isVisibilityValid(Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void truncate();

    @Override
    public abstract void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract Authorizations createAuthorizations(String... auths);

    @Override
    public FetchHints getDefaultFetchHints() {
        return defaultFetchHints;
    }

    public String getBackupDir() {
        return this.configuration.get(GraphOptions.BACKUP_DIR);
    }

    public GraphBackup getBackupTool(String outputFile) {
        return new GraphBackup(getBackupDir(), outputFile);
    }

    public GraphRestore getRestoreTool() {
        return new GraphRestore(getBackupDir());
    }

    @Override
    public boolean isQuerySimilarToTextSupported() {
        return getSearchIndex().isQuerySimilarToTextSupported();
    }
}
