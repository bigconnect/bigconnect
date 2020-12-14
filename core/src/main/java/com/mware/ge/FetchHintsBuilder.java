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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FetchHintsBuilder {
    private boolean includeAllProperties;
    private Set<String> propertyNamesToInclude;
    private boolean includeAllPropertyMetadata;
    private Set<String> metadataKeysToInclude;
    private boolean includeHidden;
    private boolean includeAllEdgeRefs;
    private boolean includeOutEdgeRefs;
    private boolean includeInEdgeRefs;
    private boolean includeEdgeIds = true;
    private boolean includeEdgeVertexIds = true;
    private Set<String> edgeLabelsOfEdgeRefsToInclude;
    private boolean includeEdgeLabelsAndCounts;
    private boolean includeExtendedDataTableNames;

    public FetchHintsBuilder() {

    }

    public FetchHintsBuilder(FetchHints fetchHints) {
        includeAllProperties = fetchHints.isIncludeAllProperties();
        propertyNamesToInclude = fetchHints.getPropertyNamesToInclude();
        includeAllPropertyMetadata = fetchHints.isIncludeAllPropertyMetadata();
        metadataKeysToInclude = fetchHints.getMetadataKeysToInclude();
        includeHidden = fetchHints.isIncludeHidden();
        includeAllEdgeRefs = fetchHints.isIncludeAllEdgeRefs();
        includeOutEdgeRefs = fetchHints.isIncludeOutEdgeRefs();
        includeInEdgeRefs = fetchHints.isIncludeInEdgeRefs();
        includeEdgeIds = fetchHints.isIncludeEdgeIds();
        includeEdgeVertexIds = fetchHints.isIncludeEdgeVertexIds();
        edgeLabelsOfEdgeRefsToInclude = fetchHints.getEdgeLabelsOfEdgeRefsToInclude();
        includeEdgeLabelsAndCounts = fetchHints.isIncludeEdgeLabelsAndCounts();
        includeExtendedDataTableNames = fetchHints.isIncludeExtendedDataTableNames();
    }

    public FetchHints build() {
        if (!isIncludeProperties() && isIncludePropertyMetadata()) {
            includeAllProperties = true;
        }
        if ((includeAllEdgeRefs || includeOutEdgeRefs || includeInEdgeRefs) && !includeEdgeIds && !includeEdgeVertexIds) {
            throw new GeException("Cannot exclude both includeEdgeIds and includeEdgeVertexIds. Instead exclude edge refs.");
        }
        return new FetchHints(
                includeAllProperties,
                propertyNamesToInclude == null ? null : ImmutableSet.copyOf(propertyNamesToInclude),
                includeAllPropertyMetadata,
                metadataKeysToInclude == null ? null : ImmutableSet.copyOf(metadataKeysToInclude),
                includeHidden,
                includeAllEdgeRefs,
                includeOutEdgeRefs || includeAllEdgeRefs,
                includeInEdgeRefs || includeAllEdgeRefs,
                includeEdgeIds,
                includeEdgeVertexIds,
                edgeLabelsOfEdgeRefsToInclude == null ? null : ImmutableSet.copyOf(edgeLabelsOfEdgeRefsToInclude),
                includeEdgeLabelsAndCounts,
                includeExtendedDataTableNames
        );
    }

    public FetchHintsBuilder setIncludeEdgeIds(boolean includeEdgeIds) {
        this.includeEdgeIds = includeEdgeIds;
        return this;
    }

    public FetchHintsBuilder setIncludeEdgeVertexIds(boolean includeEdgeVertexIds) {
        this.includeEdgeVertexIds = includeEdgeVertexIds;
        return this;
    }

    public static FetchHintsBuilder parse(JSONObject fetchHintsJson) {
        if (fetchHintsJson == null) {
            fetchHintsJson = new JSONObject();
        }

        return new FetchHintsBuilder()
                .setIncludeAllProperties(fetchHintsJson.optBoolean("includeAllProperties", false))
                .setIncludeAllPropertyMetadata(fetchHintsJson.optBoolean("includeAllPropertyMetadata", false))
                .setIncludeHidden(fetchHintsJson.optBoolean("includeHidden", false))
                .setIncludeAllEdgeRefs(fetchHintsJson.optBoolean("includeAllEdgeRefs", false))
                .setIncludeOutEdgeRefs(fetchHintsJson.optBoolean("includeOutEdgeRefs", false))
                .setIncludeInEdgeRefs(fetchHintsJson.optBoolean("includeInEdgeRefs", false))
                .setIncludeEdgeLabelsAndCounts(fetchHintsJson.optBoolean("includeEdgeLabelsAndCounts", false))
                .setIncludeExtendedDataTableNames(fetchHintsJson.optBoolean("includeExtendedDataTableNames", false))
                .setPropertyNamesToInclude(fetchHintsJson.has("propertyNamesToInclude")
                        ? jsonArrayToSet(fetchHintsJson.getJSONArray("propertyNamesToInclude"))
                        : null)
                .setMetadataKeysToInclude(fetchHintsJson.has("metadataKeysToInclude")
                        ? jsonArrayToSet(fetchHintsJson.getJSONArray("metadataKeysToInclude"))
                        : null)
                .setEdgeLabelsOfEdgeRefsToInclude(fetchHintsJson.has("edgeLabelsOfEdgeRefsToInclude")
                        ? jsonArrayToSet(fetchHintsJson.getJSONArray("edgeLabelsOfEdgeRefsToInclude"))
                        : null);
    }

    private static Set<String> jsonArrayToSet(JSONArray jsonArray) {
        if (jsonArray == null) {
            return null;
        }

        Set<String> set = new HashSet<>();
        for (int i = 0;i < jsonArray.length(); i++) {
            set.add((String) jsonArray.get(i));
        }

        return set;
    }

    private boolean isIncludeProperties() {
        return includeAllProperties || (propertyNamesToInclude != null && propertyNamesToInclude.size() > 0);
    }

    private boolean isIncludePropertyMetadata() {
        return includeAllPropertyMetadata || (metadataKeysToInclude != null && metadataKeysToInclude.size() > 0);
    }

    public FetchHintsBuilder setIncludeAllProperties(boolean includeAllProperties) {
        this.includeAllProperties = includeAllProperties;
        return this;
    }

    public FetchHintsBuilder setPropertyNamesToInclude(Set<String> propertyNamesToInclude) {
        this.propertyNamesToInclude = propertyNamesToInclude;
        return this;
    }

    public FetchHintsBuilder setPropertyNamesToInclude(String... propertyNamesToInclude) {
        this.propertyNamesToInclude = Sets.newHashSet(propertyNamesToInclude);
        return this;
    }

    public FetchHintsBuilder setIncludeAllPropertyMetadata(boolean includeAllPropertyMetadata) {
        this.includeAllPropertyMetadata = includeAllPropertyMetadata;
        return this;
    }

    public FetchHintsBuilder setMetadataKeysToInclude(Set<String> metadataKeysToInclude) {
        this.metadataKeysToInclude = metadataKeysToInclude;
        return this;
    }

    public FetchHintsBuilder setMetadataKeysToInclude(String... metadataKeysToInclude) {
        this.metadataKeysToInclude = Sets.newHashSet(metadataKeysToInclude);
        return this;
    }

    public FetchHintsBuilder setIncludeHidden(boolean includeHidden) {
        this.includeHidden = includeHidden;
        return this;
    }

    public FetchHintsBuilder setIncludeAllEdgeRefs(boolean includeAllEdgeRefs) {
        this.includeAllEdgeRefs = includeAllEdgeRefs;
        return this;
    }

    public FetchHintsBuilder setIncludeOutEdgeRefs(boolean includeOutEdgeRefs) {
        this.includeOutEdgeRefs = includeOutEdgeRefs;
        return this;
    }

    public FetchHintsBuilder setIncludeInEdgeRefs(boolean includeInEdgeRefs) {
        this.includeInEdgeRefs = includeInEdgeRefs;
        return this;
    }

    public FetchHintsBuilder setEdgeLabelsOfEdgeRefsToInclude(Set<String> edgeLabelsOfEdgeRefsToInclude) {
        this.edgeLabelsOfEdgeRefsToInclude = edgeLabelsOfEdgeRefsToInclude;
        return this;
    }

    public FetchHintsBuilder setEdgeLabelsOfEdgeRefsToInclude(String... edgeLabelsOfEdgeRefsToInclude) {
        this.edgeLabelsOfEdgeRefsToInclude = Sets.newHashSet(edgeLabelsOfEdgeRefsToInclude);
        return this;
    }

    public FetchHintsBuilder setIncludeEdgeLabelsAndCounts(boolean includeEdgeLabelsAndCounts) {
        this.includeEdgeLabelsAndCounts = includeEdgeLabelsAndCounts;
        return this;
    }

    public FetchHintsBuilder setIncludeExtendedDataTableNames(boolean includeExtendedDataTableNames) {
        this.includeExtendedDataTableNames = includeExtendedDataTableNames;
        return this;
    }
}
