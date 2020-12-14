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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class FetchHints implements Serializable  {
    private final boolean includeAllProperties;
    private final ImmutableSet<String> propertyNamesToInclude;
    private final boolean includeAllPropertyMetadata;
    private final ImmutableSet<String> metadataKeysToInclude;
    private final boolean includeHidden;
    private final boolean includeAllEdgeRefs;
    private final boolean includeOutEdgeRefs;
    private final boolean includeInEdgeRefs;
    private final boolean includeEdgeIds;
    private final boolean includeEdgeVertexIds;
    private final ImmutableSet<String> edgeLabelsOfEdgeRefsToInclude;
    private final boolean includeEdgeLabelsAndCounts;
    private final boolean includeExtendedDataTableNames;

    public static final FetchHints NONE = new FetchHintsBuilder()
            .build();

    public static final FetchHints PROPERTIES = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .build();

    public static final FetchHints PROPERTIES_AND_EDGE_REFS = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeExtendedDataTableNames(true)
            .setIncludeAllEdgeRefs(true)
            .build();

    public static final FetchHints PROPERTIES_AND_METADATA = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .build();

    public static final FetchHints ALL = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeAllEdgeRefs(true)
            .setIncludeExtendedDataTableNames(true)
            .build();

    public static final FetchHints ALL_INCLUDING_HIDDEN = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeAllEdgeRefs(true)
            .setIncludeExtendedDataTableNames(true)
            .setIncludeHidden(true)
            .build();

    public static final FetchHints EDGE_REFS = new FetchHintsBuilder()
            .setIncludeAllEdgeRefs(true)
            .build();

    public static final FetchHints EDGE_LABELS = new FetchHintsBuilder()
            .setIncludeEdgeLabelsAndCounts(true)
            .build();

    FetchHints(
            boolean includeAllProperties,
            ImmutableSet<String> propertyNamesToInclude,
            boolean includeAllPropertyMetadata,
            ImmutableSet<String> metadataKeysToInclude,
            boolean includeHidden,
            boolean includeAllEdgeRefs,
            boolean includeOutEdgeRefs,
            boolean includeInEdgeRefs,
            boolean includeEdgeIds,
            boolean includeEdgeVertexIds,
            ImmutableSet<String> edgeLabelsOfEdgeRefsToInclude,
            boolean includeEdgeLabelsAndCounts,
            boolean includeExtendedDataTableNames
    ) {
        this.includeAllProperties = includeAllProperties;
        this.propertyNamesToInclude = propertyNamesToInclude;
        this.includeAllPropertyMetadata = includeAllPropertyMetadata;
        this.metadataKeysToInclude = metadataKeysToInclude;
        this.includeHidden = includeHidden;
        this.includeAllEdgeRefs = includeAllEdgeRefs;
        this.includeOutEdgeRefs = includeOutEdgeRefs;
        this.includeInEdgeRefs = includeInEdgeRefs;
        this.includeEdgeIds = includeEdgeIds;
        this.includeEdgeVertexIds = includeEdgeVertexIds;
        this.edgeLabelsOfEdgeRefsToInclude = includeAllEdgeRefs ? null : edgeLabelsOfEdgeRefsToInclude;
        this.includeEdgeLabelsAndCounts = includeEdgeLabelsAndCounts;
        this.includeExtendedDataTableNames = includeExtendedDataTableNames;
    }

    public static FetchHints copyAndMakeVisible(FetchHints fetchHints) {
        return new FetchHintsBuilder(fetchHints)
                .setIncludeHidden(true).build();
    }

    public boolean isIncludeAllProperties() {
        return includeAllProperties;
    }

    public ImmutableSet<String> getPropertyNamesToInclude() {
        return propertyNamesToInclude;
    }

    public boolean isIncludeAllPropertyMetadata() {
        return includeAllPropertyMetadata;
    }

    public ImmutableSet<String> getMetadataKeysToInclude() {
        return metadataKeysToInclude;
    }

    public boolean isIncludeHidden() {
        return includeHidden;
    }

    public boolean isIncludeAllEdgeRefs() {
        return includeAllEdgeRefs;
    }

    public boolean isIncludeOutEdgeRefs() {
        return includeOutEdgeRefs;
    }

    public boolean isIncludeInEdgeRefs() {
        return includeInEdgeRefs;
    }

    public boolean isIncludeEdgeIds() {
        return includeEdgeIds;
    }

    public boolean isIncludeEdgeVertexIds() {
        return includeEdgeVertexIds;
    }

    public ImmutableSet<String> getEdgeLabelsOfEdgeRefsToInclude() {
        return edgeLabelsOfEdgeRefsToInclude;
    }

    public boolean isIncludeEdgeLabelsAndCounts() {
        return includeEdgeLabelsAndCounts;
    }

    public boolean isIncludeExtendedDataTableNames() {
        return includeExtendedDataTableNames;
    }

    public boolean isIncludePropertyMetadata() {
        return isIncludeAllPropertyMetadata() || (getMetadataKeysToInclude() != null && getMetadataKeysToInclude().size() > 0);
    }

    public boolean isIncludeProperties() {
        return isIncludeAllProperties() || (getPropertyNamesToInclude() != null && getPropertyNamesToInclude().size() > 0);
    }

    public boolean isIncludePropertyAndMetadata(String propertyName) {
        return isIncludeProperty(propertyName) && isIncludeAllPropertyMetadata();
    }

    public boolean isIncludeProperty(String propertyName) {
        if (isIncludeAllProperties()) {
            return true;
        }
        if (getPropertyNamesToInclude() != null && getPropertyNamesToInclude().contains(propertyName)) {
            return true;
        }
        return false;
    }

    public boolean isIncludeMetadata(String metadataKey) {
        if (isIncludeAllPropertyMetadata()) {
            return true;
        }
        if (getMetadataKeysToInclude() != null && getMetadataKeysToInclude().contains(metadataKey)) {
            return true;
        }
        return false;
    }

    public boolean isIncludeEdgeRefLabel(String label) {
        if (isIncludeAllEdgeRefs()) {
            return true;
        }
        if (getEdgeLabelsOfEdgeRefsToInclude() != null) {
            if (getEdgeLabelsOfEdgeRefsToInclude().contains(label)) {
                return true;
            } else {
                return false;
            }
        }
        if (isIncludeOutEdgeRefs() || isIncludeInEdgeRefs()) {
            return true;
        }
        return false;
    }

    public boolean isIncludeEdgeRefs() {
        return isIncludeAllEdgeRefs() || isIncludeInEdgeRefs() || isIncludeOutEdgeRefs()
                || (getEdgeLabelsOfEdgeRefsToInclude() != null && getEdgeLabelsOfEdgeRefsToInclude().size() > 0);
    }

    public boolean hasEdgeLabelsOfEdgeRefsToInclude() {
        return getEdgeLabelsOfEdgeRefsToInclude() != null && getEdgeLabelsOfEdgeRefsToInclude().size() > 0;
    }

    public void validateHasEdgeFetchHints(Direction direction, String... labels) {
        if (!isIncludeEdgeRefs()) {
            throw new GeMissingFetchHintException(this, "edgeRefs");
        }
        switch (direction) {
            case OUT:
                if (!isIncludeOutEdgeRefs() && !hasEdgeLabelsOfEdgeRefsToInclude()) {
                    throw new GeMissingFetchHintException(this, "outEdgeRefs or edgeLabels");
                }
                break;
            case IN:
                if (!isIncludeInEdgeRefs() && !hasEdgeLabelsOfEdgeRefsToInclude()) {
                    throw new GeMissingFetchHintException(this, "inEdgeRefs or edgeLabels");
                }
                break;
        }

        if (labels != null
                && labels.length != 0
                && !isIncludeAllEdgeRefs()
                && !isIncludeInEdgeRefs()
                && !isIncludeOutEdgeRefs()
                && (getEdgeLabelsOfEdgeRefsToInclude() != null && getEdgeLabelsOfEdgeRefsToInclude().size() > 0)) {
            for (String label : labels) {
                if (!getEdgeLabelsOfEdgeRefsToInclude().contains(label)) {
                    throw new GeMissingFetchHintException(this, "edgeLabel:" + label);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "FetchHints{" +
                "includeAllProperties=" + includeAllProperties +
                ", propertyNamesToInclude=" + setToString(propertyNamesToInclude) +
                ", includeAllPropertyMetadata=" + includeAllPropertyMetadata +
                ", metadataKeysToInclude=" + setToString(metadataKeysToInclude) +
                ", includeHidden=" + includeHidden +
                ", includeAllEdgeRefs=" + includeAllEdgeRefs +
                ", edgeLabelsOfEdgeRefsToInclude=" + setToString(edgeLabelsOfEdgeRefsToInclude) +
                ", includeEdgeLabelsAndCounts=" + includeEdgeLabelsAndCounts +
                ", includeExtendedDataTableNames=" + includeExtendedDataTableNames +
                '}';
    }

    private String setToString(ImmutableSet<String> set) {
        return set == null ? "" : Joiner.on(",").join(set);
    }

    public static FetchHintsBuilder builder() {
        return new FetchHintsBuilder();
    }

    public static FetchHintsBuilder builder(FetchHints fetchHints) {
        return new FetchHintsBuilder(fetchHints);
    }

    public void assertPropertyIncluded(String name) {
        if (isIncludeProperty(name)) {
            return;
        }
        throw new GeMissingFetchHintException(this, "property:" + name);
    }

    public void assertMetadataIncluded(String key) {
        if (isIncludeMetadata(key)) {
            return;
        }
        throw new GeMissingFetchHintException(this, "metadata:" + key);
    }

    /**
     * Tests that these fetch hints have at least the requested fetch hints
     *
     * @return true if this has all or more fetch hints then those passed in.
     */
    public boolean hasFetchHints(FetchHints fetchHints) {
        if (fetchHints.includeHidden && !this.includeHidden) {
            return false;
        }

        if (fetchHints.includeEdgeLabelsAndCounts && !this.includeEdgeLabelsAndCounts) {
            return false;
        }

        if (fetchHints.includeAllEdgeRefs && !this.includeAllEdgeRefs) {
            return false;
        }

        if (fetchHints.includeOutEdgeRefs && !(this.includeOutEdgeRefs || this.includeAllEdgeRefs)) {
            return false;
        }

        if (fetchHints.includeInEdgeRefs && !(this.includeInEdgeRefs || this.includeAllEdgeRefs)) {
            return false;
        }

        if (fetchHints.includeEdgeIds && !this.includeEdgeIds) {
            return false;
        }

        if (fetchHints.includeEdgeVertexIds && !this.includeEdgeVertexIds) {
            return false;
        }

        if (fetchHints.includeExtendedDataTableNames && !this.includeExtendedDataTableNames) {
            return false;
        }

        if (fetchHints.includeAllPropertyMetadata && !this.includeAllPropertyMetadata) {
            return false;
        }

        if (fetchHints.includeAllProperties && !this.includeAllProperties) {
            return false;
        }

        if (fetchHints.edgeLabelsOfEdgeRefsToInclude != null
                && fetchHints.edgeLabelsOfEdgeRefsToInclude.size() > 0
                && !isEdgeLabelsOfEdgeRefsIncluded(fetchHints.edgeLabelsOfEdgeRefsToInclude)) {
            return false;
        }

        if (fetchHints.propertyNamesToInclude != null
                && fetchHints.propertyNamesToInclude.size() > 0
                && !isPropertyNamesIncluded(fetchHints.propertyNamesToInclude)) {
            return false;
        }

        if (fetchHints.metadataKeysToInclude != null
                && fetchHints.metadataKeysToInclude.size() > 0
                && !isMetadataKeysIncluded(fetchHints.metadataKeysToInclude)) {
            return false;
        }

        return true;
    }

    private boolean isMetadataKeysIncluded(ImmutableSet<String> metadataKeysToInclude) {
        if (includeAllPropertyMetadata) {
            return true;
        }
        if (this.metadataKeysToInclude == null) {
            return false;
        }
        for (String metadataKey : metadataKeysToInclude) {
            if (!this.metadataKeysToInclude.contains(metadataKey)) {
                return false;
            }
        }
        return true;
    }

    private boolean isPropertyNamesIncluded(ImmutableSet<String> propertyNamesToInclude) {
        if (includeAllProperties) {
            return true;
        }
        if (this.propertyNamesToInclude == null) {
            return false;
        }
        for (String propertyName : propertyNamesToInclude) {
            if (!this.propertyNamesToInclude.contains(propertyName)) {
                return false;
            }
        }
        return true;
    }

    private boolean isEdgeLabelsOfEdgeRefsIncluded(ImmutableSet<String> edgeLabelsOfEdgeRefsToInclude) {
        if (includeAllEdgeRefs) {
            return true;
        }
        if (this.edgeLabelsOfEdgeRefsToInclude == null) {
            return false;
        }
        for (String edgeLabel : edgeLabelsOfEdgeRefsToInclude) {
            if (!this.edgeLabelsOfEdgeRefsToInclude.contains(edgeLabel)) {
                return false;
            }
        }
        return true;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FetchHints that = (FetchHints) o;
        return includeAllProperties == that.includeAllProperties &&
                includeAllPropertyMetadata == that.includeAllPropertyMetadata &&
                includeHidden == that.includeHidden &&
                includeAllEdgeRefs == that.includeAllEdgeRefs &&
                includeOutEdgeRefs == that.includeOutEdgeRefs &&
                includeInEdgeRefs == that.includeInEdgeRefs &&
                includeEdgeIds == that.includeEdgeIds &&
                includeEdgeVertexIds == that.includeEdgeVertexIds &&
                includeEdgeLabelsAndCounts == that.includeEdgeLabelsAndCounts &&
                includeExtendedDataTableNames == that.includeExtendedDataTableNames &&
                Objects.equals(propertyNamesToInclude, that.propertyNamesToInclude) &&
                Objects.equals(metadataKeysToInclude, that.metadataKeysToInclude) &&
                Objects.equals(edgeLabelsOfEdgeRefsToInclude, that.edgeLabelsOfEdgeRefsToInclude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeAllProperties,
                propertyNamesToInclude,
                includeAllPropertyMetadata,
                metadataKeysToInclude,
                includeHidden,
                includeAllEdgeRefs,
                includeOutEdgeRefs,
                includeInEdgeRefs,
                includeEdgeIds,
                includeEdgeVertexIds,
                edgeLabelsOfEdgeRefsToInclude,
                includeEdgeLabelsAndCounts,
                includeExtendedDataTableNames);
    }

    public static FetchHints union(FetchHints... fetchHints) {
        return union(Arrays.asList(fetchHints));
    }

    public static FetchHints union(Iterable<FetchHints> fetchHints) {
        boolean includeAllProperties = false;
        Set<String> propertyNamesToInclude = null;
        boolean includeAllPropertyMetadata = false;
        Set<String> metadataKeysToInclude = null;
        Boolean includeHidden = null;
        boolean includeAllEdgeRefs = false;
        boolean includeOutEdgeRefs = false;
        boolean includeInEdgeRefs = false;
        boolean includeEdgeIds = false;
        boolean includeEdgeVertexIds = false;
        boolean includeEdgeLabelsAndCounts = false;
        boolean includeExtendedDataTableNames = false;
        Set<String> edgeLabelsOfEdgeRefsToInclude = null;

        for (FetchHints fetchHint : fetchHints) {
            if (fetchHint.isIncludeAllProperties()) {
                includeAllProperties = true;
            }
            if (fetchHint.isIncludeAllPropertyMetadata()) {
                includeAllPropertyMetadata = true;
            }
            if (fetchHint.isIncludeAllEdgeRefs()) {
                includeAllEdgeRefs = true;
            }
            if (fetchHint.isIncludeOutEdgeRefs()) {
                includeOutEdgeRefs = true;
            }
            if (fetchHint.isIncludeInEdgeRefs()) {
                includeInEdgeRefs = true;
            }
            if (fetchHint.isIncludeEdgeIds()) {
                includeEdgeIds = true;
            }
            if (fetchHint.isIncludeEdgeVertexIds()) {
                includeEdgeVertexIds = true;
            }
            if (fetchHint.isIncludeEdgeLabelsAndCounts()) {
                includeEdgeLabelsAndCounts = true;
            }
            if (fetchHint.isIncludeExtendedDataTableNames()) {
                includeExtendedDataTableNames = true;
            }

            if (includeHidden != null && includeHidden != fetchHint.isIncludeHidden()) {
                throw new GeException("Incompatible fetch hints to combine (includeHidden).");
            }
            includeHidden = fetchHint.isIncludeHidden();

            if (fetchHint.getPropertyNamesToInclude() != null) {
                if (propertyNamesToInclude == null) {
                    propertyNamesToInclude = new HashSet<>(fetchHint.getPropertyNamesToInclude());
                } else {
                    propertyNamesToInclude.addAll(fetchHint.getPropertyNamesToInclude());
                }
            }

            if (fetchHint.getMetadataKeysToInclude() != null) {
                if (metadataKeysToInclude == null) {
                    metadataKeysToInclude = new HashSet<>(fetchHint.getMetadataKeysToInclude());
                } else {
                    metadataKeysToInclude.addAll(fetchHint.getMetadataKeysToInclude());
                }
            }

            if (fetchHint.getEdgeLabelsOfEdgeRefsToInclude() != null) {
                if (edgeLabelsOfEdgeRefsToInclude == null) {
                    edgeLabelsOfEdgeRefsToInclude = new HashSet<>(fetchHint.getEdgeLabelsOfEdgeRefsToInclude());
                } else {
                    edgeLabelsOfEdgeRefsToInclude.addAll(fetchHint.getEdgeLabelsOfEdgeRefsToInclude());
                }
            }
        }

        return new FetchHints(
                includeAllProperties,
                propertyNamesToInclude == null ? null : ImmutableSet.copyOf(propertyNamesToInclude),
                includeAllPropertyMetadata,
                metadataKeysToInclude == null ? null : ImmutableSet.copyOf(metadataKeysToInclude),
                includeHidden == null ? false : includeHidden,
                includeAllEdgeRefs,
                includeOutEdgeRefs,
                includeInEdgeRefs,
                includeEdgeIds,
                includeEdgeVertexIds,
                edgeLabelsOfEdgeRefsToInclude == null ? null : ImmutableSet.copyOf(edgeLabelsOfEdgeRefsToInclude),
                includeEdgeLabelsAndCounts,
                includeExtendedDataTableNames
        );
    }
}
