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
package com.mware.ge.accumulo.iterator.model;

import com.mware.ge.accumulo.iterator.util.ByteSequenceUtils;
import org.apache.accumulo.core.data.ByteSequence;

import java.io.Serializable;
import java.util.Set;
import java.util.stream.Collectors;

public class IteratorFetchHints implements Serializable {
    private static final long serialVersionUID = -6302969731435846529L;
    private final boolean includeAllProperties;
    private final Set<ByteSequence> propertyNamesToInclude;
    private final boolean includeAllPropertyMetadata;
    private final Set<ByteSequence> metadataKeysToInclude;
    private final boolean includeHidden;
    private final boolean includeAllEdgeRefs;
    private final boolean includeOutEdgeRefs;
    private final boolean includeInEdgeRefs;
    private final boolean includeEdgeIds;
    private final boolean includeEdgeVertexIds;
    private final Set<String> edgeLabelsOfEdgeRefsToInclude;
    private final boolean includeEdgeLabelsAndCounts;
    private final boolean includeExtendedDataTableNames;

    public IteratorFetchHints() {
        this.includeAllProperties = false;
        this.propertyNamesToInclude = null;
        this.includeAllPropertyMetadata = false;
        this.metadataKeysToInclude = null;
        this.includeHidden = false;
        this.includeAllEdgeRefs = false;
        this.includeOutEdgeRefs = false;
        this.includeInEdgeRefs = false;
        this.includeEdgeIds = true;
        this.includeEdgeVertexIds = true;
        this.edgeLabelsOfEdgeRefsToInclude = null;
        this.includeEdgeLabelsAndCounts = false;
        this.includeExtendedDataTableNames = false;
    }

    public IteratorFetchHints(
            boolean includeAllProperties,
            Set<ByteSequence> propertyNamesToInclude,
            boolean includeAllPropertyMetadata,
            Set<ByteSequence> metadataKeysToInclude,
            boolean includeHidden,
            boolean includeAllEdgeRefs,
            boolean includeOutEdgeRefs,
            boolean includeInEdgeRefs,
            boolean includeEdgeIds,
            boolean includeEdgeVertexIds,
            Set<String> edgeLabelsOfEdgeRefsToInclude,
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
        this.edgeLabelsOfEdgeRefsToInclude = edgeLabelsOfEdgeRefsToInclude;
        this.includeEdgeLabelsAndCounts = includeEdgeLabelsAndCounts;
        this.includeExtendedDataTableNames = includeExtendedDataTableNames;
    }

    public boolean isIncludeAllProperties() {
        return includeAllProperties;
    }

    public Set<ByteSequence> getPropertyNamesToInclude() {
        return propertyNamesToInclude;
    }

    public boolean isIncludeAllPropertyMetadata() {
        return includeAllPropertyMetadata;
    }

    public Set<ByteSequence> getMetadataKeysToInclude() {
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

    public Set<String> getEdgeLabelsOfEdgeRefsToInclude() {
        return edgeLabelsOfEdgeRefsToInclude;
    }

    public boolean isIncludeEdgeLabelsAndCounts() {
        return includeEdgeLabelsAndCounts;
    }

    public boolean isIncludeExtendedDataTableNames() {
        return includeExtendedDataTableNames;
    }

    @Override
    public String toString() {
        return "IteratorFetchHints{" +
                "includeAllProperties=" + includeAllProperties +
                ", propertyNamesToInclude=" + setOfByteSequencesToString(propertyNamesToInclude) +
                ", includeAllPropertyMetadata=" + includeAllPropertyMetadata +
                ", metadataKeysToInclude=" + setOfByteSequencesToString(metadataKeysToInclude) +
                ", includeHidden=" + includeHidden +
                ", includeAllEdgeRefs=" + includeAllEdgeRefs +
                ", includeOutEdgeRefs=" + includeOutEdgeRefs +
                ", includeInEdgeRefs=" + includeInEdgeRefs +
                ", includeEdgeIds=" + includeEdgeIds +
                ", includeEdgeVertexIds=" + includeEdgeVertexIds +
                ", edgeLabelsOfEdgeRefsToInclude=" + setToString(edgeLabelsOfEdgeRefsToInclude) +
                ", includeEdgeLabelsAndCounts=" + includeEdgeLabelsAndCounts +
                ", includeExtendedDataTableNames=" + includeExtendedDataTableNames +
                '}';
    }

    private String setOfByteSequencesToString(Set<ByteSequence> set) {
        if (set == null) {
            return "";
        }
        return set.stream()
                .map(ByteSequenceUtils::toString)
                .collect(Collectors.joining(","));
    }

    private String setToString(Set<String> set) {
        if (set == null) {
            return "";
        }
        return String.join(",", set);
    }
}
