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
package com.mware.core.model.properties.types;

import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.user.User;
import com.mware.ge.Metadata;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.Value;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PropertyMetadata {
    private static final Visibility DEFAULT_VISIBILITY = new Visibility("");

    private final ZonedDateTime modifiedDate;
    private final User modifiedBy;
    private final VisibilityJson visibilityJson;
    private final Visibility propertyVisibility;
    private final List<AdditionalMetadataItem> additionalMetadataItems = new ArrayList<>();

    /**
     * @param modifiedBy The user to set as the modifiedBy metadata
     * @param visibilityJson The visibility json to use in the metadata
     * @param propertyVisibility The visibility of the property
     */
    public PropertyMetadata(User modifiedBy, VisibilityJson visibilityJson, Visibility propertyVisibility) {
        this(ZonedDateTime.now(), modifiedBy, visibilityJson, propertyVisibility);
    }

    /**
     * @param modifiedDate The date to use as modifiedDate
     * @param modifiedBy The user to set as the modifiedBy metadata
     * @param visibilityJson The visibility json to use in the metadata
     * @param propertyVisibility The visibility of the property
     */
    public PropertyMetadata(ZonedDateTime modifiedDate, User modifiedBy, VisibilityJson visibilityJson, Visibility propertyVisibility) {
        this(modifiedDate, modifiedBy, null, visibilityJson, propertyVisibility);
    }

    /**
     * @param modifiedDate The date to use as modifiedDate
     * @param modifiedBy The user to set as the modifiedBy metadata
     * @param confidence The confidence metadata value
     * @param visibilityJson The visibility json to use in the metadata
     * @param propertyVisibility The visibility of the property
     */
    public PropertyMetadata(
            ZonedDateTime modifiedDate,
            User modifiedBy,
            Double confidence,
            VisibilityJson visibilityJson,
            Visibility propertyVisibility
    ) {
        this.modifiedDate = modifiedDate;
        this.modifiedBy = modifiedBy;
        this.visibilityJson = visibilityJson;
        this.propertyVisibility = propertyVisibility;
    }

    public PropertyMetadata(PropertyMetadata metadata) {
        this(
                metadata.getModifiedDate(),
                metadata.getModifiedBy(),
                metadata.getVisibilityJson(),
                metadata.getPropertyVisibility()
        );
        for (AdditionalMetadataItem item : metadata.getAdditionalMetadataItems()) {
            add(item.getKey(), item.getValue(), item.getVisibility());
        }
    }

    public Metadata createMetadata() {
        Metadata metadata = Metadata.create();
        BcSchema.MODIFIED_DATE_METADATA.setMetadata(metadata, modifiedDate, DEFAULT_VISIBILITY);
        BcSchema.MODIFIED_BY_METADATA.setMetadata(metadata, modifiedBy.getUserId(), DEFAULT_VISIBILITY);
        BcSchema.VISIBILITY_JSON_METADATA.setMetadata(metadata, visibilityJson, DEFAULT_VISIBILITY);
        for (AdditionalMetadataItem additionalMetadataItem : additionalMetadataItems) {
            metadata.add(
                    additionalMetadataItem.getKey(),
                    additionalMetadataItem.getValue(),
                    additionalMetadataItem.getVisibility()
            );
        }
        return metadata;
    }

    public User getModifiedBy() {
        return modifiedBy;
    }

    public ZonedDateTime getModifiedDate() {
        return modifiedDate;
    }

    public VisibilityJson getVisibilityJson() {
        return visibilityJson;
    }

    public Visibility getPropertyVisibility() {
        return propertyVisibility;
    }

    public Iterable<AdditionalMetadataItem> getAdditionalMetadataItems() {
        return additionalMetadataItems;
    }

    public void add(String key, Value value, Visibility visibility) {
        additionalMetadataItems.add(new AdditionalMetadataItem(key, value, visibility));
    }

    private static class AdditionalMetadataItem {
        private final String key;
        private final Value value;
        private final Visibility visibility;

        public AdditionalMetadataItem(String key, Value value, Visibility visibility) {
            this.key = key;
            this.value = value;
            this.visibility = visibility;
        }

        public String getKey() {
            return key;
        }

        public Value getValue() {
            return value;
        }

        public Visibility getVisibility() {
            return visibility;
        }
    }
}
