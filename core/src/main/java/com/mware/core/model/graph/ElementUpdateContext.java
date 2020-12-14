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
package com.mware.core.model.graph;

import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.types.BcPropertyUpdate;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.ge.Edge;
import com.mware.ge.Element;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.EdgeMutation;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.mutation.VertexMutation;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ElementUpdateContext<T extends Element> {
    private final VisibilityTranslator visibilityTranslator;
    private final ElementMutation<T> mutation;
    private final User user;
    private final List<BcPropertyUpdate> properties = new ArrayList<>();
    private final T element;

    public ElementUpdateContext(VisibilityTranslator visibilityTranslator, ElementMutation<T> mutation, User user) {
        this.visibilityTranslator = visibilityTranslator;
        this.mutation = mutation;
        this.user = user;
        if (mutation instanceof ExistingElementMutation) {
            element = ((ExistingElementMutation<T>) mutation).getElement();
        } else {
            element = null;
        }
    }

    public boolean isNewElement() {
        return getElement() == null;
    }

    public ElementMutation<T> getMutation() {
        return mutation;
    }

    public List<BcPropertyUpdate> getProperties() {
        return properties;
    }

    public T getElement() {
        return element;
    }

    public void updateBuiltInProperties(
            ZonedDateTime modifiedDate,
            VisibilityJson visibilityJson
    ) {
        checkNotNull(user, "User cannot be null");
        Visibility defaultVisibility = visibilityTranslator.getDefaultVisibility();
        if (user != null) {
            BcSchema.MODIFIED_BY.updateProperty(this, user.getUserId(), defaultVisibility);
        }
        BcSchema.MODIFIED_DATE.updateProperty(this, modifiedDate, defaultVisibility);
        BcSchema.VISIBILITY_JSON.updateProperty(this, visibilityJson, defaultVisibility);
    }

    public void updateBuiltInProperties(PropertyMetadata propertyMetadata) {
        updateBuiltInProperties(propertyMetadata.getModifiedDate(), propertyMetadata.getVisibilityJson());
    }

    public void setConceptType(String conceptType) {
        if (isEdgeMutation()) {
            throw new IllegalArgumentException("Cannot set concept type on edges");
        }

        ((VertexMutation)mutation).alterConceptType(conceptType);
    }

    private boolean isEdgeMutation() {
        if (getMutation() instanceof EdgeMutation) {
            return true;
        }
        if (getMutation() instanceof ExistingElementMutation) {
            ExistingElementMutation m = (ExistingElementMutation) getMutation();
            if (m.getElement() instanceof Edge) {
                return true;
            }
        }
        return false;
    }
}
