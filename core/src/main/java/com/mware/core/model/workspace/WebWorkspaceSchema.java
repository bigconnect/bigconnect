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
package com.mware.core.model.workspace;

import com.mware.core.model.properties.types.BooleanSingleValueBcProperty;
import com.mware.core.model.properties.types.StreamingBcProperty;
import com.mware.core.model.properties.types.StringBcProperty;
import com.mware.core.model.properties.types.StringSingleValueBcProperty;

public class WebWorkspaceSchema {
    public static final String DASHBOARD_CONCEPT_NAME = "__dbd";
    public static final String PRODUCT_CONCEPT_NAME = "__pd";
    public static final String DASHBOARD_ITEM_CONCEPT_NAME = "__dbdi";

    public static final String WORKSPACE_TO_DASHBOARD_RELATIONSHIP_NAME = "__wsToDbd";
    public static final String WORKSPACE_TO_PRODUCT_RELATIONSHIP_NAME = "__wsToPd";
    public static final String PRODUCT_TO_ENTITY_RELATIONSHIP_NAME = "__pdToE";
    public static final String DASHBOARD_TO_DASHBOARD_ITEM_RELATIONSHIP_NAME = "__dbdToItm";

    public static final BooleanSingleValueBcProperty PRODUCT_TO_ENTITY_IS_ANCILLARY = new BooleanSingleValueBcProperty("productToEntityAncillary");

    public static final StringSingleValueBcProperty LAST_ACTIVE_PRODUCT_ID = new StringSingleValueBcProperty("workspaceLastActiveProductId");

    public static final StringSingleValueBcProperty DASHBOARD_ITEM_EXTENSION_ID = new StringSingleValueBcProperty("dashboardItemExtensionId");
    public static final StringSingleValueBcProperty DASHBOARD_ITEM_CONFIGURATION = new StringSingleValueBcProperty("dashboardItemConfiguration");

    public static final StringSingleValueBcProperty PRODUCT_KIND = new StringSingleValueBcProperty("productKind");
    public static final StringBcProperty PRODUCT_DATA = new StringBcProperty("productData");
    public static final StringBcProperty PRODUCT_EXTENDED_DATA = new StringBcProperty("productExtendedData");
    public static final StreamingBcProperty PRODUCT_PREVIEW_DATA_URL = new StreamingBcProperty("productPreviewDataUrl");
}
