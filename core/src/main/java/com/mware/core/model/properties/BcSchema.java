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
package com.mware.core.model.properties;

import com.mware.core.exception.BcException;
import com.mware.core.model.properties.types.*;
import com.mware.core.model.termMention.TermMentionForProperty;

import java.lang.reflect.Field;

public class BcSchema {
    public static final StringMetadataBcProperty TEXT_DESCRIPTION_METADATA = new StringMetadataBcProperty("textDescription");
    public static final StringMetadataBcProperty TEXT_LANGUAGE_METADATA = new StringMetadataBcProperty("textLanguage");
    public static final StringMetadataBcProperty MIME_TYPE_METADATA = new StringMetadataBcProperty("mimeType");

    public static final DateSingleValueBcProperty MODIFIED_DATE = new DateSingleValueBcProperty("modifiedDate");
    public static final DateMetadataBcProperty MODIFIED_DATE_METADATA = new DateMetadataBcProperty("modifiedDate");

    public static final VisibilityJsonBcProperty VISIBILITY_JSON = new VisibilityJsonBcProperty("visibilityJson");
    public static final VisibilityJsonMetadataBcProperty VISIBILITY_JSON_METADATA = new VisibilityJsonMetadataBcProperty("visibilityJson");

    public static final StreamingSingleValueBcProperty METADATA_JSON = new StreamingSingleValueBcProperty("metadataJson");
    public static final StreamingBcProperty TEXT = new StreamingBcProperty("text");
    public static final StreamingSingleValueBcProperty RAW = new StreamingSingleValueBcProperty("raw");

    public static final StringBcProperty DATA_WORKER_WHITE_LIST = new StringBcProperty("graphPropertyWorkerWhiteList");
    public static final StringBcProperty DATA_WORKER_BLACK_LIST = new StringBcProperty("graphPropertyWorkerBlackList");
    public static final StringBcProperty FILE_NAME = new StringBcProperty("fileName");
    public static final StringBcProperty MIME_TYPE = new StringBcProperty("mimeType");
    public static final StringSingleValueBcProperty MODIFIED_BY = new StringSingleValueBcProperty("modifiedBy");
    public static final StringMetadataBcProperty MODIFIED_BY_METADATA = new StringMetadataBcProperty("modifiedBy");
    public static final StringSingleValueBcProperty JUSTIFICATION = new StringSingleValueBcProperty("justification");
    public static final StringMetadataBcProperty JUSTIFICATION_METADATA = new StringMetadataBcProperty("justification");
    public static final StringBcProperty TITLE = new StringBcProperty("title");
    public static final StringBcProperty COMMENT = new StringBcProperty("commentEntry");

    public static final LongSingleValueBcProperty TERM_MENTION_START_OFFSET = new LongSingleValueBcProperty("tmStartOffset");
    public static final LongSingleValueBcProperty TERM_MENTION_END_OFFSET = new LongSingleValueBcProperty("tmEndOffset");
    public static final StringSingleValueBcProperty TERM_MENTION_PROCESS = new StringSingleValueBcProperty("tmProcess");
    public static final StringSingleValueBcProperty TERM_MENTION_PROPERTY_KEY = new StringSingleValueBcProperty("tmPropertyKey");
    public static final StringSingleValueBcProperty TERM_MENTION_PROPERTY_NAME = new StringSingleValueBcProperty("tmPropertyName");
    public static final StringSingleValueBcProperty TERM_MENTION_RESOLVED_EDGE_ID = new StringSingleValueBcProperty("tmResolvedEdgeId");
    public static final StringSingleValueBcProperty TERM_MENTION_TITLE = new StringSingleValueBcProperty("tmTitle");
    public static final StringSingleValueBcProperty TERM_MENTION_CONCEPT_TYPE = new StringSingleValueBcProperty("tmConceptType");
    public static final VisibilityJsonBcProperty TERM_MENTION_VISIBILITY_JSON = new VisibilityJsonBcProperty("tmVisibilityJson");
    public static final StringSingleValueBcProperty TERM_MENTION_REF_PROPERTY_KEY = new StringSingleValueBcProperty("tmRefPropertyKey");
    public static final StringSingleValueBcProperty TERM_MENTION_REF_PROPERTY_NAME = new StringSingleValueBcProperty("tmRefPropertyName");
    public static final StringSingleValueBcProperty TERM_MENTION_REF_PROPERTY_VISIBILITY = new StringSingleValueBcProperty("tmRefPropertyVisibility");
    public static final StringSingleValueBcProperty TERM_MENTION_FOR_ELEMENT_ID = new StringSingleValueBcProperty("tmForElementId");
    public static final TermMentionForProperty TERM_MENTION_FOR_TYPE = new TermMentionForProperty("tmForType");
    public static final StringSingleValueBcProperty TERM_MENTION_SNIPPET = new StringSingleValueBcProperty("tmSnippet");
    public static final String TERM_MENTION_LABEL_HAS_TERM_MENTION = "tmHasTermMention";
    public static final String TERM_MENTION_LABEL_RESOLVED_TO = "tmRsolvedTo";
    public static final String TERM_MENTION_RESOLVED_FROM = "tmResolvedFrom";

    public static final DateSingleValueBcProperty EVENT_TIME = new DateSingleValueBcProperty("eventTime");

    private BcSchema() {
        throw new UnsupportedOperationException("do not construct utility class");
    }

    public static boolean isBuiltInProperty(String propertyName) {
        return isBuiltInProperty(BcSchema.class, propertyName);
    }

    public static boolean isBuiltInProperty(Class propertiesClass, String propertyName) {
        for (Field field : propertiesClass.getFields()) {
            try {
                Object fieldValue = field.get(null);
                if (fieldValue instanceof BcPropertyBase) {
                    if (((BcPropertyBase) fieldValue).getPropertyName().equals(propertyName)) {
                        return true;
                    }
                }
            } catch (IllegalAccessException e) {
                throw new BcException("Could not get field: " + field, e);
            }
        }
        return false;
    }
}
