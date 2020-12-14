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

import com.mware.core.model.properties.types.*;

public class RawObjectSchema {
    public static final StringSingleValueBcProperty TAG = new StringSingleValueBcProperty("tag");
    public static final StreamingBcProperty CACHED_IMAGE = new StreamingBcProperty("cachedImage");
    public static final StringBcProperty CONTENT_HASH = new StringBcProperty("contentHash");
    public static final StringBcProperty ENTITY_IMAGE_URL = new StringBcProperty("entityImageUrl");
    public static final StringBcProperty SOURCE = new StringBcProperty("source");
    public static final StringSingleValueBcProperty URL = new StringSingleValueBcProperty("url");
    public static final StringSingleValueBcProperty ENTITY_IMAGE_VERTEX_ID = new StringSingleValueBcProperty("entityImageVertexId");
    public static final StringSingleValueBcProperty IMAGE_OWNER_VERTEX_ID = new StringSingleValueBcProperty("imageOwnerVertexId");
    public static final StringSingleValueBcProperty IMAGE_OWNER_CONCEPT_TYPE = new StringSingleValueBcProperty("imageOwnerConceptType");
    public static final StringSingleValueBcProperty RAW_LANGUAGE = new StringSingleValueBcProperty("language");
    public static final StringSingleValueBcProperty RAW_SENTIMENT = new StringSingleValueBcProperty("sentiment");
    public static final StringSingleValueBcProperty RAW_TYPE = new StringSingleValueBcProperty("rawType");
    public static final DetectedObjectProperty DETECTED_OBJECT = new DetectedObjectProperty("detectedObject");
    public static final StringBcProperty PROCESS = new StringBcProperty("process");
    public static final StringBcProperty ROW_KEY = new StringBcProperty("rowKey");
    public static final StringMetadataBcProperty COMMENT_PATH_METADATA = new StringMetadataBcProperty("commentPath");
    public static final GeoPointBcProperty GEOLOCATION_PROPERTY = new GeoPointBcProperty("geoLocation");
    public static final GeoShapeBcProperty GEOSHAPE_PROPERTY = new GeoShapeBcProperty("geoShape");
    public static final StringMetadataBcProperty SOURCE_FILE_NAME_METADATA = new StringMetadataBcProperty("sourceFileName");
    public static final StringMetadataBcProperty LINK_TITLE_METADATA = new StringMetadataBcProperty("linkTitle");
    public static final LongMetadataBcProperty SOURCE_FILE_OFFSET_METADATA = new LongMetadataBcProperty("sourceFileOffset");
    public static final JsonBcProperty TERM_MENTION = new JsonBcProperty("termMention");
    public static final StringSingleValueBcProperty AUTHOR = new StringSingleValueBcProperty("author");
    public static final StringSingleValueBcProperty AUTHOR_ID = new StringSingleValueBcProperty("authorId");
    public static final StringSingleValueBcProperty AUTHOR_REFERENCE = new StringSingleValueBcProperty("authorRef");
    public static final IntegerSingleValueBcProperty PAGE_COUNT = new IntegerSingleValueBcProperty("pageCount");
    public static final DateSingleValueBcProperty SOURCE_DATE = new DateSingleValueBcProperty("sourceDate");
    public static final IntegerSingleValueBcProperty LIKES = new IntegerSingleValueBcProperty("socialLikes");
    public static final IntegerSingleValueBcProperty SHARES = new IntegerSingleValueBcProperty("socialShares");
    public static final IntegerSingleValueBcProperty COMMENTS = new IntegerSingleValueBcProperty("socialComments");
    public static final StringSingleValueBcProperty ORIGINAL_POST = new StringSingleValueBcProperty("socialOrigPost");
    public static final StringSingleValueBcProperty ORIGINAL_POST_ID = new StringSingleValueBcProperty("socialOrigPostId");
    public static final StringSingleValueBcProperty ORIGINAL_AUTHOR = new StringSingleValueBcProperty("socialOrigAuthor");
    public static final StringSingleValueBcProperty ORIGINAL_AUTHOR_ID = new StringSingleValueBcProperty("socialOrigAuthorId");
    public static final StringBcProperty LINKS = new StringBcProperty("links");
    public static final StringBcProperty HASHTAGS = new StringBcProperty("hashtags");
}
