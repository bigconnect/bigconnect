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
package com.mware.core.model.schema;

public interface SchemaConstants {
    String CONCEPT_TYPE_THING = "thing";
    String CONCEPT_TYPE_RAW = "raw";
    String CONCEPT_TYPE_DOCUMENT = "document";
    String CONCEPT_TYPE_AUDIO = "audio";
    String CONCEPT_TYPE_IMAGE = "image";
    String CONCEPT_TYPE_VIDEO = "video";
    String CONCEPT_TYPE_PERSON = "person";
    String CONCEPT_TYPE_EVENT = "event";
    String CONCEPT_TYPE_LOCATION = "location";
    String CONCEPT_TYPE_SOCIAL_POST = "socialPost";
    String CONCEPT_TYPE_SOCIAL_COMMENT = "socialComment";
    String CONCEPT_TYPE_ORGANIZATION = "organization";

    String EDGE_LABEL_HAS_SOURCE = "hasSource";
    String EDGE_LABEL_HAS_ENTITY = "hasEntity";
    String EDGE_LABEL_RAW_CONTAINS_IMAGE_OF_ENTITY = "rawContainsImageOfEntity";
    String EDGE_LABEL_HAS_IMAGE = "entityHasImageRaw";
    String EDGE_LABEL_HAS_DETECTED_ENTITY = "hasDetectedEntity";
    String EDGE_LABEL_HAS_SOCIAL_COMMENT= "hasSocialComment";
    String EDGE_LABEL_FACE_OF = "faceOf";
    String EDGE_LABEL_FACE_EVENT = "faceEvent";

    String INTENT_ENTITY_IMAGE = "entityImage";
    String INTENT_ARTIFACT_CONTAINS_IMAGE = "artifactContainsImage";
    String INTENT_ARTIFACT_TITLE = "artifactTitle";
    String INTENT_ARTIFACT_HAS_ENTITY = "artifactHasEntity";
    String INTENT_ARTIFACT_CONTAINS_IMAGE_OF_ENTITY = "artifactContainsImageOfEntity";
    String INTENT_ENTITY_HAS_IMAGE = "entityHasImage";
    String INTENT_MEDIA_DURATION = "media.duration";
    String INTENT_MEDIA_DATE_TAKEN = "media.dateTaken";
    String INTENT_MEDIA_DEVICE_MAKE = "media.deviceMake";
    String INTENT_MEDIA_DEVICE_MODEL = "media.deviceModel";
    String INTENT_MEDIA_WIDTH = "media.width";
    String INTENT_MEDIA_HEIGHT = "media.height";
    String INTENT_MEDIA_METADATA = "media.metadata";
    String INTENT_MEDIA_FILE_SIZE = "media.fileSize";
    String INTENT_MEDIA_DESCRIPTION = "media.description";
    String INTENT_MEDIA_IMAGE_HEADING = "media.imageHeading";
    String INTENT_MEDIA_Y_AXIS_FLIPPED = "media.yAxisFlipped";
    String INTENT_MEDIA_CLOCKWISE_ROTATION = "media.clockwiseRotation";

    String INTENT_AUDIO_DURATION = "audioDuration";
    String INTENT_VIDEO_DURATION = "videoDuration";
    String INTENT_GEOLOCATION = "geoLocation";

    String CUSTOM_DISPLAY_LONGTEXT = "longText";
    String CUSTOM_DISPLAY_HEADING = "heading";
    String CUSTOM_DISPLAY_BYTE = "byte";
    String CUSTOM_DISPLAY_LINK = "link";
    String CUSTOM_DISPLAY_DURATION = "duration";
    String CUSTOM_DISPLAY_DATEONLY = "dateOnly";
    String CUSTOM_DISPLAY_GEOLOCATION = "geoLocation";

    String DISPLAY_TYPE_AUDIO = "audio";
    String DISPLAY_TYPE_IMAGE = "image";
    String DISPLAY_TYPE_VIDEO = "video";
    String DISPLAY_TYPE_DOCUMENT = "document";

}
