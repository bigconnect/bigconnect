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

/**
 * BcProperties for media files (video, images, etc.).
 */
public interface MediaBcSchema {
    VideoTranscriptProperty VIDEO_TRANSCRIPT = new VideoTranscriptProperty("videoTranscript");
    StreamingBcProperty VIDEO_FRAME = new StreamingBcProperty("videoFrame");

    String MIME_TYPE_VIDEO_MP4 = "video/mp4";
    String MIME_TYPE_VIDEO_WEBM = "video/webm";
    String MIME_TYPE_AUDIO_MP3 = "audio/mp3";
    String MIME_TYPE_AUDIO_MP4 = "audio/mp4";
    String MIME_TYPE_AUDIO_OGG = "audio/ogg";

    String METADATA_VIDEO_FRAME_START_TIME = "videoFrameStartTime";

    StreamingBcProperty RAW_POSTER_FRAME = new StreamingBcProperty("rawPosterFrame");
    StreamingSingleValueBcProperty VIDEO_PREVIEW_IMAGE = new StreamingSingleValueBcProperty("videoPreviewImage");

    StreamingSingleValueBcProperty AUDIO_MP3 = new StreamingSingleValueBcProperty("audio-mp3");

    // Used by Image Analysis data workers (eg. Google Image Analysis)
    StringBcProperty IMAGE_TAG = new StringBcProperty("imageTag");
    DoubleMetadataBcProperty IMAGE_TAG_SCORE = new DoubleMetadataBcProperty("imageTagScore");

    StringBcProperty MEDIA_DESCRIPTION = new StringBcProperty("mediaDescription");
    StringSingleValueBcProperty MEDIA_DURATION = new StringSingleValueBcProperty("mediaDuration");
    DateSingleValueBcProperty MEDIA_DATE_TAKEN = new DateSingleValueBcProperty("mediaDateTaken");
    StringSingleValueBcProperty MEDIA_DEVICE_MAKE = new StringSingleValueBcProperty("mediaDeviceMake");
    StringSingleValueBcProperty MEDIA_DEVICE_MODEL = new StringSingleValueBcProperty("mediaDeviceModel");
    IntegerSingleValueBcProperty MEDIA_WIDTH = new IntegerSingleValueBcProperty("mediaWidth");
    IntegerSingleValueBcProperty MEDIA_HEIGHT = new IntegerSingleValueBcProperty("mediaHeight");
    StringSingleValueBcProperty MEDIA_METADATA = new StringSingleValueBcProperty("mediaMetadata");
    IntegerSingleValueBcProperty MEDIA_FILE_SIZE = new IntegerSingleValueBcProperty("mediaFileSize");
    DoubleSingleValueBcProperty MEDIA_IMAGE_HEADING = new DoubleSingleValueBcProperty("mediaImageHeading");
    BooleanSingleValueBcProperty MEDIA_Y_AXIS_FLIPPED = new BooleanSingleValueBcProperty("mediaYAxisFlipped");
    IntegerSingleValueBcProperty MEDIA_CLOCKWISE_ROTATION = new IntegerSingleValueBcProperty("mediaClockwiseRotation");
    DetectedObjectProperty DETECTED_OBJECT = new DetectedObjectProperty("detectedObject");
    StringSingleValueBcProperty MEDIA_VIDEO_FORMAT = new StringSingleValueBcProperty("mediaVideoFormat");
    StringSingleValueBcProperty MEDIA_AUDIO_FORMAT = new StringSingleValueBcProperty("mediaAudioFormat");
    StringSingleValueBcProperty MEDIA_VIDEO_CODEC = new StringSingleValueBcProperty("mediaVideoCodec");
    StringSingleValueBcProperty MEDIA_AUDIO_CODEC = new StringSingleValueBcProperty("mediaAudioCodec");
}
