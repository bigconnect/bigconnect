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

import com.google.common.collect.ImmutableSet;
import com.mware.core.model.properties.types.*;

import java.util.Set;

public class SchemaProperties {
    public static final BooleanSingleValueBcProperty CORE_CONCEPT = new BooleanSingleValueBcProperty("coreConcept");

    public static final StringSingleValueBcProperty TITLE = new StringSingleValueBcProperty("title");
    public static final IntegerSingleValueBcProperty DEPENDENT_PROPERTY_ORDER_PROPERTY_NAME = new IntegerSingleValueBcProperty("order");
    public static final StringBcProperty TEXT_INDEX_HINTS = new StringBcProperty("textIndexHints");
    public static final StringSingleValueBcProperty ONTOLOGY_TITLE = new StringSingleValueBcProperty("ontologyTitle");
    public static final StringSingleValueBcProperty DISPLAY_NAME = new StringSingleValueBcProperty("dispayName");
    public static final StringSingleValueBcProperty DISPLAY_TYPE = new StringSingleValueBcProperty("displayType");
    public static final BooleanSingleValueBcProperty USER_VISIBLE = new BooleanSingleValueBcProperty("userVisible");
    public static final StringSingleValueBcProperty GLYPH_ICON_FILE_NAME = new StringSingleValueBcProperty("glyphIconFileName");
    public static final StreamingSingleValueBcProperty GLYPH_ICON = new StreamingSingleValueBcProperty("glyphIcon");
    public static final StringSingleValueBcProperty GLYPH_ICON_SELECTED_FILE_NAME = new StringSingleValueBcProperty("glyphIconSelectedFileName");
    public static final StreamingSingleValueBcProperty GLYPH_ICON_SELECTED = new StreamingSingleValueBcProperty("glyphIconSelected");
    public static final StreamingSingleValueBcProperty MAP_GLYPH_ICON = new StreamingSingleValueBcProperty("mapGlyphIcon");
    public static final StringSingleValueBcProperty MAP_GLYPH_ICON_FILE_NAME = new StringSingleValueBcProperty("mapGlyphIconFileName");
    public static final JsonArraySingleValueBcProperty ADD_RELATED_CONCEPT_WHITE_LIST = new JsonArraySingleValueBcProperty("addRelatedConceptWhiteList");
    public static final StringBcProperty INTENT = new StringBcProperty("intent");
    public static final BooleanSingleValueBcProperty SEARCHABLE = new BooleanSingleValueBcProperty("searchable");
    public static final BooleanSingleValueBcProperty SORTABLE = new BooleanSingleValueBcProperty("sortable");
    public static final BooleanSingleValueBcProperty ADDABLE = new BooleanSingleValueBcProperty("addable");
    public static final StringSingleValueBcProperty DISPLAY_FORMULA = new StringSingleValueBcProperty("displayFormula");
    public static final StringSingleValueBcProperty PROPERTY_GROUP = new StringSingleValueBcProperty("propertyGroup");
    public static final StringSingleValueBcProperty VALIDATION_FORMULA = new StringSingleValueBcProperty("validationFormula");
    public static final StringSingleValueBcProperty TIME_FORMULA = new StringSingleValueBcProperty("timeFormula");
    public static final StringSingleValueBcProperty TITLE_FORMULA = new StringSingleValueBcProperty("titleFormula");
    public static final StringSingleValueBcProperty SUBTITLE_FORMULA = new StringSingleValueBcProperty("subtitleFormula");
    public static final StringSingleValueBcProperty COLOR = new StringSingleValueBcProperty("color");
    public static final StringSingleValueBcProperty DATA_TYPE = new StringSingleValueBcProperty("dataType");
    public static final DoubleSingleValueBcProperty BOOST = new DoubleSingleValueBcProperty("boost");
    public static final JsonSingleValueBcProperty POSSIBLE_VALUES = new JsonSingleValueBcProperty("possibleValues");
    public static final BooleanSingleValueBcProperty DELETEABLE = new BooleanSingleValueBcProperty("deletable");
    public static final BooleanSingleValueBcProperty UPDATEABLE = new BooleanSingleValueBcProperty("updatable");
    public static final IntegerSingleValueBcProperty SORT_PRIORITY = new IntegerSingleValueBcProperty("sortPriority");

    public static final BooleanSingleValueBcProperty SEARCH_FACET = new BooleanSingleValueBcProperty("searchFacet");
    public static final BooleanSingleValueBcProperty SYSTEM_PROPERTY = new BooleanSingleValueBcProperty("systemProperty");
    public static final StringSingleValueBcProperty AGG_TYPE = new StringSingleValueBcProperty("aggType");
    public static final IntegerSingleValueBcProperty AGG_PRECISION = new IntegerSingleValueBcProperty("aggPrecision");
    public static final StringSingleValueBcProperty AGG_INTERVAL = new StringSingleValueBcProperty("aggInterval");
    public static final LongSingleValueBcProperty AGG_MIN_DOCUMENT_COUNT = new LongSingleValueBcProperty("aggMinDocumentCount");
    public static final StringSingleValueBcProperty AGG_TIMEZONE = new StringSingleValueBcProperty("aggTimeZone");
    public static final StringSingleValueBcProperty AGG_CALENDAR_FIELD = new StringSingleValueBcProperty("aggCalendarField");

    public static final Set<String> CHANGEABLE_PROPERTY_NAME = ImmutableSet.of(
            DISPLAY_TYPE.getPropertyName(),
            USER_VISIBLE.getPropertyName(),
            DELETEABLE.getPropertyName(),
            UPDATEABLE.getPropertyName(),
            ADDABLE.getPropertyName(),
            SORTABLE.getPropertyName(),
            SEARCHABLE.getPropertyName(),
            INTENT.getPropertyName(),
            POSSIBLE_VALUES.getPropertyName(),
            COLOR.getPropertyName(),
            SUBTITLE_FORMULA.getPropertyName(),
            TIME_FORMULA.getPropertyName(),
            TITLE_FORMULA.getPropertyName(),
            VALIDATION_FORMULA.getPropertyName(),
            PROPERTY_GROUP.getPropertyName(),
            DISPLAY_FORMULA.getPropertyName(),
            GLYPH_ICON_FILE_NAME.getPropertyName(),
            GLYPH_ICON.getPropertyName(),
            GLYPH_ICON_SELECTED_FILE_NAME.getPropertyName(),
            GLYPH_ICON_SELECTED.getPropertyName(),
            MAP_GLYPH_ICON.getPropertyName(),
            MAP_GLYPH_ICON_FILE_NAME.getPropertyName(),
            ADD_RELATED_CONCEPT_WHITE_LIST.getPropertyName()
    );
}
