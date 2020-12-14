/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.mware.ge.cypher.schema;

import com.mware.ge.ElementType;
import org.apache.commons.lang3.ArrayUtils;
import com.mware.ge.cypher.TokenNameLookup;

import java.util.Arrays;

public class LabelSchemaDescriptorImpl implements LabelSchemaDescriptor {
    private final String labelId;
    private final String[] propertyIds;

    LabelSchemaDescriptorImpl(String labelId, String... propertyIds) {
        this.labelId = labelId;
        this.propertyIds = propertyIds;
    }

    @Override
    public boolean isAffected(String[] entityTokenIds) {
        return ArrayUtils.contains(entityTokenIds, labelId);
    }

    @Override
    public <R> R computeWith(SchemaComputer<R> processor) {
        return processor.computeSpecific(this);
    }

    @Override
    public void processWith(SchemaProcessor processor) {
        processor.processSpecific(this);
    }

    @Override
    public String userDescription(TokenNameLookup tokenNameLookup) {
        return String.format(":%s(%s)", tokenNameLookup.labelGetName(labelId),
                SchemaUtil.niceProperties(tokenNameLookup, propertyIds));
    }

    @Override
    public String getLabelId() {
        return labelId;
    }

    @Override
    public String keyId() {
        return getLabelId();
    }

    @Override
    public ElementType entityType() {
        return ElementType.VERTEX;
    }

    @Override
    public PropertySchemaType propertySchemaType() {
        return PropertySchemaType.COMPLETE_ALL_TOKENS;
    }

    @Override
    public String[] getPropertyIds() {
        return propertyIds;
    }

    @Override
    public String[] getEntityTokenIds() {
        return new String[]{labelId};
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LabelSchemaDescriptorImpl) {
            LabelSchemaDescriptorImpl that = (LabelSchemaDescriptorImpl) o;
            return labelId == that.getLabelId() && Arrays.equals(propertyIds, that.getPropertyIds());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(propertyIds) + 31 * labelId.hashCode();
    }

    @Override
    public String toString() {
        return "LabelSchemaDescriptor( " + userDescription(SchemaUtil.idTokenNameLookup) + " )";
    }

    @Override
    public LabelSchemaDescriptorImpl schema() {
        return this;
    }
}
