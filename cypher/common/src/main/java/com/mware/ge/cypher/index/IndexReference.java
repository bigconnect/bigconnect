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
package com.mware.ge.cypher.index;

import com.mware.ge.cypher.schema.SchemaDescriptor;
import com.mware.ge.values.storable.ValueCategory;

/**
 * Reference to a specific index together with it's capabilities. This reference is valid until the schema of the database changes
 * (that is a create/drop of an index or constraint occurs).
 */
public interface IndexReference extends IndexCapability {
    String UNNAMED_INDEX = "Unnamed index";

    /**
     * Returns true if this index only allows one value per key.
     */
    boolean isUnique();

    /**
     * Returns the propertyKeyIds associated with this index.
     */
    String[] properties();

    SchemaDescriptor schema();

    /**
     * @return a user friendly description of what this index indexes.
     */
    String userDescription();


    /**
     * The unique name for this index - either automatically generated or user supplied - or the {@link #UNNAMED_INDEX} constant.
     */
    String name();


    IndexReference NO_INDEX = new IndexReference() {
        @Override
        public IndexOrder[] orderCapability(ValueCategory... valueCategories) {
            return NO_CAPABILITY.orderCapability(valueCategories);
        }

        @Override
        public IndexValueCapability valueCapability(ValueCategory... valueCategories) {
            return NO_CAPABILITY.valueCapability(valueCategories);
        }

        @Override
        public boolean isFulltextIndex() {
            return false;
        }

        @Override
        public boolean isEventuallyConsistent() {
            return false;
        }

        @Override
        public boolean isUnique() {
            return false;
        }

        @Override
        public String[] properties() {
            return new String[0];
        }

        @Override
        public String name() {
            return UNNAMED_INDEX;
        }

        @Override
        public SchemaDescriptor schema() {
            return SchemaDescriptor.NO_SCHEMA;
        }

        @Override
        public String userDescription() {
            return "";
        }
    };
}
