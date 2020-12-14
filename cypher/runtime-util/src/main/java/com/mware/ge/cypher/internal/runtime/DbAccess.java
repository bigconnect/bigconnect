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
package com.mware.ge.cypher.internal.runtime;

import com.mware.ge.values.storable.Value;
import com.mware.ge.values.virtual.ListValue;
import com.mware.ge.values.virtual.MapValue;
import com.mware.ge.values.virtual.NodeValue;
import com.mware.ge.values.virtual.RelationshipValue;

/**
 * Used to expose db access to expressions
 */
public interface DbAccess extends EntityById {

    Value nodeProperty(String node, String property);

    String[] nodePropertyIds(String node);

    String propertyKey(String name);

    String nodeLabel(String name);

    String relationshipType(String name);

    boolean nodeHasProperty(String node, String property);

    Value relationshipProperty(String node, String property);

    String[] relationshipPropertyIds(String node);

    boolean relationshipHasProperty(String node, String property);

    int nodeGetOutgoingDegree(String node);

    int nodeGetOutgoingDegree(String node, String relationship);

    int nodeGetIncomingDegree(String node);

    int nodeGetIncomingDegree(String node, String relationship);

    int nodeGetTotalDegree(String node);

    int nodeGetTotalDegree(String node, String relationship);

    NodeValue relationshipGetStartNode(RelationshipValue relationship);

    NodeValue relationshipGetEndNode(RelationshipValue relationship);

    ListValue getLabelsForNode(String id);

    boolean isLabelSetOnNode(String label, String id);

    String getPropertyKeyName(String token);

    MapValue nodeAsMap(String id);

    MapValue relationshipAsMap(String id);

}
