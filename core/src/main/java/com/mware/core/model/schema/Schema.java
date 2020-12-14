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

import com.mware.core.model.clientapi.dto.SandboxStatus;

import java.util.*;
import java.util.stream.Collectors;

import static com.mware.core.util.StreamUtil.stream;

public class Schema {
    private final String namespace;
    private final Map<String, Concept> conceptsByName;
    private final Map<String, Relationship> relationshipsByName;
    private final Map<String, ExtendedDataTableProperty> extendedDataTablesByName;
    private final Map<String, SchemaProperty> propertiesByName;

    public Schema(
            Iterable<Concept> concepts,
            Iterable<Relationship> relationships,
            Iterable<ExtendedDataTableProperty> extendedDataTables,
            Map<String, SchemaProperty> propertiesByName,
            String namespace
    ) {
        this.namespace = namespace;

        Map<String, SchemaProperty> propertyMap = new HashMap<>();

        conceptsByName = Collections.unmodifiableMap(stream(concepts)
                .collect(Collectors.toMap(Concept::getName, concept -> {
                    Collection<SchemaProperty> properties = concept.getProperties();
                    if (properties != null && properties.size() > 0) {
                        properties.forEach(property -> propertyMap.put(property.getName(), property));
                    }
                    return concept;
                })));
        relationshipsByName = Collections.unmodifiableMap(stream(relationships)
                .collect(Collectors.toMap(Relationship::getName, relationship -> {
                    Collection<SchemaProperty> properties = relationship.getProperties();
                    if (properties != null && properties.size() > 0) {
                        properties.forEach(property -> propertyMap.put(property.getName(), property));
                    }
                    return relationship;
                })));
        extendedDataTablesByName = Collections.unmodifiableMap(stream(extendedDataTables)
                .collect(Collectors.toMap(ExtendedDataTableProperty::getName, table -> {
                            List<SchemaProperty> properties = stream(table.getTablePropertyNames())
                                    .map(propertiesByName::get)
                                    .collect(Collectors.toList());
                            properties.forEach(property -> propertyMap.put(property.getName(), property));
                            return table;
                })));

        this.propertiesByName = Collections.unmodifiableMap(propertyMap);
    }

    public String getNamespace() {
        return namespace;
    }

    public Collection<Concept> getConcepts() {
        return conceptsByName.values();
    }

    public Map<String, Concept> getConceptsByName() {
        return conceptsByName;
    }

    public Concept getConceptByName(String name) {
        return conceptsByName.get(name);
    }

    public Collection<Relationship> getRelationships() {
        return relationshipsByName.values();
    }

    public Map<String, Relationship> getRelationshipsByName() {
        return relationshipsByName;
    }

    public Relationship getRelationshipByName(String name) {
        return relationshipsByName.get(name);
    }

    public Collection<SchemaProperty> getProperties() {
        return propertiesByName.values();
    }

    public Map<String, SchemaProperty> getPropertiesByName() {
        return propertiesByName;
    }

    public SchemaProperty getPropertyByName(String name) {
        return propertiesByName.get(name);
    }

    public Map<String, ExtendedDataTableProperty> getExtendedDataTablesByName() {
        return extendedDataTablesByName;
    }

    public SandboxStatus getSandboxStatus() {
        for (Concept concept : getConcepts()) {
            SandboxStatus sandboxStatus = concept.getSandboxStatus();
            if (sandboxStatus != SandboxStatus.PUBLIC) {
                return sandboxStatus;
            }
        }

        for (Relationship relationship : getRelationships()) {
            SandboxStatus sandboxStatus = relationship.getSandboxStatus();
            if (sandboxStatus != SandboxStatus.PUBLIC) {
                return sandboxStatus;
            }
        }

        for (SchemaProperty property : getProperties()) {
            SandboxStatus sandboxStatus = property.getSandboxStatus();
            if (sandboxStatus != SandboxStatus.PUBLIC) {
                return sandboxStatus;
            }
        }

        return SandboxStatus.PUBLIC;
    }
}
