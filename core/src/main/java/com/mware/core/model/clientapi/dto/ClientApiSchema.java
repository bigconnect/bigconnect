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
package com.mware.core.model.clientapi.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class ClientApiSchema implements ClientApiObject {
    private List<Concept> concepts = new ArrayList<Concept>();
    private List<Property> properties = new ArrayList<Property>();
    private List<Relationship> relationships = new ArrayList<Relationship>();

    public List<Concept> getConcepts() {
        return concepts;
    }

    public List<Property> getProperties() {
        return properties;
    }

    public List<Relationship> getRelationships() {
        return relationships;
    }

    public void addAllConcepts(Collection<Concept> concepts) {
        this.concepts.addAll(concepts);
    }

    public void addAllProperties(Collection<Property> properties) {
        this.properties.addAll(properties);
    }

    public void addAllRelationships(Collection<Relationship> relationships) {
        this.relationships.addAll(relationships);
    }

    public ClientApiSchema merge(
            Collection<Concept> mergeConcepts,
            Collection<Property> mergeProperties,
            Collection<Relationship> mergeRelationships) {
        ClientApiSchema copy = new ClientApiSchema();

        mergeCollections(copy.getConcepts(), concepts, mergeConcepts);
        mergeCollections(copy.getProperties(), properties, mergeProperties);
        mergeCollections(copy.getRelationships(), relationships, mergeRelationships);

        return copy;
    }

    private <T extends SchemaId> void mergeCollections(Collection<T> newList, Collection<T> old, Collection<T> merge) {
        if (merge == null || merge.size() == 0) {
            newList.addAll(old);
        } else {
            List<T> unmerged = new ArrayList<T>();
            unmerged.addAll(merge);
            for (T existing : old) {
                String existingName = existing.getTitle();
                T update = existing;

                for (T unmergedObject : unmerged) {
                    if (unmergedObject.getTitle().equals(existingName)) {
                        update = unmergedObject;
                        unmerged.remove(unmergedObject);
                        break;
                    }
                }
                newList.add(update);
            }
            newList.addAll(unmerged);
        }
    }

    interface SchemaId {
        String getTitle();
    }

    public static class Concept implements ClientApiObject, SchemaId {

        private String[] conceptClassAttibutes = new String[] {
                "id","title","displayName","displayType","titleFormula","subtitleFormula","timeFormula",
                "parentConcept","pluralDisplayName","userVisible","analyticsVisible","searchable", "glyphIconHref",
                "glyphIconSelectedHref", "color", "deleteable","updateable", "coreConcept"};

        private Boolean coreConcept;
        private String id;
        private String title;
        private String displayName;
        private String displayType;
        private String titleFormula;
        private String subtitleFormula;
        private String timeFormula;
        private String parentConcept;
        private String pluralDisplayName;
        private Boolean userVisible;
        private Boolean analyticsVisible;
        private Boolean searchable;
        private String glyphIconHref;
        private String glyphIconSelectedHref;
        private String color;
        private Boolean deleteable;
        private Boolean updateable;
        private List<String> intents = new ArrayList<String>();
        private List<String> addRelatedConceptWhiteList = new ArrayList<String>();
        private List<String> properties = new ArrayList<String>();
        private Map<String, String> metadata = new HashMap<String, String>();
        private SandboxStatus sandboxStatus;

        public JSONObject toJson() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            JSONObject json = new JSONObject();

            for (String conceptAttribute: conceptClassAttibutes) {
                json.put(conceptAttribute, this.getClass().getMethod("get" + conceptAttribute.substring(0, 1).toUpperCase() + conceptAttribute.substring(1),new Class[] {}).invoke(this));
            }

            return json;
        }

        public Boolean getCoreConcept() {
            return coreConcept;
        }

        public void setCoreConcept(Boolean coreConcept) {
            this.coreConcept = coreConcept;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        public String getDisplayType() {
            return displayType;
        }

        public void setDisplayType(String displayType) {
            this.displayType = displayType;
        }

        public String getTitleFormula() {
            return titleFormula;
        }

        public void setTitleFormula(String titleFormula) {
            this.titleFormula = titleFormula;
        }

        public String getSubtitleFormula() {
            return subtitleFormula;
        }

        public void setSubtitleFormula(String subtitleFormula) {
            this.subtitleFormula = subtitleFormula;
        }

        public String getTimeFormula() {
            return timeFormula;
        }

        public void setTimeFormula(String timeFormula) {
            this.timeFormula = timeFormula;
        }

        public String getParentConcept() {
            return parentConcept;
        }

        public void setParentConcept(String parentConcept) {
            this.parentConcept = parentConcept;
        }

        public String getPluralDisplayName() {
            return pluralDisplayName;
        }

        public void setPluralDisplayName(String pluralDisplayName) {
            this.pluralDisplayName = pluralDisplayName;
        }

        public Boolean getUserVisible() {
            return userVisible;
        }

        public void setUserVisible(Boolean userVisible) {
            this.userVisible = userVisible;
        }

        public Boolean getAnalyticsVisible() {
            return analyticsVisible;
        }

        public void setAnalyticsVisible(boolean analyticsVisible) {
            this.analyticsVisible = analyticsVisible;
        }

        public Boolean getSearchable() {
            return searchable;
        }

        public void setSearchable(Boolean searchable) {
            this.searchable = searchable;
        }

        public String getGlyphIconHref() {
            return glyphIconHref;
        }

        public void setGlyphIconHref(String glyphIconHref) {
            this.glyphIconHref = glyphIconHref;
        }

        public String getGlyphIconSelectedHref() {
            return glyphIconSelectedHref;
        }

        public void setGlyphIconSelectedHref(String glyphIconSelectedHref) {
            this.glyphIconSelectedHref = glyphIconSelectedHref;
        }

        public String getColor() {
            return color;
        }

        public void setColor(String color) {
            this.color = color;
        }

        public Boolean getUpdateable() {
            return updateable;
        }

        public void setUpdateable(Boolean updateable) {
            this.updateable = updateable;
        }

        public Boolean getDeleteable() {
            return deleteable;
        }

        public void setDeleteable(Boolean deleteable) {
            this.deleteable = deleteable;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public Map<String, String> getMetadata() {
            return metadata;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> getAddRelatedConceptWhiteList() {
            return addRelatedConceptWhiteList;
        }

        public List<String> getProperties() {
            return properties;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> getIntents() {
            return intents;
        }

        public SandboxStatus getSandboxStatus() {
            return sandboxStatus;
        }

        public void setSandboxStatus(SandboxStatus sandboxStatus) {
            this.sandboxStatus = sandboxStatus;
        }
    }

    public static class Property implements ClientApiObject, SchemaId {
        private String[] propertyClassAttibutes = new String[] {
                "title","displayName","displayType","userVisible","analyticsVisible","searchable", "addable",
                "sortable", "aggTimeZone","aggCalendarField", "searchFacet", "aggType", "aggPrecision", "aggInterval",
                "aggMinDocumentCount", "propertyGroup", "systemProperty",
                "validationFormula", "displayFormula", "deleteable", "updateable"};

        private String title;
        private String displayName;
        private boolean userVisible;
        private boolean analyticsVisible;
        private boolean searchFacet;
        private boolean systemProperty;
        private String aggType;
        private int aggPrecision;
        private String aggInterval;
        private long aggMinDocumentCount;
        private String aggTimeZone;
        private String aggCalendarField;
        private boolean searchable;
        private boolean addable;
        private boolean sortable;
        private Integer sortPriority;
        private PropertyType dataType;
        private String displayType;
        private String propertyGroup;
        private Map<String, String> possibleValues = new HashMap<String, String>();
        private String validationFormula;
        private String displayFormula;
        private String[] dependentPropertyIris;
        private boolean deleteable;
        private boolean updateable;
        private List<String> intents = new ArrayList<String>();
        private List<String> textIndexHints = new ArrayList<String>();
        private Map<String, String> metadata = new HashMap<>();
        private SandboxStatus sandboxStatus;

        public JSONObject toJson() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            JSONObject json = new JSONObject();

            for (String conceptAttribute: propertyClassAttibutes) {
                json.put(conceptAttribute, this.getClass().getMethod("get" + conceptAttribute.substring(0, 1).toUpperCase() + conceptAttribute.substring(1),new Class[] {}).invoke(this));
            }
            json.put("dataType",dataType.toString());

            JSONArray possibleValuesJSON = new JSONArray();
            for (Map.Entry<String,String> possibleValue : possibleValues.entrySet()) {
                JSONObject possibleValueJSON = new JSONObject();
                possibleValueJSON.put("value", possibleValue.getKey());
                possibleValueJSON.put("displayName", possibleValue.getValue());
                possibleValuesJSON.put(possibleValueJSON);
            }
            json.put("possibleValues", possibleValuesJSON);

            JSONArray dependentPropertiesJSON = new JSONArray();
            if (dependentPropertyIris!=null) {
                for (String dependentProperty : dependentPropertyIris) {
                    possibleValuesJSON.put(dependentProperty);
                }
                json.put("dependentProperties", dependentPropertiesJSON);
            }

            JSONArray intentsJSON = new JSONArray();
            if (intents!=null) {
                for (String intent : intents) {
                    possibleValuesJSON.put(intent);
                }
                json.put("intents", intentsJSON);
            }

            JSONArray textIndexHintsJSON = new JSONArray();
            if (textIndexHints!=null) {
                for (String hint : textIndexHints) {
                    textIndexHintsJSON.put(hint);
                }
                json.put("textIndexHints", textIndexHintsJSON);
            }


            return json;
        }


        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        public boolean isAnalyticsVisible() {
            return analyticsVisible;
        }
        public boolean getAnalyticsVisible() {
            return analyticsVisible;
        }

        public void setAnalyticsVisible(boolean analyticsVisible) {
            this.analyticsVisible = analyticsVisible;
        }

        public boolean isSearchFacet() {
            return searchFacet;
        }
        public boolean getSearchFacet() {
            return searchFacet;
        }

        public void setSearchFacet(boolean searchFacet) {
            this.searchFacet = searchFacet;
        }

        public boolean isSystemProperty() {
            return systemProperty;
        }

        public boolean getSystemProperty() {
            return systemProperty;
        }

        public void setSystemProperty(boolean systemProperty) {
            this.systemProperty = systemProperty;
        }

        public String getAggType() {
            return aggType;
        }

        public void setAggType(String aggType) {
            this.aggType = aggType;
        }

        public int getAggPrecision() {
            return aggPrecision;
        }

        public void setAggPrecision(int aggPrecision) {
            this.aggPrecision = aggPrecision;
        }

        public String getAggInterval() {
            return aggInterval;
        }

        public void setAggInterval(String aggInterval) {
            this.aggInterval = aggInterval;
        }

        public long getAggMinDocumentCount() {
            return aggMinDocumentCount;
        }

        public void setAggMinDocumentCount(long aggMinDocumentCount) {
            this.aggMinDocumentCount = aggMinDocumentCount;
        }

        public String getAggTimeZone() {
            return aggTimeZone;
        }

        public void setAggTimeZone(String aggTimeZone) {
            this.aggTimeZone = aggTimeZone;
        }

        public String getAggCalendarField() {
            return aggCalendarField;
        }

        public void setAggCalendarField(String aggCalendarField) {
            this.aggCalendarField = aggCalendarField;
        }

        public boolean isUserVisible() {
            return userVisible;
        }
        public boolean getUserVisible() {
            return userVisible;
        }

        public void setUserVisible(boolean userVisible) {
            this.userVisible = userVisible;
        }

        public boolean isSearchable() {
            return searchable;
        }
        public boolean getSearchable() {
            return searchable;
        }

        public void setSearchable(boolean searchable) {
            this.searchable = searchable;
        }

        public boolean isAddable() {
            return addable;
        }
        public boolean getAddable() {
            return addable;
        }

        public void setAddable(boolean addable) {
            this.addable = addable;
        }

        public boolean isSortable() {
            return sortable;
        }
        public boolean getSortable() {
            return sortable;
        }

        public void setSortable(boolean sortable) {
            this.sortable = sortable;
        }

        public Integer getSortPriority() {
            return sortPriority;
        }

        public void setSortPriority(Integer sortPriority) {
            this.sortPriority = sortPriority;
        }

        public boolean isUpdateable() {
            return updateable;
        }
        public boolean getUpdateable() {
            return updateable;
        }

        public void setUpdateable(boolean updateable) {
            this.updateable = updateable;
        }

        public boolean isDeleteable() {
            return deleteable;
        }
        public boolean getDeleteable() {
            return deleteable;
        }

        public void setDeleteable(boolean deleteable) {
            this.deleteable = deleteable;
        }

        public PropertyType getDataType() {
            return dataType;
        }

        public void setDataType(PropertyType dataType) {
            this.dataType = dataType;
        }

        public String getDisplayType() {
            return displayType;
        }

        public void setDisplayType(String displayType) {
            this.displayType = displayType;
        }

        public String getPropertyGroup() {
            return propertyGroup;
        }

        public void setPropertyGroup(String propertyGroup) {
            this.propertyGroup = propertyGroup;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public Map<String, String> getPossibleValues() {
            return possibleValues;
        }

        public void setValidationFormula(String validationFormula) {
            this.validationFormula = validationFormula;
        }

        public String getValidationFormula() {
            return validationFormula;
        }

        public void setDisplayFormula(String displayFormula) {
            this.displayFormula = displayFormula;
        }

        public String getDisplayFormula() {
            return displayFormula;
        }

        @JsonSetter
        public void setDependentPropertyNames(String[] dependentPropertyIris) {
            this.dependentPropertyIris = dependentPropertyIris;
        }

        public void setDependentPropertyNames(Collection<String> dependentPropertyIris) {
            if (dependentPropertyIris == null || dependentPropertyIris.size() == 0) {
                this.dependentPropertyIris = null;
            } else {
                this.dependentPropertyIris = dependentPropertyIris.toArray(new String[dependentPropertyIris.size()]);
            }
        }

        public String[] getDependentPropertyIris() {
            return dependentPropertyIris;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public Map<String, String> getMetadata() {
            return metadata;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> getIntents() {
            return intents;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> getTextIndexHints() {
            return textIndexHints;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public void setTextIndexHints(List<String> textIndexHints) {
            this.textIndexHints = textIndexHints;
        }

        public SandboxStatus getSandboxStatus() {
            return sandboxStatus;
        }

        public void setSandboxStatus(SandboxStatus sandboxStatus) {
            this.sandboxStatus = sandboxStatus;
        }
    }

    public static class ExtendedDataTableProperty extends Property {
        private String titleFormula;
        private String subtitleFormula;
        private String timeFormula;
        private List<String> tablePropertyIris = new ArrayList<String>();

        public String getTitleFormula() {
            return titleFormula;
        }

        public void setTitleFormula(String titleFormula) {
            this.titleFormula = titleFormula;
        }

        public String getSubtitleFormula() {
            return subtitleFormula;
        }

        public void setSubtitleFormula(String subtitleFormula) {
            this.subtitleFormula = subtitleFormula;
        }

        public String getTimeFormula() {
            return timeFormula;
        }

        public void setTimeFormula(String timeFormula) {
            this.timeFormula = timeFormula;
        }

        public void setTablePropertyNames(List<String> tablePropertyIris) {
            this.tablePropertyIris.clear();
            this.tablePropertyIris.addAll(tablePropertyIris);
        }

        public List<String> getTablePropertyIris() {
            return tablePropertyIris;
        }
    }

    public static class Relationship implements ClientApiObject, SchemaId {
        private String[] relationshipClassAttibutes = new String[] {
                "parentIri","title","displayName","userVisible","analyticsVisible",
                "timeFormula","color","updateable", "deleteable", "titleFormula", "subtitleFormula", "coreConcept"};

        private String parentIri;
        private String title;
        private String displayName;
        private Boolean userVisible;
        private Boolean analyticsVisible;
        private Boolean updateable;
        private Boolean deleteable;
        private String titleFormula;
        private String subtitleFormula;
        private String timeFormula;
        private String color;
        private Boolean coreConcept;
        private List<String> domainConceptIris = new ArrayList<String>();
        private List<String> rangeConceptIris = new ArrayList<String>();
        private List<InverseOf> inverseOfs = new ArrayList<InverseOf>();
        private List<String> intents = new ArrayList<String>();
        private List<String> properties = new ArrayList<String>();
        private Map<String, String> metadata = new HashMap<>();
        private SandboxStatus sandboxStatus;

        public JSONObject toJson() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            JSONObject json = new JSONObject();

            for (String relationshipProperty: relationshipClassAttibutes) {
                json.put(relationshipProperty, this.getClass().getMethod("get" + relationshipProperty.substring(0, 1).toUpperCase() + relationshipProperty.substring(1),new Class[] {}).invoke(this));
            }

            JSONArray domainConceptsJSON = new JSONArray();
            for (String domainConceptIri : domainConceptIris) {
                JSONObject domainConceptJSON = new JSONObject();
                domainConceptJSON.put("IRI", domainConceptIri);
                domainConceptsJSON.put(domainConceptJSON);
            }
            json.put("domainConcepts", domainConceptsJSON);

            JSONArray rangeConceptsJSON = new JSONArray();
            for (String rangeConceptIri : rangeConceptIris) {
                JSONObject rangeConceptJSON = new JSONObject();
                rangeConceptJSON.put("IRI", rangeConceptIri);
                rangeConceptsJSON.put(rangeConceptJSON);
            }
            json.put("rangeConcepts", rangeConceptsJSON);

            JSONArray intentsJSON = new JSONArray();
            for (String intentIRI : intents) {
                JSONObject intentJSON = new JSONObject();
                intentJSON.put("intent", intentIRI);
                intentsJSON.put(intentJSON);
            }
            json.put("intents", intentsJSON);

            JSONArray propertiesJSON = new JSONArray();
            for (String propertyIRI : properties) {
                JSONObject propertyJSON = new JSONObject();
                propertyJSON.put("IRI", propertyIRI);
                propertiesJSON.put(propertyJSON);
            }
            json.put("properties", propertiesJSON);

            if (inverseOfs != null && !inverseOfs.isEmpty()) {
                for (InverseOf io : inverseOfs) {
                    json.put("inverseOf", io.getIri());
                }
            }

            return json;
        }


        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getParentIri() {
            return parentIri;
        }

        public void setParentName(String parentIri) {
            this.parentIri = parentIri;
        }

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        public List<String> getDomainConceptIris() {
            return domainConceptIris;
        }

        public void setDomainConceptNames(List<String> domainConceptIris) {
            this.domainConceptIris = domainConceptIris;
        }

        public List<String> getRangeConceptIris() {
            return rangeConceptIris;
        }

        public void setRangeConceptNames(List<String> rangeConceptIris) {
            this.rangeConceptIris = rangeConceptIris;
        }

        public Boolean getAnalyticsVisible() {
            return analyticsVisible;
        }

        public void setAnalyticVisible(Boolean analyticsVisible) {
            this.analyticsVisible = analyticsVisible;
        }

        public Boolean getUserVisible() {
            return userVisible;
        }

        public void setUserVisible(Boolean userVisible) {
            this.userVisible = userVisible;
        }

        public Boolean getUpdateable() {
            return updateable;
        }

        public void setUpdateable(Boolean updateable) {
            this.updateable = updateable;
        }

        public Boolean getDeleteable() {
            return deleteable;
        }

        public void setDeleteable(Boolean deleteable) {
            this.deleteable = deleteable;
        }

        public List<String> getProperties() {
            return properties;
        }

        public void setProperties(List<String> properties) {
            this.properties = properties;
        }

        public String getTitleFormula() {
            return titleFormula;
        }

        public void setTitleFormula(String titleFormula) {
            this.titleFormula = titleFormula;
        }

        public String getSubtitleFormula() {
            return subtitleFormula;
        }

        public void setSubtitleFormula(String subtitleFormula) {
            this.subtitleFormula = subtitleFormula;
        }

        public String getTimeFormula() {
            return timeFormula;
        }

        public void setTimeFormula(String timeFormula) {
            this.timeFormula = timeFormula;
        }

        public void setCoreConcept(Boolean coreConcept) {
            this.coreConcept = coreConcept;
        }

        public Boolean getCoreConcept() {
            return coreConcept;
        }

        public String getColor() {
            return color;
        }

        public void setColor(String color) {
            this.color = color;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<InverseOf> getInverseOfs() {
            return inverseOfs;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> getIntents() {
            return intents;
        }

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public Map<String, String> getMetadata() {
            return metadata;
        }

        public SandboxStatus getSandboxStatus() {
            return sandboxStatus;
        }

        public void setSandboxStatus(SandboxStatus sandboxStatus) {
            this.sandboxStatus = sandboxStatus;
        }

        public static class InverseOf {
            private String iri;
            private String primaryIri;

            public String getIri() {
                return iri;
            }

            public void setName(String iri) {
                this.iri = iri;
            }

            public String getPrimaryIri() {
                return primaryIri;
            }

            public void setPrimaryName(String primaryIri) {
                this.primaryIri = primaryIri;
            }
        }
    }
}
