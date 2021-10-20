/*
 * Copyright 2021 BigConnect Authors
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.bigconnect.biggraph.backend.query;

import io.bigconnect.biggraph.backend.BackendException;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.id.SplicingIdGenerator;
import io.bigconnect.biggraph.backend.query.Condition.Relation;
import io.bigconnect.biggraph.backend.query.Condition.RelationType;
import io.bigconnect.biggraph.perf.PerfUtil.Watched;
import io.bigconnect.biggraph.structure.BigElement;
import io.bigconnect.biggraph.structure.BigProperty;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.CollectionType;
import io.bigconnect.biggraph.type.define.BigKeys;
import io.bigconnect.biggraph.util.E;
import io.bigconnect.biggraph.util.LongEncoding;
import io.bigconnect.biggraph.util.NumericUtil;
import io.bigconnect.biggraph.util.collection.CollectionFactory;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.util.*;

public final class ConditionQuery extends IdQuery {

    private static final Set<Condition> EMPTY_CONDITIONS = ImmutableSet.of();

    // Conditions will be concated with `and` by default
    private Set<Condition> conditions = EMPTY_CONDITIONS;

    private OptimizedType optimizedType = OptimizedType.NONE;
    private Function<BigElement, Boolean> resultsFilter = null;
    private Element2IndexValueMap element2IndexValueMap = null;

    public ConditionQuery(BigType resultType) {
        super(resultType);
    }

    public ConditionQuery(BigType resultType, Query originQuery) {
        super(resultType, originQuery);
    }

    public ConditionQuery query(Condition condition) {
        // Query by id (HugeGraph-259)
        if (condition instanceof Relation) {
            Relation relation = (Relation) condition;
            if (relation.key().equals(BigKeys.ID) &&
                relation.relation() == RelationType.EQ) {
                E.checkArgument(relation.value() instanceof Id,
                                "Invalid id value '%s'", relation.value());
                super.query((Id) relation.value());
                return this;
            }
        }

        if (this.conditions == EMPTY_CONDITIONS) {
            this.conditions = CollectionFactory.newSet(CollectionType.EC);
        }
        this.conditions.add(condition);
        return this;
    }

    public ConditionQuery query(List<Condition> conditions) {
        for (Condition condition : conditions) {
            this.query(condition);
        }
        return this;
    }

    public ConditionQuery eq(BigKeys key, Object value) {
        // Filter value by key
        return this.query(Condition.eq(key, value));
    }

    public ConditionQuery gt(BigKeys key, Object value) {
        return this.query(Condition.gt(key, value));
    }

    public ConditionQuery gte(BigKeys key, Object value) {
        return this.query(Condition.gte(key, value));
    }

    public ConditionQuery lt(BigKeys key, Object value) {
        return this.query(Condition.lt(key, value));
    }

    public ConditionQuery lte(BigKeys key, Object value) {
        return this.query(Condition.lte(key, value));
    }

    public ConditionQuery neq(BigKeys key, Object value) {
        return this.query(Condition.neq(key, value));
    }

    public ConditionQuery prefix(BigKeys key, Id value) {
        return this.query(Condition.prefix(key, value));
    }

    public ConditionQuery key(BigKeys key, Object value) {
        return this.query(Condition.containsKey(key, value));
    }

    public ConditionQuery scan(String start, String end) {
        return this.query(Condition.scan(start, end));
    }

    @Override
    public Set<Condition> conditions() {
        return Collections.unmodifiableSet(this.conditions);
    }

    public void resetConditions(Set<Condition> conditions) {
        this.conditions = conditions;
    }

    public void resetConditions() {
        this.conditions = new LinkedHashSet<>();
    }

    public void recordIndexValue(Id propertyId, Id id, Object indexValue) {
        this.ensureElement2IndexValueMap();
        this.element2IndexValueMap.addIndexValue(propertyId, id, indexValue);
    }

    public void selectedIndexField(Id indexField) {
        this.ensureElement2IndexValueMap();
        this.element2IndexValueMap.selectedIndexField(indexField);
    }

    public Set<LeftIndex> getElementLeftIndex(Id elementId) {
        if (this.element2IndexValueMap == null) {
            return null;
        }
        return this.element2IndexValueMap.getLeftIndex(elementId);
    }

    public void removeElementLeftIndex(Id elementId) {
        if (this.element2IndexValueMap == null) {
            return;
        }
        this.element2IndexValueMap.removeElementLeftIndex(elementId);
    }

    public boolean existLeftIndex(Id elementId) {
        return this.getElementLeftIndex(elementId) != null;
    }

    public List<Condition.Relation> relations() {
        List<Condition.Relation> relations = new ArrayList<>();
        for (Condition c : this.conditions) {
            relations.addAll(c.relations());
        }
        return relations;
    }

    public Relation relation(Id key){
        for (Relation r : this.relations()) {
            if (r.key().equals(key)) {
                return r;
            }
        }
        return null;
    }

    @Watched
    public <T> T condition(Object key) {
        List<Object> values = new ArrayList<>();
        for (Condition c : this.conditions) {
            if (c.isRelation()) {
                Condition.Relation r = (Condition.Relation) c;
                if (r.key().equals(key) && r.relation() == RelationType.EQ) {
                    values.add(r.value());
                }
            }
        }
        if (values.isEmpty()) {
            return null;
        }
        E.checkState(values.size() == 1,
                     "Illegal key '%s' with more than one value", key);
        @SuppressWarnings("unchecked")
        T value = (T) values.get(0);
        return value;
    }

    public void unsetCondition(Object key) {
        for (Iterator<Condition> iter = this.conditions.iterator();
             iter.hasNext();) {
            Condition c = iter.next();
            E.checkState(c.isRelation(), "Can't unset condition '%s'", c);
            if (((Condition.Relation) c).key().equals(key)) {
                iter.remove();
            }
        }
    }

    public boolean containsCondition(BigKeys key) {
        for (Condition c : this.conditions) {
            if (c.isRelation()) {
                Condition.Relation r = (Condition.Relation) c;
                if (r.key().equals(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean containsCondition(BigKeys key,
                                     Condition.RelationType type) {
        for (Relation r : this.relations()) {
            if (r.key().equals(key) && r.relation().equals(type)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsCondition(Condition.RelationType type) {
        for (Relation r : this.relations()) {
            if (r.relation().equals(type)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsScanCondition() {
        return this.containsCondition(Condition.RelationType.SCAN);
    }

    public boolean containsContainsCondition(Id key) {
        for (Relation r : this.relations()) {
            if (r.key().equals(key)) {
                return r.relation().equals(RelationType.CONTAINS) ||
                       r.relation().equals(RelationType.TEXT_CONTAINS);
            }
        }
        return false;
    }

    public boolean allSysprop() {
        for (Condition c : this.conditions) {
            if (!c.isSysprop()) {
                return false;
            }
        }
        return true;
    }

    public boolean allRelation() {
        for (Condition c : this.conditions) {
            if (!c.isRelation()) {
                return false;
            }
        }
        return true;
    }

    public List<Condition> syspropConditions() {
        this.checkFlattened();
        List<Condition> conds = new ArrayList<>();
        for (Condition c : this.conditions) {
            if (c.isSysprop()) {
                conds.add(c);
            }
        }
        return conds;
    }

    public List<Condition> syspropConditions(BigKeys key) {
        this.checkFlattened();
        List<Condition> conditions = new ArrayList<>();
        for (Condition condition : this.conditions) {
            Relation relation = (Relation) condition;
            if (relation.key().equals(key)) {
                conditions.add(relation);
            }
        }
        return conditions;
    }

    public List<Condition> userpropConditions() {
        this.checkFlattened();
        List<Condition> conds = new ArrayList<>();
        for (Condition c : this.conditions) {
            if (!c.isSysprop()) {
                conds.add(c);
            }
        }
        return conds;
    }

    public List<Condition> userpropConditions(Id key) {
        this.checkFlattened();
        List<Condition> conditions = new ArrayList<>();
        for (Condition condition : this.conditions) {
            Relation relation = (Relation) condition;
            if (relation.key().equals(key)) {
                conditions.add(relation);
            }
        }
        return conditions;
    }

    public List<Relation> userpropRelations() {
        List<Relation> relations = new ArrayList<>();
        for (Relation r : this.relations()) {
            if (!r.isSysprop()) {
                relations.add(r);
            }
        }
        return relations;
    }

    public void resetUserpropConditions() {
        this.conditions.removeIf(condition -> !condition.isSysprop());
    }

    public Set<Id> userpropKeys() {
        Set<Id> keys = new LinkedHashSet<>();
        for (Relation r : this.relations()) {
            if (!r.isSysprop()) {
                Condition.UserpropRelation ur = (Condition.UserpropRelation) r;
                keys.add(ur.key());
            }
        }
        return keys;
    }

    /**
     * This method is only used for secondary index scenario,
     * its relation must be EQ
     * @param fields the user property fields
     * @return the corresponding user property serial values of fields
     */
    public String userpropValuesString(List<Id> fields) {
        List<Object> values = new ArrayList<>(fields.size());
        for (Id field : fields) {
            boolean got = false;
            for (Relation r : this.userpropRelations()) {
                if (r.key().equals(field) && !r.isSysprop()) {
                    E.checkState(r.relation == RelationType.EQ ||
                                 r.relation == RelationType.CONTAINS,
                                 "Method userpropValues(List<String>) only " +
                                 "used for secondary index, " +
                                 "relation must be EQ or CONTAINS, but got %s",
                                 r.relation());
                    values.add(r.serialValue());
                    got = true;
                }
            }
            if (!got) {
                throw new BackendException(
                          "No such userprop named '%s' in the query '%s'",
                          field, this);
            }
        }
        return concatValues(values);
    }

    public Set<Object> userpropValues(Id field) {
        Set<Object> values = new HashSet<>();
        for (Relation r : this.userpropRelations()) {
            if (r.key().equals(field)) {
                values.add(r.serialValue());
            }
        }
        return values;
    }

    public Object userpropValue(Id field) {
        Set<Object> values = this.userpropValues(field);
        if (values.isEmpty()) {
            return null;
        }
        E.checkState(values.size() == 1,
                     "Expect one user-property value of field '%s', " +
                     "but got '%s'", field, values.size());
        return values.iterator().next();
    }

    public boolean hasRangeCondition() {
        // NOTE: we need to judge all the conditions, including the nested
        for (Condition.Relation r : this.relations()) {
            if (r.relation().isRangeType()) {
                return true;
            }
        }
        return false;
    }

    public boolean hasSearchCondition() {
        // NOTE: we need to judge all the conditions, including the nested
        for (Condition.Relation r : this.relations()) {
            if (r.relation().isSearchType()) {
                return true;
            }
        }
        return false;
    }

    public boolean hasSecondaryCondition() {
        // NOTE: we need to judge all the conditions, including the nested
        for (Condition.Relation r : this.relations()) {
            if (r.relation().isSecondaryType()) {
                return true;
            }
        }
        return false;
    }

    public boolean hasNeqCondition() {
        // NOTE: we need to judge all the conditions, including the nested
        for (Condition.Relation r : this.relations()) {
            if (r.relation() == RelationType.NEQ) {
                return true;
            }
        }
        return false;
    }

    public boolean matchUserpropKeys(List<Id> keys) {
        Set<Id> conditionKeys = this.userpropKeys();
        return keys.size() > 0 && conditionKeys.containsAll(keys);
    }

    @Override
    public ConditionQuery copy() {
        ConditionQuery query = (ConditionQuery) super.copy();
        query.originQuery(this);
        query.conditions = this.conditions == EMPTY_CONDITIONS ?
                           EMPTY_CONDITIONS :
                           CollectionFactory.newSet(CollectionType.EC,
                                                    this.conditions);

        query.optimizedType = OptimizedType.NONE;
        query.resultsFilter = null;

        return query;
    }

    public ConditionQuery copyAndResetUnshared() {
        ConditionQuery query = this.copy();
        // These fields should not be shared by multiple sub-query
        query.optimizedType = OptimizedType.NONE;
        query.resultsFilter = null;
        return query;
    }

    @Override
    public boolean test(BigElement element) {
        if (!this.ids().isEmpty() && !super.test(element)) {
            return false;
        }

        if (this.resultsFilter != null) {
            return this.resultsFilter.apply(element);
        }
        boolean valid = true;
        for (Condition cond : this.conditions()) {
            valid &= cond.test(element);
            valid &= (this.element2IndexValueMap == null ||
                      this.element2IndexValueMap.validRangeIndex(element,  cond));
        }
        return valid;
    }

    public void checkFlattened() {
        E.checkState(this.isFlattened(),
                     "Query has none-flatten condition: %s", this);
    }

    public boolean isFlattened() {
        for (Condition condition : this.conditions) {
            if (!condition.isFlattened()) {
                return false;
            }
        }
        return true;
    }

    public boolean mayHasDupKeys(Set<BigKeys> keys) {
        Map<BigKeys, Integer> keyCounts = new HashMap<>();
        for (Condition condition : this.conditions()) {
            if (!condition.isRelation()) {
                // Assume may exist duplicate keys when has nested conditions
                return true;
            }
            Relation relation = (Relation) condition;
            if (keys.contains(relation.key())) {
                int keyCount = keyCounts.getOrDefault(relation.key(), 0);
                if (++keyCount > 1) {
                    return true;
                }
                keyCounts.put((BigKeys) relation.key(), keyCount);
            }
        }
        return false;
    }

    public void optimized(OptimizedType optimizedType) {
        assert this.optimizedType.ordinal() <= optimizedType.ordinal() :
               this.optimizedType + " !<= " + optimizedType;
        this.optimizedType = optimizedType;

        Query originQuery = this.originQuery();
        if (originQuery instanceof ConditionQuery) {
            ConditionQuery cq = ((ConditionQuery) originQuery);
            /*
             * Two sub-query(flatten) will both set optimized of originQuery,
             * here we just keep the higher one, this may not be a perfect way
             */
            if (optimizedType.ordinal() > cq.optimized().ordinal()) {
                cq.optimized(optimizedType);
            }
        }
    }

    public OptimizedType optimized() {
        return this.optimizedType;
    }

    public void registerResultsFilter(Function<BigElement, Boolean> filter) {
        assert this.resultsFilter == null;
        this.resultsFilter = filter;

        Query originQuery = this.originQuery();
        if (originQuery instanceof ConditionQuery) {
            ConditionQuery cq = ((ConditionQuery) originQuery);
            cq.registerResultsFilter(filter);
        }
    }

    public ConditionQuery originConditionQuery() {
        Query originQuery = this.originQuery();
        if (!(originQuery instanceof ConditionQuery)) {
            return null;
        }

        while (originQuery.originQuery() instanceof ConditionQuery) {
            originQuery = originQuery.originQuery();
        }
        return (ConditionQuery) originQuery;
    }

    private void ensureElement2IndexValueMap() {
        if (this.element2IndexValueMap == null) {
            this.element2IndexValueMap = new Element2IndexValueMap();
        }
    }

    public static String concatValues(List<Object> values) {
        List<Object> newValues = new ArrayList<>(values.size());
        for (Object v : values) {
            newValues.add(convertNumberIfNeeded(v));
        }
        return SplicingIdGenerator.concatValues(newValues);
    }

    public static String concatValues(Object value) {
        if (value instanceof List) {
            return concatValues((List<Object>)value);
        }

        if (needConvertNumber(value)) {
            return LongEncoding.encodeNumber(value);
        }
        return value.toString();
    }

    private static Object convertNumberIfNeeded(Object value) {
        if (needConvertNumber(value)) {
            return LongEncoding.encodeNumber(value);
        }
        return value;
    }

    private static boolean needConvertNumber(Object value) {
        // Numeric or date values should be converted to number from string
        return NumericUtil.isNumber(value) || value instanceof Date;
    }

    public enum OptimizedType {
        NONE,
        PRIMARY_KEY,
        SORT_KEYS,
        INDEX,
        INDEX_FILTER
    }

    public static final class Element2IndexValueMap {

        private final Map<Id, Set<LeftIndex>> leftIndexMap;
        private final Map<Id, Map<Id, Set<Object>>> filed2IndexValues;
        private Id selectedIndexField;

        public Element2IndexValueMap() {
            this.filed2IndexValues = new HashMap<>();
            this.leftIndexMap = new HashMap<>();
        }

        public void addIndexValue(Id indexField, Id elementId,
                                  Object indexValue) {
            if (!this.filed2IndexValues.containsKey(indexField)) {
                this.filed2IndexValues.put(indexField, new HashMap<>());
            }
            Map<Id, Set<Object>> element2IndexValueMap =
                                 this.filed2IndexValues.get(indexField);
            if (element2IndexValueMap.containsKey(elementId)) {
                element2IndexValueMap.get(elementId).add(indexValue);
            } else {
                element2IndexValueMap.put(elementId,
                                          Sets.newHashSet(indexValue));
            }
        }

        public void selectedIndexField(Id indexField) {
            this.selectedIndexField = indexField;
        }

        public Set<Object> removeIndexValues(Id indexField, Id elementId) {
            if (!this.filed2IndexValues.containsKey(indexField)) {
                return null;
            }
            return this.filed2IndexValues.get(indexField).get(elementId);
        }

        public void addLeftIndex(Id indexField, Set<Object> indexValues,
                                 Id elementId) {
            LeftIndex leftIndex = new LeftIndex(indexValues, indexField);
            if (this.leftIndexMap.containsKey(elementId)) {
                this.leftIndexMap.get(elementId).add(leftIndex);
            } else {
                this.leftIndexMap.put(elementId, Sets.newHashSet(leftIndex));
            }
        }

        public Set<LeftIndex> getLeftIndex(Id elementId) {
            return this.leftIndexMap.get(elementId);
        }

        public void removeElementLeftIndex(Id elementId) {
            this.leftIndexMap.remove(elementId);
        }

        public boolean validRangeIndex(BigElement element, Condition cond) {
            // Not UserpropRelation
            if (!(cond instanceof Condition.UserpropRelation)) {
                return true;
            }

            Condition.UserpropRelation propRelation =
                                       (Condition.UserpropRelation) cond;
            Id propId = propRelation.key();
            Set<Object> fieldValues = this.removeIndexValues(propId,
                                                             element.id());
            if (fieldValues == null) {
                // Not range index
                return true;
            }

            BigProperty<Object> hugeProperty = element.getProperty(propId);
            if (hugeProperty == null) {
                // Property value has been deleted
                this.addLeftIndex(propId, fieldValues, element.id());
                return false;
            }

            /*
             * NOTE: If success remove means has correct index,
             * we should add left index values to left index map
             * waiting to be removed
             */
            boolean hasRightValue = removeValue(fieldValues, hugeProperty.value());
            if (fieldValues.size() > 0) {
                this.addLeftIndex(propId, fieldValues, element.id());
            }

            /*
             * NOTE: When query by more than one range index field,
             * if current field is not the selected one, it can only be used to
             * determine whether the index values matched, can't determine
             * the element is valid or not
             */
            if (this.selectedIndexField != null) {
                return !propId.equals(this.selectedIndexField) || hasRightValue;
            }

            return hasRightValue;
        }

        private static boolean removeValue(Set<Object> values, Object value){
            for (Object compareValue : values) {
                if (numberEquals(compareValue, value)) {
                    values.remove(compareValue);
                    return true;
                }
            }
            return false;
        }

        private static boolean numberEquals(Object number1, Object number2) {
            // Same class compare directly
            if (number1.getClass().equals(number2.getClass())) {
                return number1.equals(number2);
            }

            // Otherwise convert to BigDecimal to make two numbers comparable
            Number n1 = NumericUtil.convertToNumber(number1);
            Number n2 = NumericUtil.convertToNumber(number2);
            BigDecimal b1 = new BigDecimal(n1.doubleValue());
            BigDecimal b2 = new BigDecimal(n2.doubleValue());
            return b1.compareTo(b2) == 0;
        }
    }

    public static final class LeftIndex {

        private final Set<Object> indexFieldValues;
        private final Id indexField;

        public LeftIndex(Set<Object> indexFieldValues, Id indexField) {
            this.indexFieldValues = indexFieldValues;
            this.indexField = indexField;
        }

        public Set<Object> indexFieldValues() {
            return indexFieldValues;
        }

        public Id indexField() {
            return indexField;
        }
    }
}
