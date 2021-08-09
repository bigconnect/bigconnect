package com.mware.ge.query.builder;

import com.mware.ge.*;
import com.mware.ge.query.Compare;
import com.mware.ge.query.GeoCompare;
import com.mware.ge.query.Predicate;
import com.mware.ge.query.TextPredicate;
import com.mware.ge.values.storable.GeoShapeValue;
import com.mware.ge.values.storable.TemporalValue;
import com.mware.ge.values.storable.Value;

import java.util.Set;
import java.util.stream.Collectors;

public class GeQueryBuilders {
    private GeQueryBuilders() {
    }

    /**
     * A query that matches on all documents.
     */
    public static SearchAllQueryBuilder searchAll() {
        return new SearchAllQueryBuilder();
    }

    /**
     * Query all documents using ElasticSearch Query DSL
     */
    public static SearchQueryBuilder search(String query) {
        return new SearchQueryBuilder(query);
    }

    /**
     * A Query that matches documents matching boolean combinations of other queries.
     */
    public static BoolQueryBuilder boolQuery() {
        return new BoolQueryBuilder();
    }

    public static ExistsQueryBuilder exists(String propertyName) { return new ExistsQueryBuilder(propertyName); }

    public static BoolQueryBuilder exists(Graph graph, Class<? extends Value> dataType) {
        Set<String> propertyNames = graph.getPropertyDefinitions().stream()
                .filter(propertyDefinition -> isPropertyOfType(propertyDefinition, dataType))
                .map(PropertyDefinition::getPropertyName)
                .collect(Collectors.toSet());

        if (propertyNames.isEmpty()) {
            throw new GeException("Invalid query parameters, no properties of type " + dataType.getName() + " found");
        }

        BoolQueryBuilder boolQueryBuilder = boolQuery();
        propertyNames.forEach(propName -> boolQueryBuilder.or(exists(propName)));
        return boolQueryBuilder;
    }

    public static PropertyQueryBuilder hasFilter(String propertyName, Predicate predicate, Value value) {
        return new PropertyQueryBuilder(propertyName, predicate, value);
    }

    public static PropertyQueryBuilder hasFilter(String propertyName, Value value) {
        return new PropertyQueryBuilder(propertyName, Compare.EQUAL, value);
    }

    public static BoolQueryBuilder hasFilter(Graph graph, Class<? extends Value> dataType, Predicate predicate, Value value) {
        Set<String> propertyNames = graph.getPropertyDefinitions().stream()
                .filter(propertyDefinition -> isPropertyOfType(propertyDefinition, dataType))
                .map(PropertyDefinition::getPropertyName)
                .collect(Collectors.toSet());

        if (propertyNames.isEmpty()) {
            throw new GeException("Invalid query parameters, no properties of type " + dataType.getName() + " found");
        }

        propertyNames.forEach(propName -> {
            PropertyDefinition propertyDefinition = PropertyDefinition.findPropertyDefinition(graph.getPropertyDefinitions(), propName);
            if (predicate instanceof TextPredicate && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                throw new GeException("Check your TextIndexHint settings. Property " + propertyDefinition.getPropertyName() + " is not full text indexed.");
            } else if (predicate instanceof GeoCompare && !isPropertyOfType(propertyDefinition, GeoShapeValue.class)) {
                throw new GeException("GeoCompare query is only allowed for GeoShape types. Property " + propertyDefinition.getPropertyName() + " is not a GeoShape.");
            } else if (Compare.STARTS_WITH.equals(predicate) && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                throw new GeException("Check your TextIndexHint settings. Property " + propertyDefinition.getPropertyName() + " is not exact match indexed.");
            }
        });

        BoolQueryBuilder boolQueryBuilder = boolQuery();
        propertyNames.forEach(propName -> boolQueryBuilder.or(hasFilter(propName, predicate, value)));
        return boolQueryBuilder;
    }

    public static ConceptTypeQueryBuilder hasConceptType(String... conceptTypes) {
        return new ConceptTypeQueryBuilder(conceptTypes);
    }

    public static EdgeLabelQueryBuilder hasEdgeLabel(String... edgeLabels) {
        return new EdgeLabelQueryBuilder(edgeLabels);
    }

    public static HasExtendedDataQueryBuilder hasExtendedData(String tableName) {
        return new HasExtendedDataQueryBuilder(null, null, tableName);
    }

    public static HasExtendedDataQueryBuilder hasExtendedData(ElementType elementType, String elementId) {
        return new HasExtendedDataQueryBuilder(elementType, elementId, null);
    }

    public static HasExtendedDataQueryBuilder hasExtendedData(ElementType elementType, String elementId, String tableName) {
        return new HasExtendedDataQueryBuilder(elementType, elementId, tableName);
    }

    public static IdQueryBuilder hasIds(String ...ids) {
        return new IdQueryBuilder(ids);
    }

    public static AuthorizationQueryBuilder hasAuthorization(String ...auths) {
        return new AuthorizationQueryBuilder(auths);
    }

    /**
     * Queries for properties in the given range.
     *
     * @param propertyName Name of property.
     * @param startValue   Inclusive start value.
     * @param endValue     Inclusive end value.
     * @return RangeQueryBuilder
     */
    public static <T extends Value> RangeQueryBuilder range(String propertyName, T startValue, T endValue) {
        return new RangeQueryBuilder(propertyName, startValue, true, endValue, true);
    }

    /**
     * Queries for properties in the given range.
     *
     * @param propertyName        Name of property.
     * @param startValue          Inclusive start value.
     * @param inclusiveStartValue true, to include the start value
     * @param endValue            Inclusive end value.
     * @param inclusiveEndValue   true, to include the end value
     * @return RangeQueryBuilder
     */
    public static <T extends Value> RangeQueryBuilder range(String propertyName, T startValue, boolean inclusiveStartValue, T endValue, boolean inclusiveEndValue) {
        return new RangeQueryBuilder(propertyName, startValue, inclusiveStartValue, endValue, inclusiveEndValue);
    }

    /**
     * Creates a query builder object that finds all items similar to the given text for the specified properties.
     *
     * @param propertyNames  The properties to match against.
     * @param text           The text to find similar to.
     * @return A query builder object.
     */
    public static SimilarToQueryBuilder similarTo(String[] propertyNames, String text) {
        return new SimilarToQueryBuilder(propertyNames, text);
    }

    private static boolean isPropertyOfType(PropertyDefinition propertyDefinition, Class<? extends Value> dataType) {
        return dataType.isAssignableFrom(propertyDefinition.getDataType());
    }
}
