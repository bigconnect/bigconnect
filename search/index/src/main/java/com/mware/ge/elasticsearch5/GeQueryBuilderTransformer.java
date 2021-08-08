package com.mware.ge.elasticsearch5;

import com.google.common.base.Joiner;
import com.mware.core.util.StreamUtil;
import com.mware.ge.*;
import com.mware.ge.elasticsearch5.scoring.ElasticsearchScoringStrategy;
import com.mware.ge.query.Compare;
import com.mware.ge.query.Contains;
import com.mware.ge.query.GeoCompare;
import com.mware.ge.query.TextPredicate;
import com.mware.ge.query.builder.*;
import com.mware.ge.scoring.ScoringStrategy;
import com.mware.ge.type.*;
import com.mware.ge.values.storable.*;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceCalculator;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.mware.ge.elasticsearch5.Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME;
import static com.mware.ge.elasticsearch5.Elasticsearch5SearchIndex.GEO_POINT_PROPERTY_NAME_SUFFIX;
import static org.locationtech.spatial4j.distance.DistanceUtils.KM_TO_DEG;

public class GeQueryBuilderTransformer {
    private final Elasticsearch5SearchIndex searchIndex;
    private final Graph graph;
    private final Authorizations authorizations;
    private final GeQueryBuilder queryBuilder;
    private final StandardAnalyzer analyzer;

    public GeQueryBuilderTransformer(
            Elasticsearch5SearchIndex searchIndex,
            Graph graph,
            Authorizations authorizations,
            GeQueryBuilder queryBuilder,
            StandardAnalyzer analyzer
    ) {
        this.searchIndex = searchIndex;
        this.graph = graph;
        this.authorizations = authorizations;
        this.queryBuilder = queryBuilder;
        this.analyzer = analyzer;
    }

    public QueryBuilder getElasticQuery(EnumSet<ElasticsearchDocumentType> elementTypes) {
        QueryBuilder qb = toElasticQueryBuilder(elementTypes, queryBuilder);

        ScoringStrategy scoringStrategy = queryBuilder.getScoringStrategy();
        if (scoringStrategy != null) {
            if (!(scoringStrategy instanceof ElasticsearchScoringStrategy)) {
                throw new GeException("scoring strategies must implement " + ElasticsearchScoringStrategy.class.getName() + " to work with Elasticsearch");
            }
            qb = ((ElasticsearchScoringStrategy) scoringStrategy).updateElasticsearchQuery(
                    graph,
                    searchIndex,
                    qb,
                    authorizations
            );
        }
        return qb;
    }

    protected QueryBuilder toElasticQueryBuilder(EnumSet<ElasticsearchDocumentType> elementTypes, GeQueryBuilder qb) {
        QueryBuilder query;
        if (qb instanceof com.mware.ge.query.builder.BoolQueryBuilder) {
            query = QueryBuilders.boolQuery();
            for (GeQueryBuilder andClause : ((com.mware.ge.query.builder.BoolQueryBuilder)qb).getAndClauses()) {
                ((BoolQueryBuilder)query).must(toElasticQueryBuilder(elementTypes, andClause));
            }
            for (GeQueryBuilder andNotClause : ((com.mware.ge.query.builder.BoolQueryBuilder)qb).getNotClauses()) {
                ((BoolQueryBuilder) query).mustNot(toElasticQueryBuilder(elementTypes, andNotClause));
            }
            for (GeQueryBuilder orClause : ((com.mware.ge.query.builder.BoolQueryBuilder)qb).getOrClauses()) {
                ((BoolQueryBuilder) query).should(toElasticQueryBuilder(elementTypes, orClause));
            }
            return query;
        } else if (qb instanceof PropertyQueryBuilder) {
            return getFiltersForProperty((PropertyQueryBuilder) qb);
        } else if (qb instanceof com.mware.ge.query.builder.ExistsQueryBuilder) {
            return getFilterForExists((com.mware.ge.query.builder.ExistsQueryBuilder) qb);
        } else if (qb instanceof HasExtendedDataQueryBuilder) {
            return getFilterForHasExtendedData((HasExtendedDataQueryBuilder) qb);
        } else if (qb instanceof AuthorizationQueryBuilder) {
            return getFilterForHasAuthorization((AuthorizationQueryBuilder) qb);
        } else if (qb instanceof ConceptTypeQueryBuilder && ((elementTypes == null) || elementTypes.contains(ElasticsearchDocumentType.VERTEX))) {
            return QueryBuilders.termsQuery(Elasticsearch5SearchIndex.CONCEPT_TYPE_FIELD_NAME, ((ConceptTypeQueryBuilder)qb).getConceptTypes());
        } else if (qb instanceof EdgeLabelQueryBuilder && (elementTypes == null || elementTypes.contains(ElasticsearchDocumentType.EDGE))) {
            return QueryBuilders.termsQuery(Elasticsearch5SearchIndex.EDGE_LABEL_FIELD_NAME, ((EdgeLabelQueryBuilder)qb).getEdgeLabels());
        } else if (qb instanceof IdQueryBuilder) {
            return QueryBuilders.termsQuery(ELEMENT_ID_FIELD_NAME, ((IdQueryBuilder)qb).getIds());
        } else if (qb instanceof SearchAllQueryBuilder) {
            return QueryBuilders.matchAllQuery();
        } else if (qb instanceof SearchQueryBuilder) {
            return createQueryStringQuery(((SearchQueryBuilder)qb).getQuery());
        } else if (qb instanceof SimilarToQueryBuilder) {
            return createSimilarToTextQuery((SimilarToQueryBuilder) qb);
        }

        throw new IllegalArgumentException("Unsupported GeQueryBuilder: "+qb.getClass().getName());
    }

    protected QueryBuilder createQueryStringQuery(String queryString) {
        if (queryString == null || queryString.equals("*")) {
            return QueryBuilders.matchAllQuery();
        }

        queryString = searchIndex.getQueryStringTransformer().transform(queryString, authorizations);

        Collection<String> fields = searchIndex.getQueryablePropertyNames(graph, authorizations);
        QueryStringQueryBuilder qs = QueryBuilders.queryStringQuery(queryString);
        for (String field : fields) {
            qs = qs.field(searchIndex.replaceFieldnameDots(field));
        }
        qs.allowLeadingWildcard(false);
        return qs;
    }

    protected QueryBuilder createSimilarToTextQuery(SimilarToQueryBuilder similarTo) {
        List<String> allFields = new ArrayList<>();
        String[] fields = similarTo.getPropertyNames();
        for (String field : fields) {
            Collections.addAll(allFields, searchIndex.getPropertyNames(graph, field, authorizations));
        }
        MoreLikeThisQueryBuilder q = QueryBuilders.moreLikeThisQuery(
                allFields.toArray(new String[allFields.size()]),
                new String[]{similarTo.getText()},
                null
        );
        if (similarTo.getMinTermFrequency() != null) {
            q.minTermFreq(similarTo.getMinTermFrequency());
        }
        if (similarTo.getMaxQueryTerms() != null) {
            q.maxQueryTerms(similarTo.getMaxQueryTerms());
        }
        if (similarTo.getMinDocFrequency() != null) {
            q.minDocFreq(similarTo.getMinDocFrequency());
        }
        if (similarTo.getMaxDocFrequency() != null) {
            q.maxDocFreq(similarTo.getMaxDocFrequency());
        }
        if (similarTo.getBoost() != null) {
            q.boost(similarTo.getBoost());
        }
        return q;
    }

    protected QueryBuilder getFilterForHasAuthorization(AuthorizationQueryBuilder hasAuthorization) {
        PropertyNameVisibilitiesStore visibilitiesStore = searchIndex.getPropertyNameVisibilitiesStore();

        Set<String> hashes = Arrays.stream(hasAuthorization.getAuthorizations())
                .flatMap(authorization -> visibilitiesStore.getHashesWithAuthorization(graph, authorization, authorizations).stream())
                .collect(Collectors.toSet());

        List<QueryBuilder> filters = new ArrayList<>();
        for (PropertyDefinition propertyDefinition : graph.getPropertyDefinitions()) {
            String propertyName = propertyDefinition.getPropertyName();

            Set<String> matchingPropertyHashes = visibilitiesStore.getHashes(graph, propertyName, authorizations).stream()
                    .filter(hashes::contains)
                    .collect(Collectors.toSet());
            for (String fieldName : searchIndex.addHashesToPropertyName(propertyName, matchingPropertyHashes)) {
                filters.add(QueryBuilders.existsQuery(searchIndex.replaceFieldnameDots(fieldName)));
            }
        }

        List<String> internalFields = Arrays.asList(
                Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME,
                Elasticsearch5SearchIndex.HIDDEN_VERTEX_FIELD_NAME,
                Elasticsearch5SearchIndex.HIDDEN_PROPERTY_FIELD_NAME
        );
        internalFields.forEach(fieldName -> {
            Collection<String> fieldHashes = visibilitiesStore.getHashes(graph, fieldName, authorizations);
            Collection<String> matchingFieldHashes = fieldHashes.stream().filter(hashes::contains).collect(Collectors.toSet());
            for (String fieldNameWithHash : searchIndex.addHashesToPropertyName(fieldName, matchingFieldHashes)) {
                filters.add(QueryBuilders.existsQuery(fieldNameWithHash));
            }
        });

        if (filters.isEmpty()) {
            throw new GeNoMatchingPropertiesException(Joiner.on(", ").join(hasAuthorization.getAuthorizations()));
        }

        return getSingleFilterOrOrTheFilters(filters, hasAuthorization);
    }

    protected QueryBuilder getFilterForExists(com.mware.ge.query.builder.ExistsQueryBuilder qb) {
        PropertyDefinition propertyDefinition = graph.getPropertyDefinition(qb.getPropertyName());

        if (propertyDefinition == null) {
            // If we didn't find any property definitions, this means none of them are defined on the graph
            throw new GeNoMatchingPropertiesException(qb.getPropertyName());
        }

        List<QueryBuilder> filters = new ArrayList<>();
        String[] propertyNames = searchIndex.getPropertyNames(graph, propertyDefinition.getPropertyName(), authorizations);
        for (String propertyName : propertyNames) {
            filters.add(QueryBuilders.existsQuery(propertyName));
            if (GeoShapeValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
                filters.add(QueryBuilders.existsQuery(propertyName + Elasticsearch5SearchIndex.GEO_PROPERTY_NAME_SUFFIX));
            } else if (isExactMatchPropertyDefinition(propertyDefinition)) {
                filters.add(QueryBuilders.existsQuery(propertyName + Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX));
            }
        }

        if (filters.isEmpty()) {
            // If we didn't add any filters, this means it doesn't exist on any elements so raise an error
            throw new GeNoMatchingPropertiesException(qb.getPropertyName());
        }

        return getSingleFilterOrOrTheFilters(filters, qb);
    }

    protected QueryBuilder getFiltersForProperty(PropertyQueryBuilder qb) {
        if (qb.getPredicate() instanceof Compare) {
            return getFilterForComparePredicate((Compare) qb.getPredicate(), qb);
        } else if (qb.getPredicate() instanceof Contains) {
            return getFilterForContainsPredicate((Contains) qb.getPredicate(), qb);
        } else if (qb.getPredicate() instanceof TextPredicate) {
            return getFilterForTextPredicate((TextPredicate) qb.getPredicate(), qb);
        } else if (qb.getPredicate() instanceof GeoCompare) {
            return getFilterForGeoComparePredicate((GeoCompare) qb.getPredicate(), qb);
        } else {
            throw new GeException("Unexpected predicate type " + qb.getPredicate().getClass().getName());
        }
    }

    private QueryBuilder getFilterForHasExtendedData(HasExtendedDataQueryBuilder has) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolean hasQuery = false;
        if (has.getElementType() != null) {
            boolQuery.must(
                    QueryBuilders.termQuery(
                            Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME,
                            ElasticsearchDocumentType.getExtendedDataDocumentTypeFromElementType(has.getElementType()).getKey()
                    )
            );
            hasQuery = true;
        }
        if (has.getElementId() != null) {
            boolQuery.must(QueryBuilders.termQuery(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME, has.getElementId()));
            hasQuery = true;
        }
        if (has.getTableName() != null) {
            boolQuery.must(QueryBuilders.termQuery(Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME, has.getTableName()));
            hasQuery = true;
        }
        if (!hasQuery) {
            throw new GeException("Cannot include a hasExtendedData clause with all nulls");
        }
        return boolQuery;
    }


    protected QueryBuilder getFilterForGeoComparePredicate(GeoCompare compare, PropertyQueryBuilder has) {
        PropertyDefinition propertyDefinition = graph.getPropertyDefinition(has.getPropertyName());
        if (propertyDefinition == null) {
            // If we didn't find any property definitions, this means none of them are defined on the graph
            throw new GeNoMatchingPropertiesException(has.getPropertyName());
        }

        if (!(has.getValue() instanceof GeoShapeValue)) {
            throw new GeNotSupportedException("GeoCompare searches only accept values of type GeoShape");
        }

        GeoShape value = (GeoShape) convertQueryValue(has.getValue());
        if (value instanceof GeoHash) {
            value = ((GeoHash) value).toGeoRect();
        }

        List<QueryBuilder> filters = new ArrayList<>();
        if (propertyDefinition != null && !GeoShapeValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
            throw new GeNotSupportedException("Unable to perform geo query on field of type: " + propertyDefinition.getDataType().getName());
        }

        String[] propertyNames = searchIndex.getPropertyNames(graph, propertyDefinition.getPropertyName(), authorizations);
        for (String propertyName : propertyNames) {
            ShapeRelation relation = ShapeRelation.getRelationByName(compare.getCompareName());
            if (GeoPointValue.class.isAssignableFrom(propertyDefinition.getDataType()) && value instanceof GeoCircle) {
                GeoCircle geoCircle = (GeoCircle) value;
                GeoDistanceQueryBuilder geoDistanceQueryBuilder = new GeoDistanceQueryBuilder(propertyName + GEO_POINT_PROPERTY_NAME_SUFFIX)
                        .point(geoCircle.getLatitude(), geoCircle.getLongitude())
                        .distance(geoCircle.getRadius(), DistanceUnit.KILOMETERS)
                        .ignoreUnmapped(true);
                if (relation == ShapeRelation.DISJOINT) {
                    BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
                    filters.add(boolQueryBuilder.mustNot(geoDistanceQueryBuilder));
                } else {
                    filters.add(geoDistanceQueryBuilder);
                }
            } else {
                ShapeBuilder shapeBuilder = getShapeBuilder(value);
                filters.add(new GeoShapeQueryBuilder(propertyName + Elasticsearch5SearchIndex.GEO_PROPERTY_NAME_SUFFIX, shapeBuilder.buildGeometry())
                        .ignoreUnmapped(true)
                        .relation(relation)
                );
            }
        }

        if (filters.isEmpty()) {
            // If we didn't add any filters, this means it doesn't exist on any elements so raise an error
            throw new GeNoMatchingPropertiesException(has.getPropertyName());
        }

        return getSingleFilterOrOrTheFilters(filters, has);
    }

    private ShapeBuilder getShapeBuilder(GeoShape geoShape) {
        if (geoShape instanceof GeoCircle) {
            return getCircleBuilder((GeoCircle) geoShape);
        } else if (geoShape instanceof GeoRect) {
            return getEnvelopeBuilder((GeoRect) geoShape);
        } else if (geoShape instanceof GeoCollection) {
            return getGeometryCollectionBuilder((GeoCollection) geoShape);
        } else if (geoShape instanceof GeoLine) {
            return getLineStringBuilder((GeoLine) geoShape);
        } else if (geoShape instanceof GeoPoint) {
            return getPointBuilder((GeoPoint) geoShape);
        } else if (geoShape instanceof GeoPolygon) {
            return getPolygonBuilder((GeoPolygon) geoShape);
        } else {
            throw new GeException("Unexpected has value type " + geoShape.getClass().getName());
        }
    }

    private GeometryCollectionBuilder getGeometryCollectionBuilder(GeoCollection geoCollection) {
        GeometryCollectionBuilder shapeBuilder = new GeometryCollectionBuilder();
        geoCollection.getGeoShapes().forEach(shape -> shapeBuilder.shape(getShapeBuilder(shape)));
        return shapeBuilder;
    }

    private PointBuilder getPointBuilder(GeoPoint geoPoint) {
        return new PointBuilder(geoPoint.getLongitude(), geoPoint.getLatitude());
    }

    private ShapeBuilder getCircleBuilder(GeoCircle geoCircle) {
        // NOTE: as of ES7, storing circles is no longer supported so we need approximate the circle with a polygon
        double radius = geoCircle.getRadius();
        double maxSideLengthKm = searchIndex.getConfig().getGeocircleToPolygonSideLength();
        maxSideLengthKm = Math.min(radius, maxSideLengthKm);

        // calculate how many points we need to use given the length of a polygon side
        int numberOfPoints = (int) Math.ceil(Math.PI / Math.asin((maxSideLengthKm / (2 * radius))));
        numberOfPoints = Math.min(numberOfPoints, searchIndex.getConfig().getGeocircleToPolygonMaxNumSides());

        // Given the number of sides, loop through slices of 360 degrees and calculate the lat/lon at that radius and heading
        SpatialContext spatialContext = SpatialContext.GEO;
        DistanceCalculator distanceCalculator = spatialContext.getDistCalc();
        Point centerPoint = spatialContext.getShapeFactory().pointXY(DistanceUtils.normLonDEG(geoCircle.getLongitude()), DistanceUtils.normLatDEG(geoCircle.getLatitude()));
        ArrayList<GeoPoint> points = new ArrayList<>();
        for (float angle = 360; angle > 0; angle -= 360.0 / numberOfPoints) {
            Point point = distanceCalculator.pointOnBearing(centerPoint, geoCircle.getRadius() * KM_TO_DEG, angle, spatialContext, null);
            points.add(new GeoPoint(point.getY(), point.getX()));
        }

        // Polygons must start/end at the same point, so add the first point onto the end
        points.add(points.get(0));

        return getPolygonBuilder(new GeoPolygon(points));
    }

    private EnvelopeBuilder getEnvelopeBuilder(GeoRect geoRect) {
        Coordinate topLeft = new Coordinate(geoRect.getNorthWest().getLongitude(), geoRect.getNorthWest().getLatitude());
        Coordinate bottomRight = new Coordinate(geoRect.getSouthEast().getLongitude(), geoRect.getSouthEast().getLatitude());
        return new EnvelopeBuilder(topLeft, bottomRight);
    }

    private LineStringBuilder getLineStringBuilder(GeoLine geoLine) {
        List<Coordinate> coordinates = geoLine.getGeoPoints().stream()
                .map(geoPoint -> new Coordinate(geoPoint.getLongitude(), geoPoint.getLatitude()))
                .collect(Collectors.toList());
        return new LineStringBuilder(coordinates);
    }

    private PolygonBuilder getPolygonBuilder(GeoPolygon geoPolygon) {
        CoordinatesBuilder coordinatesBuilder = new CoordinatesBuilder();
        geoPolygon.getOuterBoundary().stream()
                .map(geoPoint -> new Coordinate(geoPoint.getLongitude(), geoPoint.getLatitude()))
                .forEach(coordinatesBuilder::coordinate);
        PolygonBuilder polygonBuilder = new PolygonBuilder(coordinatesBuilder);
        geoPolygon.getHoles().forEach(hole -> {
            List<Coordinate> coordinates = hole.stream()
                    .map(geoPoint -> new Coordinate(geoPoint.getLongitude(), geoPoint.getLatitude()))
                    .collect(Collectors.toList());
            polygonBuilder.hole(new LineStringBuilder(coordinates));
        });
        return polygonBuilder;
    }

    private QueryBuilder getSingleFilterOrOrTheFilters(List<QueryBuilder> filters, GeQueryBuilder qb) {
        if (filters.size() > 1) {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            for (QueryBuilder filter : filters) {
                boolQuery.should(filter);
            }
            boolQuery.minimumShouldMatch(1);
            return boolQuery;
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            throw new GeException("Unexpected filter count, expected at least 1 filter for: " + qb);
        }
    }

    private QueryBuilder getSingleFilterOrAndTheFilters(List<QueryBuilder> filters, GeQueryBuilder qb) {
        if (filters.size() > 1) {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            for (QueryBuilder filter : filters) {
                boolQuery.must(filter);
            }
            return boolQuery;
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            throw new GeException("Unexpected filter count, expected at least 1 filter for: " + qb);
        }
    }

    protected QueryBuilder getFilterForTextPredicate(TextPredicate compare, PropertyQueryBuilder qb) {
        String[] propertyNames = searchIndex.getPropertyNames(graph, qb.getPropertyName(), authorizations);
        if (propertyNames.length == 0) {
            throw new GeNoMatchingPropertiesException(qb.getPropertyName());
        }

        Object value = convertQueryValue(qb.getValue());
        if (value instanceof String) {
            value = ((String) value).toLowerCase(); // using the standard analyzer all strings are lower-cased.
        }

        List<QueryBuilder> filters = new ArrayList<>();
        for (String propertyName : propertyNames) {
            switch (compare) {
                case CONTAINS:
                    if (value instanceof String) {
                        String[] terms = splitStringIntoTerms((String) value);
                        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                        for (String term : terms) {
                            boolQueryBuilder.must(QueryBuilders.termQuery(propertyName, term));
                        }
                        filters.add(boolQueryBuilder);
                    } else {
                        filters.add(QueryBuilders.termQuery(propertyName, value));
                    }
                    break;
                case DOES_NOT_CONTAIN:
                    BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
                    if (value instanceof String) {
                        String[] terms = splitStringIntoTerms((String) value);
                        filters.add(boolQueryBuilder.mustNot(QueryBuilders.termsQuery(propertyName, terms)));
                    } else {
                        filters.add(boolQueryBuilder.mustNot(QueryBuilders.termQuery(propertyName, value)));
                    }
                    break;
                default:
                    throw new GeException("Unexpected text predicate " + qb.getPredicate());
            }
        }
        if (compare.equals(TextPredicate.DOES_NOT_CONTAIN)) {
            return getSingleFilterOrAndTheFilters(filters, qb);
        }
        return getSingleFilterOrOrTheFilters(filters, qb);
    }

    protected QueryBuilder getFilterForContainsPredicate(Contains contains, PropertyQueryBuilder has) {
        String[] propertyNames = searchIndex.getPropertyNames(graph, has.getPropertyName(), authorizations);
        if (propertyNames.length == 0) {
            if (contains.equals(Contains.NOT_IN)) {
                return QueryBuilders.matchAllQuery();
            }
            throw new GeNoMatchingPropertiesException(has.getPropertyName());
        }

        Object convertedValue;

        if (ArrayValue.class.isAssignableFrom(has.getValue().getClass())) {
            convertedValue = StreamUtil.stream(((ArrayValue) has.getValue()).iterator()).map(v -> convertQueryValue((Value) v)).toArray(Object[]::new);
        } else {
            convertedValue = new Object[]{convertQueryValue(has.getValue())};
        }

        List<QueryBuilder> filters = new ArrayList<>();
        for (String propertyName : propertyNames) {
            filters.add(getFilterForProperty(contains, has, propertyName, convertedValue));
        }
        if (contains == Contains.NOT_IN) {
            return getSingleFilterOrAndTheFilters(filters, has);
        }
        return getSingleFilterOrOrTheFilters(filters, has);
    }

    private QueryBuilder getFilterForProperty(Contains contains, PropertyQueryBuilder has, String propertyName, Object value) {
        if (Element.ID_PROPERTY_NAME.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME;
        } else if (Edge.LABEL_PROPERTY_NAME.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.EDGE_LABEL_FIELD_NAME;
        } else if (Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.OUT_VERTEX_ID_FIELD_NAME;
        } else if (Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.IN_VERTEX_ID_FIELD_NAME;
        } else if (Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            return QueryBuilders.boolQuery()
                    .should(getFilterForProperty(contains, has, Edge.OUT_VERTEX_ID_PROPERTY_NAME, value))
                    .should(getFilterForProperty(contains, has, Edge.IN_VERTEX_ID_PROPERTY_NAME, value))
                    .minimumShouldMatch(1);
        } else if (ExtendedDataRow.TABLE_NAME.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME;
        } else if (ExtendedDataRow.ROW_ID.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME;
        } else if (ExtendedDataRow.ELEMENT_ID.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME;
        } else if (ExtendedDataRow.ELEMENT_TYPE.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME;
            value = ElasticsearchDocumentType.getExtendedDataDocumentTypeFromElementType(ElementType.parse(value)).getKey();
        } else if (value instanceof String
                || value instanceof String[]
                || (value instanceof Object[] && ((Object[]) value).length > 0 && ((Object[]) value)[0] instanceof String)
        ) {
            propertyName = propertyName + Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
        }
        switch (contains) {
            case IN:
                return QueryBuilders.termsQuery(propertyName, (Object[]) value);
            case NOT_IN:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(propertyName, (Object[]) value));
            default:
                throw new GeException("Unexpected Contains predicate " + has.getPredicate());
        }
    }

    protected QueryBuilder getFilterForComparePredicate(Compare compare, PropertyQueryBuilder has) {
        String[] propertyNames = searchIndex.getPropertyNames(graph, has.getPropertyName(), authorizations);
        if (propertyNames.length == 0) {
            if (compare.equals(Compare.NOT_EQUAL)) {
                return QueryBuilders.matchAllQuery();
            }
            throw new GeNoMatchingPropertiesException(has.getPropertyName());
        }

        Object convertedValue = convertQueryValue(has.getValue());

        List<QueryBuilder> filters = new ArrayList<>();
        for (String propertyName : propertyNames) {
            filters.add(getFilterForProperty(compare, has, propertyName, convertedValue));
        }
        if (compare == Compare.NOT_EQUAL) {
            return getSingleFilterOrAndTheFilters(filters, has);
        }
        return getSingleFilterOrOrTheFilters(filters, has);
    }

    private QueryBuilder getFilterForProperty(Compare compare, PropertyQueryBuilder has, String propertyName, Object convertedValue) {
        if (Element.ID_PROPERTY_NAME.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME;
        } else if (Edge.LABEL_PROPERTY_NAME.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.EDGE_LABEL_FIELD_NAME;
        } else if (Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.OUT_VERTEX_ID_FIELD_NAME;
        } else if (Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.IN_VERTEX_ID_FIELD_NAME;
        } else if (Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            return QueryBuilders.boolQuery()
                    .should(getFilterForProperty(compare, has, Edge.OUT_VERTEX_ID_PROPERTY_NAME, convertedValue))
                    .should(getFilterForProperty(compare, has, Edge.IN_VERTEX_ID_PROPERTY_NAME, convertedValue))
                    .minimumShouldMatch(1);
        } else if (ExtendedDataRow.TABLE_NAME.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME;
        } else if (ExtendedDataRow.ROW_ID.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME;
        } else if (ExtendedDataRow.ELEMENT_ID.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME;
        } else if (ExtendedDataRow.ELEMENT_TYPE.equals(propertyName)) {
            propertyName = Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME;
            convertedValue = ElasticsearchDocumentType.getExtendedDataDocumentTypeFromElementType(ElementType.parse(convertedValue)).getKey();
        } else if (convertedValue instanceof String || convertedValue instanceof String[]) {
            propertyName = propertyName + Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
        }

        switch (compare) {
            case EQUAL:
                if (has.getValue() instanceof DateValue) {
                    LocalDate dt = (LocalDate) convertedValue;
                    long lower = dt.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
                    long upper = dt.atTime(23, 59, 59, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
                    return QueryBuilders.rangeQuery(propertyName)
                            .gte(lower)
                            .lte(upper);
                } else if (has.getValue() instanceof DateTimeValue) {
                    ZonedDateTime lower = (ZonedDateTime) convertedValue;
                    ZonedDateTime upper = lower.plus(1, ChronoUnit.MILLIS);
                    return QueryBuilders.rangeQuery(propertyName)
                            .gte(lower.withZoneSameLocal(ZoneOffset.UTC).toInstant().toEpochMilli())
                            .lte(upper.withZoneSameLocal(ZoneOffset.UTC).toInstant().toEpochMilli());
                } else {
                    return QueryBuilders.termQuery(propertyName, convertedValue);
                }
            case GREATER_THAN_EQUAL:
                if (has.getValue() instanceof DateValue) {
                    LocalDate dt = (LocalDate) convertedValue;
                    long lower = dt.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
                    return QueryBuilders.rangeQuery(propertyName).gte(lower);
                } else if (has.getValue() instanceof DateTimeValue) {
                    ZonedDateTime dt = (ZonedDateTime) convertedValue;
                    return QueryBuilders.rangeQuery(propertyName).gte(dt.withZoneSameLocal(ZoneOffset.UTC).toInstant().toEpochMilli());
                }
                return QueryBuilders.rangeQuery(propertyName).gte(convertedValue);
            case GREATER_THAN:
                if (has.getValue() instanceof DateValue) {
                    LocalDate dt = (LocalDate) convertedValue;
                    long upper = dt.atTime(23, 59, 59, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
                    return QueryBuilders.rangeQuery(propertyName).gt(upper);
                } else if (has.getValue() instanceof DateTimeValue) {
                    ZonedDateTime dt = (ZonedDateTime) convertedValue;
                    return QueryBuilders.rangeQuery(propertyName).gt(dt.withZoneSameLocal(ZoneOffset.UTC).toInstant().toEpochMilli());
                }
                return QueryBuilders.rangeQuery(propertyName).gt(convertedValue);
            case LESS_THAN_EQUAL:
                if (has.getValue() instanceof DateValue) {
                    LocalDate dt = (LocalDate) convertedValue;
                    long upper = dt.atTime(23, 59, 59, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
                    return QueryBuilders.rangeQuery(propertyName).lte(upper);
                } else if (has.getValue() instanceof DateTimeValue) {
                    ZonedDateTime dt = (ZonedDateTime) convertedValue;
                    return QueryBuilders.rangeQuery(propertyName).lte(dt.withZoneSameLocal(ZoneOffset.UTC).toInstant().toEpochMilli());
                }
                return QueryBuilders.rangeQuery(propertyName).lte(convertedValue);
            case LESS_THAN:
                if (has.getValue() instanceof DateValue) {
                    LocalDate dt = (LocalDate) convertedValue;
                    long lower = dt.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
                    return QueryBuilders.rangeQuery(propertyName).lt(lower);
                } else if (has.getValue() instanceof DateTimeValue) {
                    ZonedDateTime dt = (ZonedDateTime) convertedValue;
                    return QueryBuilders.rangeQuery(propertyName).lt(dt.withZoneSameLocal(ZoneOffset.UTC).toInstant().toEpochMilli());
                }
                return QueryBuilders.rangeQuery(propertyName).lt(convertedValue);
            case NOT_EQUAL:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(propertyName, convertedValue));
            case STARTS_WITH:
                if (!(convertedValue instanceof String)) {
                    throw new GeException("STARTS_WITH may only be used to query String values");
                }
                return QueryBuilders.prefixQuery(propertyName, (String) convertedValue);
            case RANGE:
                if (!(convertedValue instanceof ZonedDateTime[])) {
                    throw new GeException("RANGE may only be used to query ZonedDateTime[] values");
                }
                ZonedDateTime[] range = (ZonedDateTime[]) convertedValue;
                ZonedDateTime startValue = range[0];
                ZonedDateTime endValue = range[1];

                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(propertyName);
                if (startValue != null) {
                    rangeQueryBuilder = rangeQueryBuilder.gte(startValue.withZoneSameLocal(ZoneOffset.UTC).toInstant().toEpochMilli());
                }
                if (endValue != null) {
                    rangeQueryBuilder = rangeQueryBuilder.lt(endValue.withZoneSameLocal(ZoneOffset.UTC).toInstant().toEpochMilli());
                }
                return rangeQueryBuilder;
            default:
                throw new GeException("Unexpected Compare predicate " + has.getPredicate());
        }
    }

    private Object convertQueryValue(Value value) {
        if (value instanceof TextValue) {
            return ((TextValue) value).stringValue();
        } else if (value instanceof NumberValue) {
            return ((NumberValue) value).asObjectCopy();
        } else if (value instanceof BooleanValue) {
            return ((BooleanValue) value).booleanValue();
        } else if (value instanceof DateValue) {
            return ((DateValue) value).asObjectCopy();
        } else if (value instanceof DateTimeValue) {
            return  ((DateTimeValue) value).asObjectCopy();
        } else if (value instanceof IntArray) {
            int[] array = ((IntArray) value).asObjectCopy();
            return IntStream.of(array).boxed().toArray(Integer[]::new);
        } else if (value instanceof GeoShapeValue) {
            return ((GeoShapeValue)value).asObjectCopy();
        } else if (value instanceof DateTimeArray) {
            return ((DateTimeArray)value).asObjectCopy();
        } else if (value instanceof NoValue) {
            return null;
        }
        throw new IllegalArgumentException("Don't know how to convert to query value: " + value.getClass().getName());
    }

    private String[] splitStringIntoTerms(String value) {
        try {
            List<String> results = new ArrayList<>();
            try (TokenStream tokens = analyzer.tokenStream("", value)) {
                CharTermAttribute term = tokens.getAttribute(CharTermAttribute.class);
                tokens.reset();
                while (tokens.incrementToken()) {
                    String t = term.toString().trim();
                    if (t.length() > 0) {
                        results.add(t);
                    }
                }
            }
            return results.toArray(new String[results.size()]);
        } catch (IOException e) {
            throw new GeException("Could not tokenize string: " + value, e);
        }
    }

    protected boolean isExactMatchPropertyDefinition(PropertyDefinition propertyDefinition) {
        return propertyDefinition != null
                && TextValue.class.isAssignableFrom(propertyDefinition.getDataType())
                && propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH);
    }
}
