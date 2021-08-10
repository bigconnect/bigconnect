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
package com.mware.ge.cypher

import com.mware.ge._
import com.mware.ge.cypher.ge.GeStatisticsHolder
import com.mware.ge.cypher.internal.compatibility.ExceptionTranslatingPlanContext
import com.mware.ge.cypher.internal.planner.spi.{GraphStatistics, IndexDescriptor, PlanContext, StatisticsCompletingGraphStatistics}
import com.mware.ge.cypher.internal.util._
import com.mware.ge.query.aggregations.{CardinalityAggregation, CardinalityResult, TermsAggregation, TermsResult}
import com.mware.ge.query.builder.GeQueryBuilders._
import com.mware.ge.query.builder.{BoolQueryBuilder, GeQueryBuilder, GeQueryBuilders}
import com.mware.ge.search.SearchIndex

import scala.collection.JavaConverters._

object GeGraphStatistics {
  def apply(graph: Graph): GraphStatistics = new StatisticsCompletingGraphStatistics(new BaseGeGraphStatistics(graph))

  class BaseGeGraphStatistics(val graph: Graph) extends GraphStatistics with ResourceCloser {
    val authorizations: Authorizations = new Authorizations("administrator");

    override def nodesWithLabelCardinality(labelId: Option[LabelId], planContext: PlanContext): Cardinality = {
      var retValue: Cardinality = GeStatisticsHolder.nodeByLabelCount.getIfPresent(labelId.get.id)

      if (retValue == null) {
        if (labelId.isEmpty || labelId.get.id == "")
          retValue = nodesAllCardinality()
        else {
          val conceptType = labelId.get.id
          val q = graph.query(searchAll().limit(0L).asInstanceOf[GeQueryBuilder], new Authorizations())
            .addAggregation(new TermsAggregation("count", SearchIndex.CONCEPT_TYPE_FIELD_NAME))

          val found = withResources(q.vertexIds(IdFetchHint.NONE))(
            iterable =>
              iterable.getAggregationResult("count", classOf[TermsResult])
                .getBuckets.asScala
                .find(b => b.key == conceptType)
          )

          if (found.isEmpty)
            retValue = atLeastOne(0)
          else
            retValue = atLeastOne(found.get.count)
        }

        GeStatisticsHolder.nodeByLabelCount.put(labelId.get.id, retValue);
      }

      retValue
    }

    override def nodesAllCardinality(): Cardinality = {
      var retValue = GeStatisticsHolder.nodeAllCount.getIfPresent("")
      if (retValue == null) {
        val count = withResources(graph.query(searchAll().limit(0L).asInstanceOf[GeQueryBuilder], new Authorizations())
          .vertexIds(IdFetchHint.NONE))(
          iterable =>
            iterable.getTotalHits
        )
        retValue = atLeastOne(count)
        GeStatisticsHolder.nodeAllCount.put("", retValue)
      }
      retValue
    }

    override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId], planContext: PlanContext): Cardinality = {
      val qb: BoolQueryBuilder = GeQueryBuilders.boolQuery()
        .limit(0L);

      // TODO: work in progress

//      if (fromLabel.isDefined) {
//        val conceptType = fromLabel.get.id
//        query.hasOutVertexTypes(conceptType)
//      }
//
//      if (toLabel.isDefined) {
//        val conceptType = toLabel.get.id
//        query.hasInVertexTypes(conceptType)
//      }

      if (relTypeId.isDefined) {
        val edgeLabel = relTypeId.get.id
        qb.and(hasEdgeLabel(edgeLabel));
      }

      val query = graph.query(qb, authorizations)
      val count = withResources(query.edgeIds(IdFetchHint.NONE))(
        iterable => iterable.getTotalHits
      )
      atLeastOne(count)
    }

    override def uniqueValueSelectivity(index: IndexDescriptor, planContext: PlanContext): Option[Selectivity] = {
      val indexCardinality = nodeByConceptAndPropertyCount(index)
      if (indexCardinality == Cardinality.EMPTY)
        Some(Selectivity.ZERO)
      else {
        // Probability of any node in the index, to have a property with a given value
        val indexSize = nodeByConceptAndPropertyCount(index).amount
        val distinctValues = countDistinctValues(index);
        if (distinctValues != null) {
          val uniqueValuesPercentage = distinctValues.amount / indexSize
          val frequencyOfNodesWithSameValue = 1.0 / uniqueValuesPercentage
          // This is = 1 / number of unique values
          val indexSelectivity = frequencyOfNodesWithSameValue / indexSize
          Selectivity.of(indexSelectivity)
        } else {
          Selectivity.of(0)
        }
      }
    }

    override def indexPropertyExistsSelectivity(index: IndexDescriptor, planContext: PlanContext): Option[Selectivity] = {
      val labeledNodes = nodesWithLabelCardinality(Some(LabelId(index.label)), planContext).amount
      if (labeledNodes == 0)
        Some(Selectivity.ZERO)
      else {
        // Probability of any node with the given label, to have a given property
        val indexCardinality = nodeByConceptAndPropertyCount(index)
        val indexSelectivity = indexCardinality.amount / labeledNodes
        Selectivity.of(indexSelectivity)
      }
    }

    private def nodeByConceptAndPropertyCount(index: IndexDescriptor): Cardinality = {
      var cacheKey = index.label.id
      index.properties.foreach(pk => cacheKey += "_" + pk.id)
      var indexCardinality = GeStatisticsHolder.nodesByPropertiesCount.getIfPresent(cacheKey)
      if (indexCardinality == null) {
        val queryBuilder: GeQueryBuilder = boolQuery()
          .and(hasConceptType(index.label.id))
          .and(exists(index.property.id))
          .limit(0L);
        val counts = withResources(graph.query(queryBuilder, authorizations).vertexIds(IdFetchHint.NONE))(
          iterable =>
            iterable.getTotalHits
        )

        indexCardinality = Cardinality(counts)
        GeStatisticsHolder.nodesByPropertiesCount.put(cacheKey, indexCardinality)
      }
      indexCardinality
    }

    private def countDistinctValues(index: IndexDescriptor): Cardinality = {
      val cacheKey = index.label.id + "_" + index.property.id
      var distinctCardinality = GeStatisticsHolder.nodesByPropertiesDinctinctCount.getIfPresent(cacheKey)
      if (distinctCardinality == null) {
        try {
          val counts = withResources(graph.query(hasConceptType(index.label.id).limit(0L).asInstanceOf[GeQueryBuilder], authorizations)
            .addAggregation(new CardinalityAggregation("count", index.property.id))
            .vertexIds(IdFetchHint.NONE))(
            iterable =>
              iterable.getAggregationResult("count", classOf[CardinalityResult])
                .value()
          )
          distinctCardinality = atLeastOne(counts.doubleValue())
          GeStatisticsHolder.nodesByPropertiesDinctinctCount.put(cacheKey, distinctCardinality)
        } catch {
          case _: GeException =>
            Cardinality.EMPTY
        }
      }
      distinctCardinality
    }

    /**
     * Due to the way cardinality calculations work, zero is a bit dangerous, as it cancels out
     * any cost that it multiplies with. To avoid this pitfall, we determine that the least count
     * available is one, not zero.
     */
    private def atLeastOne(count: Double): Cardinality = {
      if (count < 1)
        Cardinality.SINGLE
      else
        Cardinality(count)
    }


    private def workspaceId(planContext: PlanContext): String =
      planContext match {
        case e: ExceptionTranslatingPlanContext => e.inner.asInstanceOf[GePlanContext].queryContext.getWorkspaceId

        case e: GePlanContext => e.queryContext.getWorkspaceId

        case _ => throw new CypherExecutionException("I don't know how to handle a PlanContext of type:" + planContext.getClass.getName, new Exception)
      }
  }

}
