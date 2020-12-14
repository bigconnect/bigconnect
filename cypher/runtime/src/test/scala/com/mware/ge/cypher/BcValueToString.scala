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
package com.mware.ge.cypher

import java.time._
import java.time.temporal.TemporalAmount

import com.mware.core.model.schema.SchemaRepository
import com.mware.ge.{Direction, Edge, Vertex}
import com.mware.ge.values.storable.{DurationValue, LongValue}
import com.mware.ge.cypher.values.virtual.{GeEdgeWrappingValue, GeVertexWrappingNodeValue, GeWrappingPath}
import com.mware.ge.values.virtual.PathValue.DirectPathValue

import scala.collection.JavaConverters._
import scala.collection.mutable

object BcValueToString extends (Any => String) {

  def apply(value: Any): String = {
    def convertList(elements: Traversable[_]): String = {
      val convertedElements = elements.map(BcValueToString)
      s"[${convertedElements.mkString(", ")}]"
    }

    value match {
      case null => "null"

      case n: Vertex =>
        val conceptTye = n.getConceptType

        var labelString = "";
        if(!SchemaRepository.THING_CONCEPT_NAME.equals(conceptTye))
          labelString = ":"+conceptTye

        var propMap = mutable.Map[String, AnyRef]()

        for (p <- n.getProperties.asScala) {
          propMap += (p.getName -> p.getValue.asObjectCopy())
        }

        val properties = BcValueToString(propMap.asJava)
        s"($labelString$properties)"

      case n: GeVertexWrappingNodeValue =>
        val conceptTye = n.labels().stringValue(0)

        var labelString = "";
        if(!SchemaRepository.THING_CONCEPT_NAME.equals(conceptTye))
          labelString = ":"+conceptTye

        var propMap = mutable.Map[String, AnyRef]()
        val props = n.getAllProperties;

        for (p <- props.keySet().asScala) {
          propMap += (p -> props.get(p))
        }

        val properties = BcValueToString(propMap.asJava)
        s"($labelString$properties)"

      case r: Edge =>
        val relType = r.getLabel
        var propMap = mutable.Map[String, AnyRef]()

        for (p <- r.getProperties.asScala) {
          propMap += (p.getName -> p.getValue.asObjectCopy())
        }

        val properties = BcValueToString(propMap.asJava)
        s"[:$relType$properties]"

      case r: GeEdgeWrappingValue =>
        val relType = r.`type`().stringValue()
        var propMap = mutable.Map[String, AnyRef]()
        val props = r.getAllProperties;

        for (p <- props.keySet().asScala) {
          propMap += (p -> props.get(p))
        }

        val properties = BcValueToString(propMap.asJava)
        s"[:$relType$properties]"

      case a: Array[_] => convertList(a)

      case l: java.util.List[_] => convertList(l.asScala)

      case m: java.util.Map[_, _] =>
        val properties = m.asScala.map {
          case (k, v) => (k.toString, BcValueToString(v))
        }
        s"{${
          properties.map {
            case (k, v) => s"$k: $v"
          }.mkString(", ")
        }}"

      case path: GeWrappingPath =>
        val (string, _) = path.relationships().foldLeft((BcValueToString(path.startNode()), path.startNode().getId)) {
          case ((currentString, currentNodeId), nextRel) =>
            if (currentNodeId == nextRel.getVertexId(Direction.OUT)) {
              val updatedString = s"$currentString-${BcValueToString(nextRel)}->${BcValueToString(nextRel.getVertex(Direction.IN, path.getAuthorizations))}"
              updatedString -> nextRel.getVertexId(Direction.IN)
            } else {
              val updatedString = s"$currentString<-${BcValueToString(nextRel)}-${BcValueToString(nextRel.getVertex(Direction.OUT, path.getAuthorizations))}"
              updatedString -> nextRel.getVertexId(Direction.OUT)
            }
        }
        s"<$string>"

      case path: DirectPathValue =>
        val (string, _) = path.relationships().foldLeft((BcValueToString(path.startNode()), path.startNode().id())) {
          case ((currentString, currentNodeId), nextRel) =>
            if (currentNodeId == nextRel.startNodeId()) {
              val updatedString = s"$currentString-${BcValueToString(nextRel)}->${BcValueToString(nextRel.endNode())}"
              updatedString -> nextRel.endNodeId()
            } else {
              val updatedString = s"$currentString<-${BcValueToString(nextRel)}-${BcValueToString(nextRel.startNode())}"
              updatedString -> nextRel.startNodeId()
            }
        }
        s"<$string>"

      case s: String => s"'$s'"
      case l: LongValue => l.longValue().toString
      case l: Long => l.toString
      case i: Integer => i.toString
      case d: Double => d.toString
      case f: Float => f.toString
      case b: Boolean => b.toString
      // TODO workaround to escape date time strings until TCK error
      // with colons in unescaped strings is fixed.
      case x: LocalTime => s"'${x.toString}'"
      case x: LocalDate => s"'${x.toString}'"
      case x: LocalDateTime => s"'${x.toString}'"
      case x: OffsetTime => s"'${x.toString}'"
      case x: ZonedDateTime => s"'${x.toString}'"
      case x: DurationValue => s"'${x.prettyPrint}'"
      case x: TemporalAmount => s"'${x.toString}'"

      case other =>
        println(s"could not convert $other of type ${other.getClass}")
        other.toString
    }
  }

}
