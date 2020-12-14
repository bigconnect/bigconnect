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
package com.mware.ge.cypher.internal.runtime.interpreted

import java.util

import com.mware.ge.values.AnyValue
import com.mware.ge.function.ThrowingBiConsumer
import com.mware.ge.cypher.InternalException
import com.mware.ge.cypher.internal.runtime.{Operations, QueryContext}
import com.mware.ge.values.storable.Values
import com.mware.ge.values.virtual.{MapValue, VirtualNodeValue, VirtualRelationshipValue}

import scala.collection.immutable

object IsMap extends MapSupport {

  def unapply(x: AnyValue): Option[QueryContext => MapValue] = if (isMap(x)) {
    Some(castToMap(x))
  } else {
    None
  }
}

trait MapSupport {
  def isMap(x: AnyValue): Boolean = castToMap.isDefinedAt(x)

  def castToMap: PartialFunction[AnyValue, QueryContext => MapValue] = {
    case x: MapValue => _ => x
    case x: VirtualNodeValue => ctx => new LazyMap(ctx, ctx.nodeOps, x.id())
    case x: VirtualRelationshipValue => ctx => new LazyMap(ctx, ctx.relationshipOps, x.id())
  }
}

class LazyMap[T](ctx: QueryContext, ops: Operations[T], id: String)
  extends MapValue {

  import scala.collection.JavaConverters._

  private lazy val allProps: util.Map[String, AnyValue] = ops.propertyKeyIds(id)
    .map(propertyId => {
      val value: AnyValue = ops.getProperty(id, propertyId)
      propertyId -> value
    }
    ).toMap.asJava

  override def keySet(): util.Set[String] = allProps.keySet()

  override def foreach[E <: Exception](f: ThrowingBiConsumer[String, AnyValue, E]): Unit = {
    val it = allProps.entrySet().iterator()
    while(it.hasNext) {
      val entry = it.next()
      f.accept(entry.getKey, entry.getValue)
    }
  }

  override def containsKey(key: String): Boolean = ctx.getOptPropertyKeyId(key).exists(ops.hasProperty(id, _))

  override def get(key: String): AnyValue =
      ctx.getOptPropertyKeyId(key) match {
        case Some(keyId) =>
          ops.getProperty(id, keyId)
        case None =>
          Values.NO_VALUE
      }

  override def size(): Int = allProps.size()

  //we need a way forcefully load lazy values
  def load(): MapValue =
    if (allProps != null) this
    else throw new InternalException("properties must be loadable at this instant")

}

object MapSupport {

  implicit class PowerMap[A, B](m: immutable.Map[A, B]) {

    def fuse(other: immutable.Map[A, B])(f: (B, B) => B): immutable.Map[A, B] = {
      other.foldLeft(m) {
        case (acc, (k, v)) if acc.contains(k) => acc + (k -> f(acc(k), v))
        case (acc, entry) => acc + entry
      }
    }
  }

}
