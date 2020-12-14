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
package com.mware.ge.cypher.internal.planner.spi

trait TokenContext {
  def getLabelName(id: String): String
  def getOptLabelId(labelName: String): Option[String]
  def getLabelId(labelName: String): String
  def getPropertyKeyName(id: String): String
  def getOptPropertyKeyId(propertyKeyName: String): Option[String]
  def getPropertyKeyId(propertyKeyName: String): String
  def getRelTypeName(id: String): String
  def getOptRelTypeId(relType: String): Option[String]
  def getRelTypeId(relType: String): String
}

object TokenContext {
  val EMPTY = new TokenContext {
    override def getLabelName(id: String): String = throw new IllegalArgumentException("No such label.", null)

    override def getOptLabelId(labelName: String): Option[String] = None

    override def getLabelId(labelName: String): String = throw new IllegalArgumentException("No such label.", null)

    override def getPropertyKeyName(id: String): String = throw new IllegalArgumentException("No such property.", null)

    override def getOptPropertyKeyId(propertyKeyName: String): Option[String] = None

    override def getPropertyKeyId(propertyKeyName: String): String = throw new IllegalArgumentException("No such property.", null)

    override def getRelTypeName(id: String): String = throw new IllegalArgumentException("No such relationship.", null)

    override def getOptRelTypeId(relType: String): Option[String] = None

    override def getRelTypeId(relType: String): String = throw new IllegalArgumentException("No such relationship.", null)
  }
}
