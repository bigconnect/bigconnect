package com.mware.ge.cypher.internal.ir

sealed trait StringNameId {
  def id: String
}

final case class StrLabelId(id: String) extends StringNameId
final case class StrRelTypeId(id: String) extends StringNameId
final case class StrPropertyKeyId(id: String) extends StringNameId

object NameId {
  val WILDCARD: String = ""

  implicit def toKernelEncode(nameId: StringNameId): String = nameId.id
  implicit def toKernelEncode(nameId: Option[StringNameId]): String = nameId.map(toKernelEncode).getOrElse(WILDCARD)
}
