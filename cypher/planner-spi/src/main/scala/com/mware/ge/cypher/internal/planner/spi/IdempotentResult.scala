package com.mware.ge.cypher.internal.planner.spi

final case class IdempotentResult[T](value: T, wasCreated: Boolean = true) {
  def ifCreated[R](f: => R): Option[R] = if (wasCreated) Some(f) else None
}
