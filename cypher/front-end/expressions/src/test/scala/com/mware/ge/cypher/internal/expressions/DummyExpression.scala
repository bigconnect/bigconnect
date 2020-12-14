package com.mware.ge.cypher.internal.expressions

import com.mware.ge.cypher.internal.util.symbols.TypeSpec
import com.mware.ge.cypher.internal.util.{DummyPosition, InputPosition}

case class DummyExpression(possibleTypes: TypeSpec,
                           position: InputPosition = DummyPosition(0)) extends Expression
