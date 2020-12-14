package com.mware.ge

package object cypher {
  type ExceptionHandler = Throwable => Unit

  object ExceptionHandler {
    object default extends ExceptionHandler {
      def apply(v: Throwable): Unit = {}
    }
  }
}
