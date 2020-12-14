package com.mware.ge.cypher.internal.logical

import com.mware.ge.cypher.internal.logical.plans.Bound
import com.mware.ge.cypher.internal.util.NonEmptyList

package object plans {
  type Bounds[+V] = NonEmptyList[Bound[V]]
}
