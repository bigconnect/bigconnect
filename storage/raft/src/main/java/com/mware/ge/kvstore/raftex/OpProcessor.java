package com.mware.ge.kvstore.raftex;

import java.util.Optional;
import java.util.function.Function;

public interface OpProcessor extends Function<AtomicOp, Optional<String>> {
}
