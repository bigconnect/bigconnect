package com.mware.ge.kvstore.raftex;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * The operation will be atomic, if the operation failed, empty string will be returned,
 * otherwise it will return the new operation's encoded string whick should be applied atomically.
 * You could implement CAS, READ-MODIFY-WRITE operations though it.
 * */
public interface AtomicOp extends Supplier<Optional<String>> {
}
