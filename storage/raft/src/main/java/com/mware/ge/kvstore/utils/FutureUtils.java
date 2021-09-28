package com.mware.ge.kvstore.utils;

import com.mware.ge.collection.Pair;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class FutureUtils {
    // A future aggregator. The aggregator uses an evaluator to evaluate the future results.
    // The evaluator takes two params: idx and T:
    //      idx: is the position of the future being
    //      T: the value of the future
    //
    // Only the future having value will be checked by the evaluator.
    // Futures having exceptions will be considered as failure
    // The aggregator returns a future with an array of pairs<Integer, T>
    // The first element of the pair is the index to the given futures, and the second is the value of the succeeded future
    // The returned future fulfils when the given number of futures succeed, or when all given futures fulfil.
    // In either case, an array of succeeded values will be returned
    public static <T> CompletableFuture<List<Pair<Integer, T>>> collectNSucceeded(
            List<CompletableFuture<T>> futures,
            int minRequired,
            Function<Pair<Integer, T>, Boolean> evaluator
    ) {
        if (minRequired == 0)
            return CompletableFuture.completedFuture(Collections.emptyList());

        int total = futures.size();
        if (total < minRequired) {
            return CompletableFuture.failedFuture(new RuntimeException("Not enough futures"));
        }

        Context<T> ctx = new Context<>(total, evaluator);

        // for each succeeded Future, add to the result list, until
        // we have required number of futures, at which point we fulfil
        // the promise with the result list
        for (int index = 0; index < futures.size(); index++) {
            CompletableFuture<T> future = futures.get(index);
            final int idx = index;
            future.thenAccept(t -> {
                if (ctx.promise.isDone()) {
                    if (!(t instanceof Throwable)) {
                        if (ctx.eval.apply(Pair.of(idx, t))) {
                            ctx.nSucceeded.getAndIncrement();
                        }
                        ctx.results.add(Pair.of(idx, t));
                    }
                    if (ctx.numCompleted.incrementAndGet() == ctx.nTotal || ctx.nSucceeded.get() == minRequired) {
                        // Done
                        ctx.promise.complete(ctx.results);
                    }
                }
            });
        }

        return ctx.promise;
    }

    static class Context<T> {
        Function<Pair<Integer, T>, Boolean> eval;
        List<Pair<Integer, T>> results;
        AtomicInteger numCompleted = new AtomicInteger(0);
        AtomicInteger nSucceeded = new AtomicInteger(0);
        int nTotal;
        CompletableFuture<List<Pair<Integer, T>>> promise = new CompletableFuture<>();

        public Context(int total, Function<Pair<Integer, T>, Boolean> evaluator) {
            this.nTotal = total;
            this.eval = evaluator;
        }
    }
}
