package com.bluegosling.redis.concurrent;

/**
 * A simple extension of the eponymous {@link CompletableFuture} that directly, and trivially,
 * implements {@link CompletionStageFuture} (to avoid having to {@linkplain
 * CompletionStageFuture#fromCompletableFuture wrap}).
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of future value
 */
public class CompletableFuture<T> extends java.util.concurrent.CompletableFuture<T>
implements CompletionStageFuture<T> {
}
