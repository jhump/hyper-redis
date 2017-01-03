package com.bluegosling.redis.transaction;

import com.bluegosling.redis.RedisException;

/**
 * An exception thrown when a transaction is aborted due to interference. Interference occurs when
 * a {@linkplain Watcher watched} key is modified before the transaction can complete.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class TransactionAbortedException extends RedisException {
   private static final long serialVersionUID = 8476745941616994598L;
}
