package com.bluegosling.redis.transaction;

import com.bluegosling.redis.RedisException;

/**
 * An exception that indicates that a transaction is {@linkplain TransactionController#discard()
 * discarded}. Any {@linkplain Promise promises} made for queued operations are broken with this
 * kind of exception.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class TransactionDiscardedException extends RedisException {
   private static final long serialVersionUID = 6418244200346159540L;
}
