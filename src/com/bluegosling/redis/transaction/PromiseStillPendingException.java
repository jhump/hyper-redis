package com.bluegosling.redis.transaction;

/**
 * An exception that can be thrown by a {@link Promise} that is not yet complete.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class PromiseStillPendingException extends IllegalStateException {
   private static final long serialVersionUID = -7642888839388651479L;
}
