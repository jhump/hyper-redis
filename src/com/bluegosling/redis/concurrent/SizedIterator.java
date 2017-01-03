package com.bluegosling.redis.concurrent;

import java.util.Iterator;
import java.util.OptionalInt;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An iterator that can have information about the number of elements it emits.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <T> the type of value emitted by the iterator
 */
public interface SizedIterator<T> extends Iterator<T> {
   /**
    * Returns the number of remaining elements in the iterator, if known.
    * 
    * @return the number of remaining elements in the iterator or {@linkplain OptionalInt#empty()
    *       empty} if not known
    */
   OptionalInt remainingItems();
   
   /**
    * Returns a spliterator over the elements emitted by this iterator.
    * 
    * @return a spliterator
    */
   default Spliterator<T> spliterator() {
      OptionalInt items = remainingItems();
      return items.isPresent()
         ? Spliterators.spliterator(this, items.getAsInt(), 0)
         : Spliterators.spliteratorUnknownSize(this, 0);
   }
   
   /**
    * Returns a stream for the elements emitted by this iterator.
    * 
    * @return a stream
    */
   default Stream<T> remainingAsStream() {
      return StreamSupport.stream(spliterator(), false);
   }
}
