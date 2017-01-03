package com.bluegosling.redis.concurrent;

import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

/**
 * A simple FIFO queue backed by a fixed-size array. Unlike {@link ArrayDeque}, this queue supports
 * {@code null} elements.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 *
 * @param <E> the type of element in the queue
 */
class ArrayQueue<E> extends AbstractQueue<E> {
   private final E[] data;
   private int offs;
   private int size;

   ArrayQueue(int size) {
      @SuppressWarnings("unchecked")
      E[] elements = (E[]) new Object[size];
      this.data = elements;
   }

   @Override
   public int size() {
      return size;
   }

   @Override
   public boolean offer(E e) {
      int l = data.length;
      if (size == l) {
         return false;
      }
      int idx = size + offs;
      if (idx >= l) {
         idx -= l;
      }
      data[idx] = e;
      size++;
      return true;
   }

   @Override
   public E poll() {
      if (size == 0) {
         return null;
      }
      E ret = data[offs];
      offs++;
      int l = data.length;
      if (offs >= l) {
         offs -= l;
      }
      size--;
      return ret;
   }

   @Override
   public E peek() {
      if (size == 0) {
         return null;
      }
      return data[offs];
   }
   
   @Override
   public void clear() {
      size = offs = 0;
      Arrays.fill(data, null);
   }

   @Override
   public Iterator<E> iterator() {
      return new Iterator<E>() {
         final int offs = ArrayQueue.this.offs;
         final int size = ArrayQueue.this.size;
         int index;
         
         private void checkMod() {
            if (this.offs != ArrayQueue.this.offs
                  || this.size != ArrayQueue.this.size) {
               throw new ConcurrentModificationException();
            }
         }
         
         @Override
         public boolean hasNext() {
            checkMod();
            return index < size;
         }

         @Override
         public E next() {
            checkMod();
            int i = index + offs;
            int l = data.length;
            if (i >= l) {
               i -= l;
            }
            index++;
            return data[i];
         }
      };
   }
}
