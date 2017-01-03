package com.bluegosling.redis.values;

public final class ScanCursor {
   public static final ScanCursor START = ScanCursor.of("0");
   public static final ScanCursor STOP = START;

   public static ScanCursor of(String cursor) {
      return new ScanCursor(cursor);
   }
   
   private final String cursor;
   
   private ScanCursor(String cursor) {
      this.cursor = cursor;
   }
   
   @Override public boolean equals(Object o) {
      return o instanceof ScanCursor && cursor.equals(((ScanCursor) o).cursor);
   }
   
   @Override public int hashCode() {
      return cursor.hashCode();
   }
   
   @Override public String toString() {
      return cursor;
   }
}
