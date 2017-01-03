package com.bluegosling.redis.values;

import static java.util.Objects.requireNonNull;
import java.util.Collections;
import java.util.List;

public final class ScanResult<T> {
   private final ScanCursor cursor;
   private final List<T> data;
   
   public ScanResult(ScanCursor cursor, List<T> data) {
      this.cursor = requireNonNull(cursor);
      this.data = Collections.unmodifiableList(data);
   }
   
   public boolean hasMoreResults() {
      return !"0".equals(cursor);
   }
   
   public ScanCursor nextCursor() {
      return cursor;
   }
   
   public List<T> data() {
      return data;
   }

   // TODO: hashCode, equals, etc
}
