package com.bluegosling.redis.values;

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class SlowLogEntry {
   private final long id;
   private final Instant processTime;
   private final long durationMicroseconds;
   private final List<String> command;
   
   SlowLogEntry(Builder builder) {
      this.id = builder.id;
      this.processTime = builder.processTime;
      this.durationMicroseconds = builder.durationMicroseconds;
      this.command = Collections.unmodifiableList(builder.command);
   }
   
   public long id() {
      return id;
   }
   
   public Instant processTime() {
      return processTime;
   }
   
   public long durationMicroseconds() {
      return durationMicroseconds;
   }
   
   public List<String> command() {
      return command;
   }
   
   // TODO: equals, hashCode, etc
   
   public static final class Builder {
      private long id;
      private Instant processTime;
      private long durationMicroseconds;
      private ArrayList<String> command = new ArrayList<>();
      private boolean needReset;
      
      public Builder id(long id) {
         this.id = id;
         return this;
      }
      
      public Builder processTime(Instant processTime) {
         this.processTime = requireNonNull(processTime);
         return this;
      }
      
      public Builder durationMircoseconds(long durationMicros) {
         this.durationMicroseconds = durationMicros;
         return this;
      }
      
      @SuppressWarnings("unchecked")
      public Builder addCommandToken(String token) {
         if (needReset) {
            command = (ArrayList<String>) command.clone();
            needReset = false;
         }
         command.add(token);
         return this;
      }
      
      public Builder command(List<String> command) {
         if (needReset) {
            this.command = new ArrayList<>(command);
            needReset = false;
         } else {
            this.command.clear();
            this.command.addAll(command);
         }
         return this;
      }

      public SlowLogEntry build() {
         needReset = true;
         return new SlowLogEntry(this);
      }
   }
}
