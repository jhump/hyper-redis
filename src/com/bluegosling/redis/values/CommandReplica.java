package com.bluegosling.redis.values;

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;

public class CommandReplica {
   private final Instant timestamp;
   private final int dbIndex;
   private final String client;
   private final List<byte[]> command;
   
   private CommandReplica(Instant timestamp, int dbIndex, String client,
         List<byte[]> command) {
      this.timestamp = requireNonNull(timestamp);
      if (dbIndex < 0) {
         throw new IllegalArgumentException("dbIndex cannot be negative");
      }
      this.dbIndex = dbIndex;
      this.client = requireNonNull(client);
      this.command = requireNonNull(command);
      for (byte[] bytes : command) {
         requireNonNull(bytes);
      }
   }
   
   public Instant timestamp() {
      return timestamp;
   }
   
   public int dbIndex() {
      return dbIndex;
   }
   
   public String client() {
      return client;
   }
   
   public boolean clientIsLua() {
      return client.equalsIgnoreCase("lua");
   }
   
   public boolean clientIsUnixSocket() {
      return client.startsWith("unix:");
   }
   
   @Override public boolean equals(Object o) {
      if (o instanceof CommandReplica) {
         CommandReplica other = (CommandReplica) o;
         if (!timestamp.equals(other.timestamp) || dbIndex != other.dbIndex
               || !client.equals(other.client)) {
            return false;
         }
         if (command.size() != other.command.size()) {
            return false;
         }
         for (int i = 0; i < command.size(); i++) {
            byte[] mine = command.get(i);
            byte[] theirs = other.command.get(i);
            if (!Arrays.equals(mine, theirs)) {
               return false;
            }
         }
         return true;
      }
      return false;
   }
   
   @Override public int hashCode() {
      int hash = dbIndex;
      hash = 31 * hash + timestamp.hashCode();
      hash = 31 * hash + client.hashCode();
      for (byte[] token : command) {
         hash = 31 * hash + Arrays.hashCode(token);
      }
      return hash;
   }
   
   @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      try (Formatter fmt = new Formatter(sb)) {
         fmt.format("%d.%06d [%d %s]",
               timestamp.getEpochSecond(),
               TimeUnit.NANOSECONDS.toMicros(timestamp.getNano()),
               dbIndex,
               client);
         for (byte[] token : command) {
            sb.append(" \"");
            // print each token using the same formatting/escaping as Redis itself does
            // see sds.c, function sdscatrepr
            for (byte b : token) {
               int i = toInt(b);
               switch (i) {
               case '\\':
                  sb.append("\\\\");
                  break;
               case '"':
                  sb.append("\\\"");
                  break;
               case '\n':
                  sb.append("\\n");
                  break;
               case '\r':
                  sb.append("\\r");
                  break;
               case '\t':
                  sb.append("\\t");
                  break;
               case 7 /* '\a' */:
                  sb.append("\\a");
                  break;
               case '\b':
                  sb.append("\\b");
                  break;
               default:
                  if (i >= 32 && i <= 127) {
                     // printable
                     sb.append((char) b);
                  } else {
                     fmt.format("\\x%02x", i);
                  }
               }
            }
            sb.append('"');
         }
      }
      return sb.toString();
   }
   
   public static CommandReplica parseMonitorOutput(String monitorLine) {
      try {
         int pos = monitorLine.indexOf(' ');
         String timestampStr = monitorLine.substring(0, pos);
         double d = Double.parseDouble(timestampStr);
         Instant timestamp = Instant.ofEpochSecond(
               (long) d, (int)((d - ((long) d)) * 1_000_000_000.0));
         if (monitorLine.charAt(pos + 1) != '[') {
            throw new RuntimeException("Second token of monitor line expected to start with '['");
         }
         int nextPos = monitorLine.indexOf(']', pos + 2);
         String portions[] = monitorLine.substring(pos + 2, nextPos).split(" ", 2);
         int dbIndex = Integer.parseInt(portions[0]);
         String client = portions[1];

         if (monitorLine.charAt(nextPos + 1) != ' ' || monitorLine.charAt(nextPos + 2) != '"') {
            throw new RuntimeException("invalid characters when expecting command token #1");
         }
         pos = nextPos + 3;
         
         List<byte[]> tokens = new ArrayList<>();

         // now parse the commands
         while (pos < monitorLine.length()) {

            // find end-of-token
            int start = pos;
            while (true) {
               nextPos = monitorLine.indexOf('"', pos);
               int countSlashes = 0;
               for (int i = nextPos - 1; i > pos; i--) {
                  if (monitorLine.charAt(i) == '\\') {
                     countSlashes++;
                  } else {
                     break;
                  }
               }
               if ((countSlashes & 1) == 0) {
                  // even number means this quote is not escaped
                  break;
               }
               pos = nextPos + 1;
            }
            
            tokens.add(decode(monitorLine, start, nextPos));
            
            pos = nextPos + 1;
            if (pos < monitorLine.length()) {
               if (monitorLine.charAt(pos) != ' ' || monitorLine.charAt(pos + 1) != '"') {
                  throw new RuntimeException("invalid characters when expecting command token #"
                        + (tokens.size() + 1));
               }
               pos = pos + 2;
            }
         }
         
         return new CommandReplica(timestamp, dbIndex, client,
               Collections.unmodifiableList(tokens));
      } catch (Exception e) {
         throw new IllegalArgumentException("given line cannot be parsed", e);
      }
   }
   
   private static byte[] decode(String s, int start, int end) {
      ByteBuffer buffer = Charsets.UTF_8.encode(CharBuffer.wrap(s, start, end));
      boolean foundEscape = false;
      for (int i = 0; i < buffer.limit(); i++) {
         int ch = toInt(buffer.get(i));
         if (ch == '\\') {
            switch (toInt(buffer.get(++i))) {
            case '\\':
               buffer.put((byte) '\\');
               foundEscape = true;
               break;
            case 'a':
               buffer.put((byte) 7);
               foundEscape = true;
               break;
            case 'b':
               buffer.put((byte) '\b');
               foundEscape = true;
               break;
            case 'n':
               buffer.put((byte) '\n');
               foundEscape = true;
               break;
            case 'r':
               buffer.put((byte) '\r');
               foundEscape = true;
               break;
            case 't':
               buffer.put((byte) '\t');
               foundEscape = true;
               break;
            case 'f':
               buffer.put((byte) '\f');
               foundEscape = true;
               break;
            case '"':
               buffer.put((byte) '"');
               foundEscape = true;
               break;
            case '\'':
               buffer.put((byte) '\'');
               foundEscape = true;
               break;
            case 'x':
               // hex escape
               int hex1 = validHex(toInt(buffer.get(++i)));
               int hex2 = validHex(toInt(buffer.get(++i)));
               if (hex1 == -1 || hex2 == -1) {
                  // invalid hex sequence: be lenient and ignore, treating slash as literal slash
                  // (as if it were escaped)
                  if (foundEscape) {
                     buffer.put((byte) ch);
                  } else {
                     buffer.position(buffer.position() + 1);
                  }
                  i -= 3; // "push back" the characters we peeked
                  break;
               } else {
                  buffer.put((byte) ((hex1 << 4) | hex2));
                  foundEscape = true;
               }
               break;
            default:
               // be lenient: if escape is invalid, ignore and treat as literal slash
               if (foundEscape) {
                  buffer.put((byte) ch);
               } else {
                  buffer.position(buffer.position() + 1);
               }
               i--; // "push back" the one we peeked
            }
         } else {
            if (foundEscape) {
               buffer.put((byte) ch);
            } else {
               buffer.position(buffer.position() + 1);
            }
         }
      }
      byte[] array = new byte[buffer.position()];
      buffer.position(0);
      buffer.get(array);
      return array;
   }
   
   private static int toInt(byte b) {
      int i = b;
      i = i & 0xff; // mask away any sign-extended bits
      return i;
   }
   
   private static int validHex(int ch) {
      if (ch >= 'A' && ch <= 'F') {
         return ch - 'A' + 10;
      }
      if (ch >= 'a' && ch <= 'f') {
         return ch - 'a' + 10;
      }
      if (ch >= '0' && ch <= '9') {
         return ch - '0';
      }
      return -1; // not valid
   }
}
