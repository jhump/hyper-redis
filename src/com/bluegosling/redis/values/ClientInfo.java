package com.bluegosling.redis.values;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

public final class ClientInfo {
   
   public enum ClientFlag {
      SLAVE_IN_MONITOR_MODE('O'),
      SLAVE('S'),
      MASTER('M'),
      IN_MULTI_EXEC('x'),
      BLOCKED('b'),
      WAITING_FOR_VM_IO('i'),
      WATCHED_KEY_INVALIDATED('d'),
      CLOSE_PENDING('c'),
      UNBLOCKED('u'),
      UNIX_DOMAIN_SOCKET('U'),
      READ_ONLY_IN_CLUSTER('r'),
      CLOSE_IMMINENT('A');
      
      private static Map<Character, ClientFlag> reverseIndex;
      static {
         ClientFlag[] flags = values();
         Map<Character, ClientFlag> map = new HashMap<>(flags.length * 4 / 3);
         for (ClientFlag f : flags) {
            map.put(f.abbreviation(), f);
         }
         reverseIndex = Collections.unmodifiableMap(map);
      }
      
      private final char ch;
      
      ClientFlag(char ch) {
         this.ch = ch;
      }
      
      public char abbreviation() {
         return ch;
      }
      
      public static ClientFlag fromAbbreviation(char ch) {
         return reverseIndex.get(ch);
      }
   }
   
   public enum FileDescriptorEvent {
      READABLE('r'), WRITABLE('w');

      private static Map<Character, FileDescriptorEvent> reverseIndex;
      static {
         FileDescriptorEvent[] events = values();
         Map<Character, FileDescriptorEvent> map = new HashMap<>(events.length * 4 / 3);
         for (FileDescriptorEvent e : events) {
            map.put(e.abbreviation(), e);
         }
         reverseIndex = Collections.unmodifiableMap(map);
      }
      
      private final char ch;
      
      FileDescriptorEvent(char ch) {
         this.ch = ch;
      }
      
      public char abbreviation() {
         return ch;
      }
      
      public static FileDescriptorEvent fromAbbreviation(char ch) {
         return reverseIndex.get(ch);
      }
   }
   
   public static List<ClientInfo> fromClientListReply(String reply) {
      List<ClientInfo> ret = new ArrayList<>();
      int fromIndex = 0;
      while (fromIndex < reply.length()) {
         int nextIndex;
         int pos = reply.indexOf('\n', fromIndex);
         if (pos < 0) {
            nextIndex = pos = reply.length();
         } else {
            nextIndex = pos + 1;
            if (pos > 1 && reply.charAt(pos - 1) == '\r') {
               pos--;
            }
         }
         String line = reply.substring(fromIndex, pos);
         if (!line.isEmpty()) {
            ret.add(fromClientListLine(line));
         }
         fromIndex = nextIndex;
      }
      return Collections.unmodifiableList(ret);
   }
   
   private static ClientInfo fromClientListLine(String line) {
      Map<String, String> attributes = new LinkedHashMap<>();
      int fromIndex = 0;
      while (fromIndex < line.length()) {
         int pos = line.indexOf(' ', fromIndex);
         if (pos < 0) {
            pos = line.length();
         }
         if (pos - fromIndex > 0) {
            int sep = line.indexOf('=', fromIndex);
            String k, v;
            if (sep < 0 || sep > pos) {
               // wha?? gracefully handle as a pair w/ a key but no value
               k = line.substring(fromIndex, pos);
               v = "";
            } else {
               k = line.substring(fromIndex, sep);
               v = line.substring(sep + 1, pos);
            }
            attributes.put(k, v);
         }
         fromIndex = pos + 1;
      }
      
      return new ClientInfo(attributes);
   }
   
   private static final Set<String> KNOWN_ATTRIBUTES;
   static {
      Set<String> attr = new LinkedHashSet<>();
      attr.add("id");
      attr.add("addr");
      attr.add("fd");
      attr.add("age");
      attr.add("idle");
      attr.add("flags");
      attr.add("db");
      attr.add("sub");
      attr.add("psub");
      attr.add("multi");
      attr.add("qbuf");
      attr.add("qbuf-free");
      attr.add("obl");
      attr.add("oll");
      attr.add("omem");
      attr.add("events");
      attr.add("cmd");
      KNOWN_ATTRIBUTES = Collections.unmodifiableSet(attr);
   }
   
   private final Map<String, String> attributes;
   
   private ClientInfo(Map<String, String> attributes) {
      this.attributes = Collections.unmodifiableMap(attributes);
   }
   
   public Map<String, String> asMap() {
      return attributes;
   }
   
   public long id() {
      return toLong("id");
   }
   
   public String address() {
      return attributes.get("addr");
   }
   
   public int socketFd() {
      return toInt("fd");
   }
   
   public long ageSeconds() {
      return toLong("age");
   }
   
   public long idleSeconds() {
      return toLong("idle");
   }
   
   public Set<ClientFlag> flags() {
      EnumSet<ClientFlag> ret = EnumSet.noneOf(ClientFlag.class);
      String flags = attributes.get("flags");
      for (int i = 0; i < flags.length(); i++) {
         char ch = flags.charAt(i);
         if (ch == 'N') {
            continue;
         }
         ClientFlag f = ClientFlag.fromAbbreviation(ch);
         if (f != null) {
            ret.add(f);
         }
      }
      return Collections.unmodifiableSet(ret);
   }
   
   public String unknownFlags() {
      String flags = attributes.get("flags");
      StringBuilder sb = new StringBuilder(flags.length());
      for (int i = 0; i < flags.length(); i++) {
         char ch = flags.charAt(i);
         if (ClientFlag.fromAbbreviation(ch) == null) {
            sb.append(ch);
         }
      }
      return sb.toString();
   }

   public int db() {
      return toInt("db");
   }
   
   public int numSubscriptions() {
      return toInt("sub");
   }
   
   public int numPatternSubscriptions() {
      return toInt("psub");
   }
   
   public int numCommandsInMultiExec() {
      return toInt("multi");
   }
   
   public int queryBufferLength() {
      return toInt("qbuf");
   }

   public int queryBufferFree() {
      return toInt("qbuf-free");
   }
  
   public int outputBufferLength() {
      return toInt("obl");
   }

   public int outputReplyQueueLength() {
      return toInt("oll");
   }
   
   public int outputBufferMemoryUsage() {
      return toInt("omem");
   }
   
   public Set<FileDescriptorEvent> fdEvents() {
      EnumSet<FileDescriptorEvent> ret = EnumSet.noneOf(FileDescriptorEvent.class);
      String flags = attributes.get("events");
      for (int i = 0; i < flags.length(); i++) {
         char ch = flags.charAt(i);
         FileDescriptorEvent e = FileDescriptorEvent.fromAbbreviation(ch);
         if (e != null) {
            ret.add(e);
         }
      }
      return Collections.unmodifiableSet(ret);
   }
   
   public String unknownFdEvents() {
      String flags = attributes.get("events");
      StringBuilder sb = new StringBuilder(flags.length());
      for (int i = 0; i < flags.length(); i++) {
         char ch = flags.charAt(i);
         if (FileDescriptorEvent.fromAbbreviation(ch) == null) {
            sb.append(ch);
         }
      }
      return sb.toString();
   }
   
   public String lastCommand() {
      return attributes.get("cmd");
   }
   
   public Set<String> unknownAttributes() {
      return Sets.difference(attributes.keySet(), KNOWN_ATTRIBUTES);
   }
   
   private long toLong(String key) {
      String v = attributes.get(key);
      return v == null ? -1 : Long.valueOf(v);
   }

   private int toInt(String key) {
      String v = attributes.get(key);
      return v == null ? -1 : Integer.valueOf(v);
   }
   
   // TODO: hashCode, equals, etc
}
