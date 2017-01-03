package com.bluegosling.redis.values;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

public final class ClusterNodeInfo {
   
   public enum NodeFlag {
      LOCAL("myself"),
      MASTER("master"),
      SLAVE("slave"),
      PFAIL("fail?"),
      FAIL("fail"),
      HANDSHAKE("handshake"),
      NO_ADDRESS("noaddr");
      
      private static Map<String, NodeFlag> reverseIndex;
      static {
         NodeFlag[] flags = values();
         Map<String, NodeFlag> map = new HashMap<>(flags.length * 4 / 3);
         for (NodeFlag f : flags) {
            map.put(f.flagName(), f);
         }
         reverseIndex = Collections.unmodifiableMap(map);
      }
      
      private final String flag;
      
      NodeFlag(String flag) {
         this.flag = flag;
      }
      
      public String flagName() {
         return flag;
      }
      
      public static NodeFlag lookup(String flagName) {
         return reverseIndex.get(flagName);
      }
   }

   public static List<ClusterNodeInfo> fromClusterNodesReply(String reply) {
      List<ClusterNodeInfo> ret = new ArrayList<>();
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
            ret.add(new ClusterNodeInfo(line.split(" ")));
         }
         fromIndex = nextIndex;
      }
      return Collections.unmodifiableList(ret);
   }
   
   private final String nodeId;
   private final String address;
   private final Set<NodeFlag> flags;
   private final Set<String> unknownFlags;
   private final String masterNodeId;
   private final Instant lastPingSent;
   private final Instant lastPongReceived;
   private final int nodeEpoch;
   private final boolean linkConnected;
   private final RangeSet<Long> slots;
   
   private ClusterNodeInfo(String[] elements) {
      this.nodeId = elements[0];
      this.address = elements[1];
      Set<NodeFlag> flags = EnumSet.noneOf(NodeFlag.class);
      Set<String> unknownFlags = new LinkedHashSet<>();
      String[] flagEntries = elements[2].split(",");
      for (String f : flagEntries) {
         if ("noflags".equals(f)) {
            continue;
         }
         NodeFlag nodeFlag = NodeFlag.lookup(f);
         if (nodeFlag != null) {
            flags.add(nodeFlag);
         } else {
            unknownFlags.add(f);
         }
      }
      this.flags = Collections.unmodifiableSet(flags);
      this.unknownFlags = Collections.unmodifiableSet(unknownFlags);
      this.masterNodeId = elements[3];
      this.lastPingSent = Instant.ofEpochMilli(Long.valueOf(elements[4]));
      this.lastPongReceived = Instant.ofEpochMilli(Long.valueOf(elements[5]));
      this.nodeEpoch = Integer.valueOf(elements[6]);
      this.linkConnected = "connected".equals(elements[7]);
      ImmutableRangeSet.Builder<Long> slots = ImmutableRangeSet.builder();
      for (int i = 8; i < elements.length; i++) {
         if (elements[i].indexOf('-') >= 0) {
            // slot range
            String[] bounds = elements[i].split("-", 2);
            Long lower = Long.valueOf(bounds[0]);
            Long upper = Long.valueOf(bounds[1]);
            slots.add(Range.closed(lower, upper));
         } else {
            // single slot
            Long l = Long.valueOf(elements[i]);
            slots.add(Range.closed(l, l));
         }
      }
      this.slots = slots.build();
   }
   
   public String nodeId() {
      return nodeId;
   }

   public String address() {
      return address;
   }

   public Set<NodeFlag> flags() {
      return flags;
   }

   public Set<String> unknownFlags() {
      return unknownFlags;
   }

   public String masterNodeId() {
      return masterNodeId;
   }

   public Instant lastPingSent() {
      return lastPingSent;
   }

   public Instant lastPongReceived() {
      return lastPongReceived;
   }

   public int nodeEpoch() {
      return nodeEpoch;
   }

   public boolean linkConnected() {
      return linkConnected;
   }

   public RangeSet<Long> slots() {
      return slots;
   }

   // TODO: hashCode, equals, etc
}
