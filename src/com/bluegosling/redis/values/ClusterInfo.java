package com.bluegosling.redis.values;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

public final class ClusterInfo {
   
   public static ClusterInfo fromClusterInfoReply(String reply) {
      Map<String, String> attributes = new LinkedHashMap<>();
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
         if (pos - fromIndex > 0) {
            int sep = reply.indexOf(':', fromIndex);
            String k, v;
            if (sep < 0 || sep > pos) {
               // wha?? gracefully handle as a pair w/ a key but no value
               k = reply.substring(fromIndex, pos);
               v = "";
            } else {
               k = reply.substring(fromIndex, sep);
               v = reply.substring(sep + 1, pos);
            }
            attributes.put(k, v);
         }
         fromIndex = nextIndex;
      }
      
      return new ClusterInfo(attributes);      
   }
   
   private static final Set<String> KNOWN_ATTRIBUTES;
   static {
      Set<String> attr = new LinkedHashSet<>();
      attr.add("cluster_state");
      attr.add("cluster_slots_assigned");
      attr.add("cluster_slots_ok");
      attr.add("cluster_slots_pfail");
      attr.add("cluster_slots_fail");
      attr.add("cluster_known_nodes");
      attr.add("cluster_size");
      attr.add("cluster_current_epoch");
      attr.add("cluster_my_epoch");
      attr.add("cluster_stats_messages_sent");
      attr.add("cluster_stats_messages_received");
      KNOWN_ATTRIBUTES = Collections.unmodifiableSet(attr);
   }
   
   private final Map<String, String> attributes;
   
   private ClusterInfo(Map<String, String> attributes) {
      this.attributes = Collections.unmodifiableMap(attributes);
   }
   
   public boolean clusterOk() {
      String state = attributes.get("cluster_state");
      return "ok".equals(state);
   }
   
   public int numSlotsAssigned() {
      return toInt("cluster_slots_assigned");
   }
   
   public int numSlotsOk() {
      return toInt("cluster_slots_ok");
   }
   
   public int numSlotsPfail() {
      return toInt("cluster_slots_pfail");
   }
   
   public int numSlotsFail() {
      return toInt("cluster_slots_fail");
   }
   
   public int numKnownNodes() {
      return toInt("cluster_known_nodes");
   }
   
   public int clusterSize() {
      return toInt("cluster_size");
   }
   
   public int clusterEpoch() {
      return toInt("cluster_current_epoch");
   }
   
   public int localEpoch() {
      return toInt("cluster_my_epoch");
   }
   
   public long numStatsMessagesSent() {
      return toLong("cluster_stats_messages_sent");
   }
   
   public long numStatsMessagesReceived() {
      return toLong("cluster_stats_messages_received");
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
