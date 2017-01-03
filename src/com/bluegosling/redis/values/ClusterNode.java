package com.bluegosling.redis.values;

import java.util.Collections;
import java.util.List;

public final class ClusterNode {
   private final String address;
   private final String nodeId;
   private final List<Response> unknownAttributes;
   
   public ClusterNode(String address, String nodeId, List<Response> unknownAttributes) {
      this.address = address;
      this.nodeId = nodeId;
      this.unknownAttributes = Collections.unmodifiableList(unknownAttributes);
   }
   
   public String address() {
      return address;
   }
   
   public String nodeId() {
      return nodeId;
   }
   
   public List<Response> unknownAttributes() {
      return unknownAttributes;
   }

   // TODO: hashCode, equals, etc
}