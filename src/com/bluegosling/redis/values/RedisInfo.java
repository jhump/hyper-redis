package com.bluegosling.redis.values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;

import com.bluegosling.redis.RedisVersion;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

public final class RedisInfo {
   private static final Splitter COMMA_SPLITTER = Splitter.on(',');
   
   private static final String SERVER = "server";
   private static final String CLIENTS = "clients";
   private static final String MEMORY = "memory";
   private static final String PERSISTENCE = "persistence";
   private static final String STATS = "stats";
   private static final String REPLICATION = "replication";
   private static final String CPU = "cpu";
   private static final String COMMAND_STATS = "commandstats";
   private static final String CLUSTER = "cluster";
   private static final String KEYSPACE = "keyspace";
   
   public static RedisInfo fromInfoReply(String reply) {
      return parseInfoReply(reply, RedisInfo::new);
   }

   public static RedisInfo.Section fromInfoSectionReply(String sectionName, String reply) {
      return parseInfoReply(reply,
            (values, sections) -> createSection(sectionName, values, values));
   }
   
   private static <T> T parseInfoReply(String reply,
         BiFunction<NavigableMap<String, String>, SetMultimap<String, String>, T> factory) {
      Comparator<String> comp = String::compareToIgnoreCase;
      NavigableMap<String, String> keysAndValues = new TreeMap<>(comp);
      SetMultimap<String, String> sectionKeys =
            Multimaps.newSetMultimap(new TreeMap<>(comp), () -> new TreeSet<>(comp));
      try (BufferedReader in = new BufferedReader(new StringReader(reply))) {
         String currentSection = null;
         String line;
         while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
               continue;
            }
            if (line.charAt(0) == '#') {
               currentSection = line.substring(1).trim();
               continue;
            }
            int pos = line.indexOf(':');
            String key, value;
            if (pos == -1) {
               // WTF? Shouldn't actually happen...
               key = line;
               value = "";
            } else {
               key = line.substring(0, pos).trim();
               value = line.substring(pos + 1).trim();
            }
            keysAndValues.put(key, value);
            if (currentSection != null) {
               sectionKeys.put(currentSection, key);
            }
         }
      } catch (IOException e) {
         // Should not actually be possible since reader is backed by string
         throw new AssertionError(e);
      }
      return factory.apply(keysAndValues, sectionKeys);
   }
   
   private static RedisInfo.Section createSection(String sectionName,
         Map<String, String> keysAndValues, NavigableMap<String, String> all) {
      switch (sectionName.toLowerCase()) {
      case SERVER:
         return new ServerSection(keysAndValues, all);
      case CLIENTS:
         return new ClientsSection(keysAndValues, all);
      case MEMORY:
         return new MemorySection(keysAndValues, all);
      case PERSISTENCE:
         return new PersistenceSection(keysAndValues, all);
      case STATS:
         return new StatsSection(keysAndValues, all);
      case REPLICATION:
         return new ReplicationSection(keysAndValues, all);
      case CPU:
         return new CpuSection(keysAndValues, all);
      case COMMAND_STATS:
         return new CommandStatsSection(keysAndValues, all);
      case CLUSTER:
         return new ClusterSection(keysAndValues, all);
      case KEYSPACE:
         return new KeyspaceSection(keysAndValues, all);
      default:
         return new Section(keysAndValues);
      }
   }

   private final NavigableMap<String, String> allValues;
   private final SetMultimap<String, String> allSections;
   
   RedisInfo(NavigableMap<String, String> allValues, SetMultimap<String, String> allSections) {
      this.allValues = Collections.unmodifiableNavigableMap(allValues);
      this.allSections = Multimaps.unmodifiableSetMultimap(allSections);
   }
   
   public Map<String, String> asMap() {
      return allValues;
   }
   
   public Set<String> sectionNames() {
      return allSections.keySet();
   }

   public Section getSection(String sectionName) {
      return createSection(sectionName, sectionMap(sectionName), allValues);
   }

   public ServerSection serverSection() {
      return new ServerSection(sectionMap(SERVER), allValues);
   }

   public ClientsSection clientsSection() {
      return new ClientsSection(sectionMap(CLIENTS), allValues);
   }

   public MemorySection memorySection() {
      return new MemorySection(sectionMap(MEMORY), allValues);
   }

   public PersistenceSection persistenceSection() {
      return new PersistenceSection(sectionMap(PERSISTENCE), allValues);
   }

   public StatsSection statsSection() {
      return new StatsSection(sectionMap(STATS), allValues);
   }

   public ReplicationSection replicationSection() {
      return new ReplicationSection(sectionMap(REPLICATION), allValues);
   }

   public CpuSection cpuSection() {
      return new CpuSection(sectionMap(CPU), allValues);
   }

   public CommandStatsSection commandStatsSection() {
      return new CommandStatsSection(sectionMap(COMMAND_STATS), allValues);
   }

   public ClusterSection clusterSection() {
      return new ClusterSection(sectionMap(CLUSTER), allValues);
   }

   public KeyspaceSection keyspaceSection() {
      return new KeyspaceSection(sectionMap(KEYSPACE), allValues);
   }

   private Map<String, String> sectionMap(String section) {
      Set<String> sectionKeys = allSections.get(section);
      return sectionKeys.isEmpty()
            ? Collections.emptyMap()
            : new SectionMap(sectionKeys, allValues);
   }

   public static class Section {
      private final Map<String, String> values;
      
      Section(Map<String, String> values) {
         this.values = values;
      }
      
      public Map<String, String> asMap() {
         return values;
      }
   }
   
   private abstract static class SpecializedSection extends Section {
      final NavigableMap<String, String> all;
      
      SpecializedSection(Map<String, String> values, NavigableMap<String, String> allValues) {
         super(values);
         this.all = allValues;
      }
      
      String asString(String key) {
         return all.get(key);
      }
      
      boolean asBoolean(String key) {
         String fromMap = all.get(key);
         if (fromMap == null) {
            return false;
         }
         if ("1".equals(fromMap)) {
            return true;
         }
         if ("0".equals(fromMap)) {
            return false;
         }
         throw new NumberFormatException("Expecting 0 or 1, got " + fromMap);
      }
      
      int asInt(String key) {
         String fromMap = all.get(key);
         return fromMap == null ? -1 : Integer.parseInt(fromMap);
      }
      
      long asLong(String key) {
         String fromMap = all.get(key);
         return fromMap == null ? -1 : Long.parseLong(fromMap);
      }
      
      double asDouble(String key) {
         String fromMap = all.get(key);
         return fromMap == null ? Double.NaN : Long.parseLong(fromMap);
      }
      
      Map<String, String> asMap(String key) {
         String fromMap = all.get(key);
         if (fromMap == null) {
            return null;
         }
         TreeMap<String, String> map = new TreeMap<>(String::compareToIgnoreCase);
         for (String str : COMMA_SPLITTER.split(fromMap)) {
            int pos = str.indexOf('=');
            if (pos == -1) {
               map.put(str, "");
            } else {
               map.put(str.substring(0, pos), str.substring(pos + 1));
            }
         }
         return map;
      }
   }
   
   public static final class ServerSection extends SpecializedSection {
      ServerSection(Map<String, String> values, NavigableMap<String, String> allValues) {
         super(values, allValues);
      }
      
      public RedisVersion redisVersion() {
         String str = asString("redis_version");
         return str == null ? null : RedisVersion.parse(str);
      }
      
      public String redisGitSha1() {
         return asString("redis_git_sha1");
      }
      
      public boolean redisGitDirty() {
         return asBoolean("redis_git_dirty");
      }
      
      public String redisBuildId() {
         return asString("redis_build_id");
      }
      
      public String operatingSystem() {
         return asString("os");
      }
      
      public int architectureBits() {
         return asInt("arch_bits");
      }
      
      public String multiplexingApi() {
         return asString("multiplexing_api");
      }
      
      public String gccVersion() {
         return asString("gcc_version");
      }
      
      public long processId() {
         return asLong("process_id");
      }
      
      public String runId() {
         return asString("run_id");
      }
      
      public int tcpPort() {
         return asInt("tcp_port");
      }
      
      public long uptimeInSeconds() {
         return asLong("uptime_in_seconds");
      }
      
      public long uptimeInDays() {
         return asLong("uptime_in_days");
      }
      
      public long lruClock() {
         return asLong("lru_clock");
      }
      
      public String configFile() {
         return asString("config_file");
      }
   }

   public static final class ClientsSection extends SpecializedSection {
      ClientsSection(Map<String, String> values, NavigableMap<String, String> allValues) {
         super(values, allValues);
      }

      public int connectedClients() {
         return asInt("connected_clients");
      }

      public long clientLongestOutputList() {
         return asLong("client_longest_output_list");
      }

      public long clientBiggestInputBuffer() {
         return asLong("client_biggest_input_buf");
      }

      public int blockedClients() {
         return asInt("blocked_clients");
      }
   }

   public static final class MemorySection extends SpecializedSection {
      MemorySection(Map<String, String> values, NavigableMap<String, String> allValues) {
         super(values, allValues);
      }

      public long usedMemoryBytes() {
         return asLong("used_memory");
      }
      
      public String usedMemoryHuman() {
         return asString("used_memory_human");
      }

      public long usedMemoryRssBytes() {
         return asLong("used_memory_rss");
      }
      
      public long usedMemoryPeakBytes() {
         return asLong("used_memory_peak");
      }
      
      public String usedMemoryPeakHuman() {
         return asString("used_memory_peak_human");
      }

      public long usedMemoryLuaBytes() {
         return asLong("used_memory_lua");
      }
      
      public double memoryFragmentationRatio() {
         return asDouble("mem_fragmentation_ratio");
      }
      
      public String memoryAllocator() {
         return asString("mem_allocator");
      }
   }

   public static final class PersistenceSection extends SpecializedSection {
      PersistenceSection(Map<String, String> values, NavigableMap<String, String> allValues) {
         super(values, allValues);
      }
      
      public boolean loading() {
         return asBoolean("loading");
      }
      
      public Instant loadingStartTime() {
         long l = asLong("loading_start_time");
         return l == -1 ? null : Instant.ofEpochSecond(l);
      }
      
      public long loadingTotalBytes() {
         return asLong("loading_total_bytes");
      }
      
      public long loadingLoadedBytes() {
         return asLong("loading_loaded_bytes");
      }
      
      public double loadingLoadedPercentage() {
         return asDouble("loading_loaded_perc");
      }
      
      public long loadingEtaSeconds() {
         return asLong("loading_eta_seconds");
      }
      
      public long rdbChangesSinceLastSave() {
         return asLong("rdb_changes_since_last_save");
      }
      
      public boolean rdbBackgroundSaveInProgress() {
         return asBoolean("rdb_bgsave_in_progress");
      }
      
      public Instant rdbLastSaveTime() {
         long l = asLong("rdb_last_save_time");
         return l == -1 ? null : Instant.ofEpochSecond(l);
      }
      
      public String rdbLastBackgroundSaveStatus() {
         return asString("rdb_last_bgsave_status");
      }
      
      public int rdbLastBackgroundSaveTimeSeconds() {
         return asInt("rdb_last_bgsave_time_sec");
      }

      public int rdbCurrentBackgroundSaveTimeSeconds() {
         return asInt("rdb_current_bgsave_time_sec");
      }
      
      public boolean aofEnabled() {
         return asBoolean("aof_enabled");
      }
      
      public boolean aofRewriteInProgress() {
         return asBoolean("aof_rewrite_in_progress");
      }

      public boolean aofRewriteScheduled() {
         return asBoolean("aof_rewrite_scheduled");
      }

      public int aofLastRewriteTimeSeconds() {
         return asInt("aof_last_rewrite_time_sec");
      }

      public int aofCurrentRewriteTimeSeconds() {
         return asInt("aof_current_rewrite_time_sec");
      }
      
      public String aofLastRewriteStatus() {
         return asString("aof_last_bgrewrite_status");
      }
      
      public String aofLastWriteStatus() {
         return asString("aof_last_write_status");
      }
      
      public long aofCurrentSizeBytes() {
         return asLong("aof_current_size");
      }
      
      public long aofBaseSizeBytes() {
         return asLong("aof_base_size");
      }
      
      public boolean aofPendingRewrite() {
         return asBoolean("aof_pending_rewrite");
      }
      
      public long aofBufferLengthBytes() {
         return asLong("aof_buffer_length");
      }

      public long aofRewriteBufferLengthBytes() {
         return asLong("aof_rewrite_buffer_length");
      }
      
      public int aofPendingBackgroundFsyncs() {
         return asInt("aof_pending_bio_fsync");
      }
      
      public long aofDelayedFsyncs() {
         return asLong("aof_delayed_fsync");
      }
   }

   public static final class StatsSection extends SpecializedSection {
      StatsSection(Map<String, String> values, NavigableMap<String, String> allValues) {
         super(values, allValues);
      }
      
      public long totalConnectionsReceived() {
         return asLong("total_connections_received");
      }
      
      public long totalCommandsProcessed() {
         return asLong("total_commands_processed");
      }
      
      public int instantaneousOpsPerSecond() {
         return asInt("instantaneous_ops_per_sec");
      }
      
      public long totalNetInputBytes() {
         return asLong("total_net_input_bytes");
      }

      public long totalNetOutputBytes() {
         return asLong("total_net_output_bytes");
      }
      
      public double instantaneousInputKilobytesPerSecond() {
         return asDouble("instantaneous_input_kbps");
      }

      public double instantaneousOutputKilobytesPerSecond() {
         return asDouble("instantaneous_output_kbps");
      }

      public long rejectedConnections() {
         return asLong("rejected_connections");
      }
      
      public long expiredKeys() {
         return asLong("expired_keys");
      }
      
      public long evictedKeys() {
         return asLong("evicted_keys");
      }
      
      public long keyspaceHits() {
         return asLong("keyspace_hits");
      }
      
      public long keyspaceMisses() {
         return asLong("keyspace_misses");
      }
      
      public int pubsubChannels() {
         return asInt("pubsub_channels");
      }
      
      public int pubsubPatterns() {
         return asInt("pubsub_patterns");
      }
      
      public long latestForkMicroseconds() {
         return asLong("latest_fork_usec");
      }
   }
   
   public static final class ReplicationSection extends SpecializedSection {
      ReplicationSection(Map<String, String> values, NavigableMap<String, String> allValues) {
         super(values, allValues);
      }
      
      public RedisRole role() {
         return RedisRole.fromRoleName(asString("role"));
      }
      
      public String masterHost() {
         return asString("master_host");
      }
      
      public int masterPort() {
         return asInt("master_port");
      }
      
      public String masterLinkStatus() {
         return asString("master_link_status");
      }
      
      public long masterLastIoSecondsAgo() {
         return asLong("master_last_io_seconds_ago");
      }
      
      public long masterLinkDownSinceSeconds() {
         return asLong("master_link_down_since_seconds");
      }
      
      public boolean masterSyncInProgress() {
         return asBoolean("master_sync_in_progress");
      }
      
      public long masterSyncBytesLeft() {
         return asLong("master_sync_left_bytes");
      }

      public long masterSyncLastIoSecondsAgo() {
         return asLong("master_sync_last_io_seconds_ago");
      }
      
      public int connectedSlaves() {
         return asInt("connected_slaves");
      }
      
      public SlaveInfo slave(int index) {
         String str = asString("slave" + index);
         return str == null ? null : new SlaveInfo(str);
      }
      
      public List<SlaveInfo> slaves() {
         int count = connectedSlaves();
         if (count <= 0) {
            return Collections.emptyList();
         }
         List<SlaveInfo> info = new ArrayList<>(count);
         for (int i = 0; i < count; i++) {
            SlaveInfo s = slave(i);
            if (info != null) {
               info.add(s);
            }
         }
         return Collections.unmodifiableList(info);
      }
      
      public static final class SlaveInfo {
         private final String id;
         private final String address;
         private final int port;
         private final String status;
         
         SlaveInfo(String line) {
            String[] parts = line.split(",", 4);
            id = parts.length > 0 ? parts[0] : "";
            address = parts.length > 1 ? parts[1] : "";
            port = parts.length > 2 ? Integer.parseInt(parts[2]) : -1;
            status = parts.length > 3 ? parts[3] : null;
         }
         
         public String id() {
            return id;
         }
         
         public String address() {
            return address;
         }
         
         public int port() {
            return port;
         }
         
         public String status() {
            return status;
         }
      }
   }
   
   public static final class CpuSection extends SpecializedSection {
      CpuSection(Map<String, String> values, NavigableMap<String, String> allValues) {
         super(values, allValues);
      }
      
      public double usedCpuSystem() {
         return asDouble("used_cpu_sys");
      }

      public double usedCpuUser() {
         return asDouble("used_cpu_user");
      }

      public double usedCpuSystemChildren() {
         return asDouble("used_cpu_sys_children");
      }

      public double usedCpuUserChildren() {
         return asDouble("used_cpu_user_children");
      }
   }

   public static final class CommandStatsSection extends SpecializedSection {
      CommandStatsSection(Map<String, String> values, NavigableMap<String, String> allValues) {
         super(values, allValues);
      }
      
      public CommandStats commandStats(String command) {
         Map<String, String> map = asMap("cmdstat_" + command);
         return map == null ? null : new CommandStats(map);
      }
      
      public Map<String, CommandStats> commandStats() {
         Map<String, CommandStats> results = new LinkedHashMap<>();
         for (String key : all.tailMap("cmdstat_", false).keySet()) {
            if (!key.toLowerCase().startsWith("cmdstat_")) {
               break;
            }
            String cmd = key.substring("cmdstat_".length());
            results.put(cmd, new CommandStats(asMap(key)));
         }
         return Collections.unmodifiableMap(results);
      }
      
      public static final class CommandStats {
         private final Map<String, String> data;
         
         CommandStats(Map<String, String> data) {
            this.data = Collections.unmodifiableMap(data);
         }
         
         public long calls() {
            String str = data.get("calls");
            return str == null ? -1 : Long.parseLong(str);
         }
         
         public long totalMicroseconds() {
            String str = data.get("usec");
            return str == null ? -1 : Long.parseLong(str);
         }
         
         public long microsecondsPerCall() {
            String str = data.get("usec_per_call");
            return str == null ? -1 : Long.parseLong(str);
         }
         
         public Map<String, String> asMap() {
            return data;
         }
      }
   }

   public static final class ClusterSection extends SpecializedSection {
      ClusterSection(Map<String, String> values, NavigableMap<String, String> allValues) {
         super(values, allValues);
      }
      
      public boolean clusterEnabled() {
         return asBoolean("cluster_enabled");
      }
   }

   public static final class KeyspaceSection extends SpecializedSection {
      KeyspaceSection(Map<String, String> values, NavigableMap<String, String> allValues) {
         super(values, allValues);
      }
      
      public DatabaseInfo db(int dbIndex) {
         Map<String, String> map = asMap("db" + dbIndex);
         return map == null ? null : new DatabaseInfo(map);
      }
      
      public Map<Integer, DatabaseInfo> dbs() {
         Map<Integer, DatabaseInfo> results = new LinkedHashMap<>();
         for (String key : all.tailMap("db", false).keySet()) {
            if (!key.toLowerCase().startsWith("db")) {
               break;
            }
            int index;
            try {
               index = Integer.parseInt(key.substring("db".length()));
            } catch (NumberFormatException e) {
               continue;
            }
            results.put(index, new DatabaseInfo(asMap(key)));
         }
         return Collections.unmodifiableMap(results);
      }
      
      public static final class DatabaseInfo {
         private final Map<String, String> data;
         
         DatabaseInfo(Map<String, String> data) {
            this.data = Collections.unmodifiableMap(data);
         }
         
         public long keys() {
            String str = data.get("keys");
            return str == null ? -1 : Long.parseLong(str);
         }
         
         public long expiringKeys() {
            String str = data.get("expires");
            return str == null ? -1 : Long.parseLong(str);
         }
         
         public long averageTtlSeconds() {
            String str = data.get("avg_ttl");
            return str == null ? -1 : Long.parseLong(str);
         }
         
         public Map<String, String> asMap() {
            return data;
         } 
      }
   }

   private static class SectionMap extends AbstractMap<String, String> {
      private final Set<String> keys;
      private final Map<String, String> all;
      
      SectionMap(Set<String> keys, Map<String, String> all) {
         this.keys = keys;
         this.all = all;
      }

      @Override
      public int size() {
         return keys.size();
      }

      @Override
      public String get(Object key) {
         return keys.contains(key) ? all.get(key) : null;
      }

      @Override
      public String put(String key, String value) {
         throw new UnsupportedOperationException();
      }

      @Override
      public String remove(Object key) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void putAll(Map<? extends String, ? extends String> m) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
         throw new UnsupportedOperationException();
      }

      @Override
      public Set<String> keySet() {
         return keys;
      }

      @Override
      public Collection<String> values() {
         return new AbstractCollection<String>() {
            @Override
            public Iterator<String> iterator() {
               return Iterators.transform(keys.iterator(), all::get);
            }

            @Override
            public int size() {
               return keys.size();
            }

            @Override
            public boolean addAll(Collection<? extends String> c) {
               throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(Object o) {
               throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(Collection<?> c) {
               throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(Collection<?> c) {
               throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
               throw new UnsupportedOperationException();
            }
         };
      }

      @Override
      public Set<Entry<String, String>> entrySet() {
         return new AbstractSet<Entry<String, String>>() {
            @Override
            public Iterator<Entry<String, String>> iterator() {
               return Iterators.transform(keys.iterator(),
                     k -> new SimpleImmutableEntry<>(k, all.get(k)));
            }

            @Override
            public int size() {
               return keys.size();
            }
            
            @Override
            public boolean contains(Object o) {
               if (o instanceof Entry) {
                  Entry<?, ?> other = (Entry<?, ?>) o;
                  return keys.contains(other.getKey())
                        && Objects.equals(all.get(other.getKey()), other.getValue());
               }
               return false;
            }

            @Override
            public boolean addAll(Collection<? extends Entry<String, String>> c) {
               throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(Object o) {
               throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(Collection<?> c) {
               throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(Collection<?> c) {
               throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
               throw new UnsupportedOperationException();
            }
         };
      }
   }
}