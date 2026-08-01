/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.redis.client;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.PartitionSelectorException;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode.NodeFlag;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.redis.metadata.RedisConnection;
import org.apache.hop.redis.metadata.RedisDeploymentMode;

/** Builds Lettuce clients for Standalone, Sentinel and Cluster. Pool settings are shared. */
public final class RedisClientFactory {

  private static final int CLUSTER_SLOT_COUNT = 16384;

  private static final byte[] CLUSTER_WARMUP_KEY =
      "hop:cluster:warmup".getBytes(StandardCharsets.UTF_8);

  private RedisClientFactory() {}

  public static RedisClientSession create(RedisConnection connection, IVariables variables)
      throws HopException {
    RedisDeploymentMode mode =
        connection.getDeploymentMode() == null
            ? RedisDeploymentMode.STANDALONE
            : connection.getDeploymentMode();

    Duration timeout = resolveTimeout(connection, variables);
    String username = resolve(variables, connection.getUsername());
    String password = Utils.resolvePassword(variables, connection.getPassword());
    boolean useSsl = connection.isUseSsl();

    return switch (mode) {
      case STANDALONE ->
          createStandalone(connection, variables, timeout, username, password, useSsl);
      case SENTINEL -> createSentinel(connection, variables, timeout, username, password, useSsl);
      case CLUSTER -> createCluster(connection, variables, timeout, username, password, useSsl);
    };
  }

  private static RedisClientSession createStandalone(
      RedisConnection connection,
      IVariables variables,
      Duration timeout,
      String username,
      String password,
      boolean useSsl)
      throws HopException {
    String host = Const.NVL(resolve(variables, connection.getHostname()), "localhost");
    int port = Const.toInt(resolve(variables, connection.getPort()), 6379);
    int database = Const.toInt(resolve(variables, connection.getDatabase()), 0);

    RedisURI.Builder builder =
        RedisURI.builder().withHost(host).withPort(port).withDatabase(database);
    applyAuthAndSsl(builder, username, password, useSsl, timeout);

    RedisClient client = RedisClient.create(builder.build());
    return openStandaloneSession(client, connection, variables);
  }

  private static RedisClientSession createSentinel(
      RedisConnection connection,
      IVariables variables,
      Duration timeout,
      String username,
      String password,
      boolean useSsl)
      throws HopException {
    String masterName = resolve(variables, connection.getMasterName());
    if (StringUtils.isEmpty(masterName)) {
      throw new HopException("Sentinel deployment requires a master name");
    }
    List<HostPort> nodes = parseNodes(resolve(variables, connection.getSentinelNodes()), 26379);
    if (nodes.isEmpty()) {
      throw new HopException("Sentinel deployment requires at least one sentinel node (host:port)");
    }

    int database = Const.toInt(resolve(variables, connection.getDatabase()), 0);
    RedisURI.Builder builder =
        RedisURI.builder().withSentinelMasterId(masterName).withDatabase(database);
    for (HostPort node : nodes) {
      builder.withSentinel(node.host(), node.port());
    }
    applyAuthAndSsl(builder, username, password, useSsl, timeout);

    RedisClient client = RedisClient.create(builder.build());
    return openStandaloneSession(client, connection, variables);
  }

  private static RedisClientSession createCluster(
      RedisConnection connection,
      IVariables variables,
      Duration timeout,
      String username,
      String password,
      boolean useSsl)
      throws HopException {
    List<HostPort> nodes = parseNodes(resolve(variables, connection.getClusterNodes()), 6379);
    if (nodes.isEmpty()) {
      throw new HopException("Cluster deployment requires at least one cluster node (host:port)");
    }

    List<RedisURI> uris = new ArrayList<>();
    for (HostPort node : nodes) {
      RedisURI.Builder builder = RedisURI.builder().withHost(node.host()).withPort(node.port());
      applyAuthAndSsl(builder, username, password, useSsl, timeout);
      uris.add(builder.build());
    }

    RedisClusterClient client = RedisClusterClient.create(uris);
    client.setOptions(buildClusterClientOptions());
    return openClusterSession(client, connection, variables);
  }

  private static ClusterClientOptions buildClusterClientOptions() {
    ClusterTopologyRefreshOptions topologyRefreshOptions =
        ClusterTopologyRefreshOptions.builder()
            // Short-lived Hop sessions do not need periodic refresh; adaptive is enough and
            // avoids rare empty-masterCache races during background refresh.
            .enableAllAdaptiveRefreshTriggers()
            .dynamicRefreshSources(true)
            .closeStaleConnections(true)
            .build();
    return ClusterClientOptions.builder()
        .topologyRefreshOptions(topologyRefreshOptions)
        // Docker / NAT clusters often advertise node IPs that differ from the seed hosts
        .validateClusterNodeMembership(false)
        .build();
  }

  /**
   * Pool/connection are handed to {@link RedisClientSession}, which closes them. try-with-resources
   * cannot be used here: it would close the pool before the session can own it.
   */
  @SuppressWarnings({"java:S2093", "java:S2095"})
  private static RedisClientSession openStandaloneSession(
      RedisClient client, RedisConnection connection, IVariables variables) throws HopException {
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> pool = null;
    try {
      if (connection.isEnablePooling()) {
        GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> poolConfig =
            buildPoolConfig(connection, variables);
        pool =
            ConnectionPoolSupport.createGenericObjectPool(
                () -> client.connect(ByteArrayCodec.INSTANCE), poolConfig);
        StatefulRedisConnection<byte[], byte[]> conn = pool.borrowObject();
        return new RedisClientSession(
            client, conn, IRedisCommands.createStandalone(conn.sync()), pool);
      }
      StatefulRedisConnection<byte[], byte[]> conn = client.connect(ByteArrayCodec.INSTANCE);
      return new RedisClientSession(client, conn, IRedisCommands.createStandalone(conn.sync()));
    } catch (Exception e) {
      closePoolQuietly(pool);
      client.shutdown();
      throw new HopException("Unable to open Redis connection", e);
    }
  }

  /**
   * Pool/connection are handed to {@link RedisClientSession}, which closes them. try-with-resources
   * cannot be used here: it would close the pool before the session can own it.
   */
  @SuppressWarnings({"java:S2093", "java:S2095"})
  private static RedisClientSession openClusterSession(
      RedisClusterClient client, RedisConnection connection, IVariables variables)
      throws HopException {
    GenericObjectPool<StatefulRedisClusterConnection<byte[], byte[]>> pool = null;
    try {
      // Load topology before the first key command. PING alone can succeed while the slot map
      // is still empty/incomplete, which later fails with PartitionSelectorException.
      ensureClusterPartitions(client);

      if (connection.isEnablePooling()) {
        GenericObjectPoolConfig<StatefulRedisClusterConnection<byte[], byte[]>> poolConfig =
            buildPoolConfig(connection, variables);
        pool =
            ConnectionPoolSupport.createGenericObjectPool(
                () -> client.connect(ByteArrayCodec.INSTANCE), poolConfig);
        StatefulRedisClusterConnection<byte[], byte[]> conn = pool.borrowObject();
        ensureClusterPartitions(client);
        warmUpClusterRouting(client, conn);
        return new RedisClientSession(
            client, conn, IRedisCommands.createCluster(conn.sync()), pool);
      }
      StatefulRedisClusterConnection<byte[], byte[]> conn = client.connect(ByteArrayCodec.INSTANCE);
      ensureClusterPartitions(client);
      warmUpClusterRouting(client, conn);
      return new RedisClientSession(client, conn, IRedisCommands.createCluster(conn.sync()));
    } catch (HopException e) {
      closePoolQuietly(pool);
      client.shutdown();
      throw e;
    } catch (Exception e) {
      closePoolQuietly(pool);
      client.shutdown();
      throw new HopException("Unable to open Redis cluster connection", e);
    }
  }

  private static void closePoolQuietly(GenericObjectPool<?> pool) {
    if (pool == null) {
      return;
    }
    try {
      pool.close();
    } catch (Exception ignored) {
      // best-effort cleanup after a failed open
    }
  }

  /**
   * Lettuce needs a complete slot map before key commands. An empty node list OR masters without
   * full 0..16383 coverage both surface later as {@code Cannot determine a partition for slot ...}.
   */
  private static void ensureClusterPartitions(RedisClusterClient client) throws HopException {
    try {
      client.refreshPartitions();
    } catch (Exception e) {
      throw new HopException(
          "Unable to refresh Redis cluster topology. "
              + "Check Cluster nodes, credentials, and that the target is a Redis Cluster "
              + "(CLUSTER SLOTS must succeed).",
          e);
    }

    Partitions partitions = client.getPartitions();
    if (partitions == null || partitions.isEmpty()) {
      throw new HopException(
          "Unable to load Redis cluster topology (empty partitions). "
              + "Check Cluster nodes, that the target is a Redis Cluster (not Standalone), "
              + "and that CLUSTER SLOTS is reachable from Hop.");
    }

    int coveredSlots = countCoveredSlots(partitions);
    if (coveredSlots < CLUSTER_SLOT_COUNT) {
      throw new HopException(
          "Redis cluster topology is incomplete: "
              + coveredSlots
              + "/"
              + CLUSTER_SLOT_COUNT
              + " hash slots are assigned to masters. "
              + "PING can still succeed, but key commands fail with "
              + "'Cannot determine a partition for slot ...'. "
              + "Nodes: "
              + summarizePartitions(partitions)
              + ". "
              + "Fix: finish cluster creation (redis-cli --cluster create ...), "
              + "or for Docker set cluster-announce-ip/port to addresses Hop can reach.");
    }
  }

  /**
   * Force one keyed command so routing uses the refreshed slot map. Retries once after refresh if
   * Lettuce still has a stale empty master cache on the connection.
   */
  private static void warmUpClusterRouting(
      RedisClusterClient client, StatefulRedisClusterConnection<byte[], byte[]> connection)
      throws HopException {
    try {
      connection.sync().exists(CLUSTER_WARMUP_KEY);
    } catch (Exception first) {
      if (!isPartitionSelectorFailure(first)) {
        throw new HopException("Redis cluster routing warmup failed", first);
      }
      ensureClusterPartitions(client);
      try {
        connection.sync().exists(CLUSTER_WARMUP_KEY);
      } catch (Exception second) {
        throw new HopException(
            "Redis cluster cannot route key commands after topology refresh. "
                + "Nodes: "
                + summarizePartitions(client.getPartitions())
                + ". "
                + "Verify CLUSTER SLOTS covers all slots and advertised node addresses are "
                + "reachable from Hop.",
            second);
      }
    }
  }

  private static boolean isPartitionSelectorFailure(Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof PartitionSelectorException) {
        return true;
      }
    }
    return false;
  }

  static int countCoveredSlots(Partitions partitions) {
    BitSet covered = new BitSet(CLUSTER_SLOT_COUNT);
    for (RedisClusterNode node : partitions) {
      if (node == null || isUnusableClusterNode(node)) {
        continue;
      }
      // Masters own slots; replicas usually report none. Count any reported slot ranges.
      for (Integer slot : node.getSlots()) {
        if (slot != null && slot >= 0 && slot < CLUSTER_SLOT_COUNT) {
          covered.set(slot);
        }
      }
    }
    return covered.cardinality();
  }

  private static boolean isUnusableClusterNode(RedisClusterNode node) {
    return node.is(NodeFlag.FAIL)
        || node.is(NodeFlag.EVENTUAL_FAIL)
        || node.is(NodeFlag.NOADDR)
        || node.is(NodeFlag.HANDSHAKE);
  }

  static String summarizePartitions(Partitions partitions) {
    if (partitions == null) {
      return "(null)";
    }
    List<String> parts = new ArrayList<>();
    for (RedisClusterNode node : partitions) {
      if (node == null) {
        continue;
      }
      String uri = node.getUri() != null ? node.getUri().toString() : "?";
      parts.add(uri + "[slots=" + node.getSlots().size() + ", flags=" + node.getFlags() + "]");
    }
    return parts.isEmpty() ? "(none)" : String.join("; ", parts);
  }

  private static <T> GenericObjectPoolConfig<T> buildPoolConfig(
      RedisConnection connection, IVariables variables) {
    GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
    config.setMaxTotal(Const.toInt(resolve(variables, connection.getPoolMaxTotal()), 8));
    config.setMaxIdle(Const.toInt(resolve(variables, connection.getPoolMaxIdle()), 8));
    config.setMinIdle(Const.toInt(resolve(variables, connection.getPoolMinIdle()), 0));
    long maxWaitMs = Const.toLong(resolve(variables, connection.getPoolMaxWaitMs()), -1L);
    if (maxWaitMs < 0) {
      config.setMaxWait(Duration.ofMillis(-1));
    } else {
      config.setMaxWait(Duration.ofMillis(maxWaitMs));
    }
    config.setTestOnBorrow(true);
    return config;
  }

  private static void applyAuthAndSsl(
      RedisURI.Builder builder,
      String username,
      String password,
      boolean useSsl,
      Duration timeout) {
    if (StringUtils.isNotEmpty(password)) {
      if (StringUtils.isNotEmpty(username)) {
        builder.withAuthentication(username, password.toCharArray());
      } else {
        builder.withPassword(password.toCharArray());
      }
    }
    if (useSsl) {
      builder.withSsl(true);
    }
    if (timeout != null) {
      builder.withTimeout(timeout);
    }
  }

  private static Duration resolveTimeout(RedisConnection connection, IVariables variables) {
    String timeoutMs = resolve(variables, connection.getTimeoutMs());
    if (StringUtils.isEmpty(timeoutMs)) {
      return Duration.ofSeconds(10);
    }
    long ms = Const.toLong(timeoutMs, 10000L);
    return Duration.ofMillis(Math.max(ms, 1L));
  }

  private static String resolve(IVariables variables, String value) {
    if (value == null) {
      return null;
    }
    return variables.resolve(value);
  }

  static List<HostPort> parseNodes(String nodesText, int defaultPort) {
    List<HostPort> nodes = new ArrayList<>();
    if (StringUtils.isEmpty(nodesText)) {
      return nodes;
    }
    Arrays.stream(nodesText.split("[,;\\s]+"))
        .map(String::trim)
        .filter(StringUtils::isNotEmpty)
        .forEach(
            token -> {
              String host = token;
              int port = defaultPort;
              int colon = token.lastIndexOf(':');
              if (colon > 0 && colon < token.length() - 1) {
                host = token.substring(0, colon);
                port = Const.toInt(token.substring(colon + 1), defaultPort);
              }
              nodes.add(new HostPort(host, port));
            });
    return nodes;
  }

  record HostPort(String host, int port) {}
}
