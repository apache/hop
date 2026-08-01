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
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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
    return openClusterSession(client, connection, variables);
  }

  private static RedisClientSession openStandaloneSession(
      RedisClient client, RedisConnection connection, IVariables variables) throws HopException {
    try {
      if (connection.isEnablePooling()) {
        GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> poolConfig =
            buildPoolConfig(connection, variables);
        GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> pool =
            ConnectionPoolSupport.createGenericObjectPool(
                () -> client.connect(ByteArrayCodec.INSTANCE), poolConfig);
        StatefulRedisConnection<byte[], byte[]> conn = pool.borrowObject();
        return new RedisClientSession(
            client, conn, IRedisCommands.createStandalone(conn.sync()), pool);
      }
      StatefulRedisConnection<byte[], byte[]> conn = client.connect(ByteArrayCodec.INSTANCE);
      return new RedisClientSession(client, conn, IRedisCommands.createStandalone(conn.sync()));
    } catch (Exception e) {
      client.shutdown();
      throw new HopException("Unable to open Redis connection", e);
    }
  }

  private static RedisClientSession openClusterSession(
      RedisClusterClient client, RedisConnection connection, IVariables variables)
      throws HopException {
    try {
      if (connection.isEnablePooling()) {
        GenericObjectPoolConfig<StatefulRedisClusterConnection<byte[], byte[]>> poolConfig =
            buildPoolConfig(connection, variables);
        GenericObjectPool<StatefulRedisClusterConnection<byte[], byte[]>> pool =
            ConnectionPoolSupport.createGenericObjectPool(
                () -> client.connect(ByteArrayCodec.INSTANCE), poolConfig);
        StatefulRedisClusterConnection<byte[], byte[]> conn = pool.borrowObject();
        return new RedisClientSession(
            client, conn, IRedisCommands.createCluster(conn.sync()), pool);
      }
      StatefulRedisClusterConnection<byte[], byte[]> conn = client.connect(ByteArrayCodec.INSTANCE);
      return new RedisClientSession(client, conn, IRedisCommands.createCluster(conn.sync()));
    } catch (Exception e) {
      client.shutdown();
      throw new HopException("Unable to open Redis cluster connection", e);
    }
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
