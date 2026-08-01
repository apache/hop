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

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import lombok.Getter;
import org.apache.commons.pool2.impl.GenericObjectPool;

/** Holds an open Lettuce client/connection (optionally pooled) and exposes sync commands. */
public final class RedisClientSession implements AutoCloseable {

  private final AbstractRedisClient client;
  private final StatefulConnection<byte[], byte[]> connection;
  @Getter private final IRedisCommands commands;
  private final boolean cluster;

  @SuppressWarnings("rawtypes")
  private final GenericObjectPool pool;

  public RedisClientSession(
      AbstractRedisClient client,
      StatefulRedisConnection<byte[], byte[]> connection,
      IRedisCommands commands) {
    this(client, connection, commands, false, null);
  }

  public RedisClientSession(
      AbstractRedisClient client,
      StatefulRedisClusterConnection<byte[], byte[]> connection,
      IRedisCommands commands) {
    this(client, connection, commands, true, null);
  }

  @SuppressWarnings("rawtypes")
  public RedisClientSession(
      AbstractRedisClient client,
      StatefulRedisConnection<byte[], byte[]> connection,
      IRedisCommands commands,
      GenericObjectPool pool) {
    this(client, connection, commands, false, pool);
  }

  @SuppressWarnings("rawtypes")
  public RedisClientSession(
      AbstractRedisClient client,
      StatefulRedisClusterConnection<byte[], byte[]> connection,
      IRedisCommands commands,
      GenericObjectPool pool) {
    this(client, connection, commands, true, pool);
  }

  @SuppressWarnings("rawtypes")
  private RedisClientSession(
      AbstractRedisClient client,
      StatefulConnection<byte[], byte[]> connection,
      IRedisCommands commands,
      boolean cluster,
      GenericObjectPool pool) {
    this.client = client;
    this.connection = connection;
    this.commands = commands;
    this.cluster = cluster;
    this.pool = pool;
  }

  public static ByteArrayCodec codec() {
    return ByteArrayCodec.INSTANCE;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void close() {
    try {
      if (pool != null && connection != null) {
        try {
          pool.returnObject(connection);
        } finally {
          pool.close();
        }
      } else if (connection != null) {
        connection.close();
      }
    } catch (Exception e) {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (Exception ignored) {
        // ignore secondary close failures
      }
    } finally {
      if (client != null) {
        client.shutdown();
      }
    }
  }
}
