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

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Unified sync command API for Standalone/Sentinel and Cluster. */
public interface IRedisCommands {

  /** Redis {@code PING}. */
  String ping();

  /** Redis {@code GET}. */
  byte[] getValue(byte[] key);

  /** Redis {@code SET}. */
  String setValue(byte[] key, byte[] value);

  /** Redis {@code MGET}. */
  List<KeyValue<byte[], byte[]>> multiGet(byte[]... keys);

  /** Redis {@code HGET}. */
  byte[] hashGet(byte[] key, byte[] field);

  /** Redis {@code HGETALL}. */
  Map<byte[], byte[]> hashGetAll(byte[] key);

  /** Redis {@code HSET} for a single field. */
  Boolean hashSet(byte[] key, byte[] field, byte[] value);

  /** Redis {@code HSET} for multiple fields. */
  Long hashSet(byte[] key, Map<byte[], byte[]> map);

  /** Redis {@code SMEMBERS}. */
  Set<byte[]> setMembers(byte[] key);

  /** Redis {@code SISMEMBER}. */
  Boolean isSetMember(byte[] key, byte[] member);

  /** Redis {@code SADD}. */
  Long setAdd(byte[] key, byte[]... members);

  /** Redis {@code LRANGE}. */
  List<byte[]> listRange(byte[] key, long start, long stop);

  /** Redis {@code RPUSH}. */
  Long listRightPush(byte[] key, byte[]... values);

  /** Redis {@code LPUSH}. */
  Long listLeftPush(byte[] key, byte[]... values);

  /** Redis {@code EXPIRE}. */
  Boolean expire(byte[] key, long seconds);

  record KeyValue<K, V>(K key, V value) {}

  static IRedisCommands createStandalone(RedisCommands<byte[], byte[]> commands) {
    return new StandaloneCommands(commands);
  }

  static IRedisCommands createCluster(RedisAdvancedClusterCommands<byte[], byte[]> commands) {
    return new ClusterCommands(commands);
  }

  final class StandaloneCommands implements IRedisCommands {
    private final RedisCommands<byte[], byte[]> commands;

    StandaloneCommands(RedisCommands<byte[], byte[]> commands) {
      this.commands = commands;
    }

    @Override
    public String ping() {
      return commands.ping();
    }

    @Override
    public byte[] getValue(byte[] key) {
      return commands.get(key);
    }

    @Override
    public String setValue(byte[] key, byte[] value) {
      return commands.set(key, value);
    }

    @Override
    public List<KeyValue<byte[], byte[]>> multiGet(byte[]... keys) {
      return commands.mget(keys).stream()
          .map(kv -> new KeyValue<>(kv.getKey(), kv.hasValue() ? kv.getValue() : null))
          .toList();
    }

    @Override
    public byte[] hashGet(byte[] key, byte[] field) {
      return commands.hget(key, field);
    }

    @Override
    public Map<byte[], byte[]> hashGetAll(byte[] key) {
      return commands.hgetall(key);
    }

    @Override
    public Boolean hashSet(byte[] key, byte[] field, byte[] value) {
      return commands.hset(key, field, value);
    }

    @Override
    public Long hashSet(byte[] key, Map<byte[], byte[]> map) {
      return commands.hset(key, map);
    }

    @Override
    public Set<byte[]> setMembers(byte[] key) {
      return commands.smembers(key);
    }

    @Override
    public Boolean isSetMember(byte[] key, byte[] member) {
      return commands.sismember(key, member);
    }

    @Override
    public Long setAdd(byte[] key, byte[]... members) {
      return commands.sadd(key, members);
    }

    @Override
    public List<byte[]> listRange(byte[] key, long start, long stop) {
      return commands.lrange(key, start, stop);
    }

    @Override
    public Long listRightPush(byte[] key, byte[]... values) {
      return commands.rpush(key, values);
    }

    @Override
    public Long listLeftPush(byte[] key, byte[]... values) {
      return commands.lpush(key, values);
    }

    @Override
    public Boolean expire(byte[] key, long seconds) {
      return commands.expire(key, seconds);
    }
  }

  final class ClusterCommands implements IRedisCommands {
    private final RedisAdvancedClusterCommands<byte[], byte[]> commands;

    ClusterCommands(RedisAdvancedClusterCommands<byte[], byte[]> commands) {
      this.commands = commands;
    }

    @Override
    public String ping() {
      return commands.ping();
    }

    @Override
    public byte[] getValue(byte[] key) {
      return commands.get(key);
    }

    @Override
    public String setValue(byte[] key, byte[] value) {
      return commands.set(key, value);
    }

    @Override
    public List<KeyValue<byte[], byte[]>> multiGet(byte[]... keys) {
      return commands.mget(keys).stream()
          .map(kv -> new KeyValue<>(kv.getKey(), kv.hasValue() ? kv.getValue() : null))
          .toList();
    }

    @Override
    public byte[] hashGet(byte[] key, byte[] field) {
      return commands.hget(key, field);
    }

    @Override
    public Map<byte[], byte[]> hashGetAll(byte[] key) {
      return commands.hgetall(key);
    }

    @Override
    public Boolean hashSet(byte[] key, byte[] field, byte[] value) {
      return commands.hset(key, field, value);
    }

    @Override
    public Long hashSet(byte[] key, Map<byte[], byte[]> map) {
      return commands.hset(key, map);
    }

    @Override
    public Set<byte[]> setMembers(byte[] key) {
      return commands.smembers(key);
    }

    @Override
    public Boolean isSetMember(byte[] key, byte[] member) {
      return commands.sismember(key, member);
    }

    @Override
    public Long setAdd(byte[] key, byte[]... members) {
      return commands.sadd(key, members);
    }

    @Override
    public List<byte[]> listRange(byte[] key, long start, long stop) {
      return commands.lrange(key, start, stop);
    }

    @Override
    public Long listRightPush(byte[] key, byte[]... values) {
      return commands.rpush(key, values);
    }

    @Override
    public Long listLeftPush(byte[] key, byte[]... values) {
      return commands.lpush(key, values);
    }

    @Override
    public Boolean expire(byte[] key, long seconds) {
      return commands.expire(key, seconds);
    }
  }
}
