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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lettuce.core.KeyValue;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class IRedisCommandsTest {

  private static final byte[] KEY = "k".getBytes(StandardCharsets.UTF_8);
  private static final byte[] VAL = "v".getBytes(StandardCharsets.UTF_8);
  private static final byte[] FIELD = "f".getBytes(StandardCharsets.UTF_8);

  @Test
  @SuppressWarnings("unchecked")
  void standaloneDelegatesCoreCommands() {
    RedisCommands<byte[], byte[]> lettuce = mock(RedisCommands.class);
    when(lettuce.ping()).thenReturn("PONG");
    when(lettuce.get(KEY)).thenReturn(VAL);
    when(lettuce.set(KEY, VAL)).thenReturn("OK");
    when(lettuce.hget(KEY, FIELD)).thenReturn(VAL);
    when(lettuce.hset(KEY, FIELD, VAL)).thenReturn(true);
    when(lettuce.hset(eq(KEY), any(Map.class))).thenReturn(1L);
    when(lettuce.smembers(KEY)).thenReturn(Set.of(VAL));
    when(lettuce.sismember(KEY, VAL)).thenReturn(true);
    when(lettuce.sadd(KEY, VAL)).thenReturn(1L);
    when(lettuce.lrange(KEY, 0, -1)).thenReturn(List.of(VAL));
    when(lettuce.rpush(KEY, VAL)).thenReturn(1L);
    when(lettuce.lpush(KEY, VAL)).thenReturn(2L);
    when(lettuce.expire(KEY, 30L)).thenReturn(true);
    when(lettuce.mget(KEY)).thenReturn(List.of(KeyValue.just(KEY, VAL), KeyValue.empty(KEY)));

    IRedisCommands commands = IRedisCommands.createStandalone(lettuce);

    assertEquals("PONG", commands.ping());
    assertEquals(VAL, commands.getValue(KEY));
    assertEquals("OK", commands.setValue(KEY, VAL));
    assertEquals(VAL, commands.hashGet(KEY, FIELD));
    assertTrue(commands.hashSet(KEY, FIELD, VAL));
    assertEquals(1L, commands.hashSet(KEY, Map.of(FIELD, VAL)));
    assertEquals(Set.of(VAL), commands.setMembers(KEY));
    assertTrue(commands.isSetMember(KEY, VAL));
    assertEquals(1L, commands.setAdd(KEY, VAL));
    assertEquals(List.of(VAL), commands.listRange(KEY, 0, -1));
    assertEquals(1L, commands.listRightPush(KEY, VAL));
    assertEquals(2L, commands.listLeftPush(KEY, VAL));
    assertTrue(commands.expire(KEY, 30L));

    List<IRedisCommands.KeyValue<byte[], byte[]>> multi = commands.multiGet(KEY);
    assertEquals(2, multi.size());
    assertEquals(VAL, multi.get(0).value());
    assertNull(multi.get(1).value());

    Map<byte[], byte[]> hashAll = new HashMap<>();
    hashAll.put(FIELD, VAL);
    when(lettuce.hgetall(KEY)).thenReturn(hashAll);
    assertEquals(hashAll, commands.hashGetAll(KEY));
    verify(lettuce).hgetall(KEY);
  }

  @Test
  @SuppressWarnings("unchecked")
  void clusterDelegatesCoreCommands() {
    RedisAdvancedClusterCommands<byte[], byte[]> lettuce = mock(RedisAdvancedClusterCommands.class);
    when(lettuce.ping()).thenReturn("PONG");
    when(lettuce.get(KEY)).thenReturn(VAL);
    when(lettuce.set(KEY, VAL)).thenReturn("OK");
    when(lettuce.hget(KEY, FIELD)).thenReturn(VAL);
    when(lettuce.smembers(KEY)).thenReturn(Set.of(VAL));
    when(lettuce.expire(KEY, 10L)).thenReturn(true);

    IRedisCommands commands = IRedisCommands.createCluster(lettuce);
    assertEquals("PONG", commands.ping());
    assertEquals(VAL, commands.getValue(KEY));
    assertEquals("OK", commands.setValue(KEY, VAL));
    assertEquals(VAL, commands.hashGet(KEY, FIELD));
    assertEquals(Set.of(VAL), commands.setMembers(KEY));
    assertTrue(commands.expire(KEY, 10L));
  }
}
