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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.jupiter.api.Test;

class RedisClientSessionTest {

  @Test
  void codecReturnsByteArrayInstance() {
    assertSame(ByteArrayCodec.INSTANCE, RedisClientSession.codec());
  }

  @Test
  void closeWithoutPoolClosesConnectionAndShutsDownClient() {
    AbstractRedisClient client = mock(AbstractRedisClient.class);
    @SuppressWarnings("unchecked")
    StatefulRedisConnection<byte[], byte[]> connection = mock(StatefulRedisConnection.class);
    IRedisCommands commands = mock(IRedisCommands.class);

    try (RedisClientSession session = new RedisClientSession(client, connection, commands)) {
      assertSame(commands, session.getCommands());
    }

    verify(connection).close();
    verify(client).shutdown();
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void closeWithPoolReturnsObjectAndClosesPool() throws Exception {
    AbstractRedisClient client = mock(AbstractRedisClient.class);
    StatefulRedisConnection<byte[], byte[]> connection = mock(StatefulRedisConnection.class);
    IRedisCommands commands = mock(IRedisCommands.class);
    GenericObjectPool pool = mock(GenericObjectPool.class);

    try (RedisClientSession session = new RedisClientSession(client, connection, commands, pool)) {
      // closed by try-with-resources
    }

    verify(pool).returnObject(connection);
    verify(pool).close();
    verify(connection, never()).close();
    verify(client).shutdown();
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void closeStillShutsDownClientWhenReturnObjectFails() throws Exception {
    AbstractRedisClient client = mock(AbstractRedisClient.class);
    StatefulRedisConnection<byte[], byte[]> connection = mock(StatefulRedisConnection.class);
    IRedisCommands commands = mock(IRedisCommands.class);
    GenericObjectPool pool = mock(GenericObjectPool.class);
    doThrow(new RuntimeException("return failed")).when(pool).returnObject(connection);

    new RedisClientSession(client, connection, commands, pool).close();

    verify(pool).close();
    verify(client).shutdown();
  }
}
