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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class RedisClientFactoryTest {

  @Test
  void parseNodesSupportsCommaAndDefaultPort() {
    List<RedisClientFactory.HostPort> nodes =
        RedisClientFactory.parseNodes("localhost:6380, 127.0.0.1, host2:7000", 6379);
    assertEquals(3, nodes.size());
    assertEquals("localhost", nodes.get(0).host());
    assertEquals(6380, nodes.get(0).port());
    assertEquals("127.0.0.1", nodes.get(1).host());
    assertEquals(6379, nodes.get(1).port());
    assertEquals("host2", nodes.get(2).host());
    assertEquals(7000, nodes.get(2).port());
  }

  @Test
  void parseNodesEmpty() {
    assertTrue(RedisClientFactory.parseNodes(null, 6379).isEmpty());
    assertTrue(RedisClientFactory.parseNodes("  ", 6379).isEmpty());
  }
}
