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

package org.apache.hop.redis.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RedisConnectionTest {

  @Test
  void defaults() {
    RedisConnection connection = new RedisConnection();
    assertEquals(RedisDeploymentMode.STANDALONE, connection.getDeploymentMode());
    assertEquals("localhost", connection.getHostname());
    assertEquals("6379", connection.getPort());
    assertEquals("0", connection.getDatabase());
    assertEquals("10000", connection.getTimeoutMs());
    assertFalse(connection.isUseSsl());
    assertFalse(connection.isEnablePooling());
    assertEquals("8", connection.getPoolMaxTotal());
    assertEquals("8", connection.getPoolMaxIdle());
    assertEquals("0", connection.getPoolMinIdle());
    assertEquals("-1", connection.getPoolMaxWaitMs());
  }

  @Test
  void copyConstructorCopiesFields() {
    RedisConnection source = new RedisConnection();
    source.setName("prod");
    source.setDeploymentMode(RedisDeploymentMode.CLUSTER);
    source.setHostname("192.168.1.1");
    source.setPort("7000");
    source.setClusterNodes("192.168.1.1:7000");
    source.setPassword("secret");
    source.setUseSsl(true);
    source.setEnablePooling(true);
    source.setPoolMaxTotal("16");

    RedisConnection copy = new RedisConnection(source);
    assertEquals("prod", copy.getName());
    assertEquals(RedisDeploymentMode.CLUSTER, copy.getDeploymentMode());
    assertEquals("192.168.1.1", copy.getHostname());
    assertEquals("7000", copy.getPort());
    assertEquals("192.168.1.1:7000", copy.getClusterNodes());
    assertEquals("secret", copy.getPassword());
    assertTrue(copy.isUseSsl());
    assertTrue(copy.isEnablePooling());
    assertEquals("16", copy.getPoolMaxTotal());
  }
}
