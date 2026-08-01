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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class RedisDeploymentModeTest {

  @Test
  void testGetNames() {
    String[] names = RedisDeploymentMode.getNames();
    assertNotNull(names);
    assertEquals(3, names.length);
    assertEquals("STANDALONE", names[0]);
    assertEquals("SENTINEL", names[1]);
    assertEquals("CLUSTER", names[2]);
  }

  @Test
  void testFromCode() {
    assertEquals(RedisDeploymentMode.STANDALONE, RedisDeploymentMode.fromCode("STANDALONE"));
    assertEquals(RedisDeploymentMode.SENTINEL, RedisDeploymentMode.fromCode("sentinel"));
    assertEquals(RedisDeploymentMode.CLUSTER, RedisDeploymentMode.fromCode("CLUSTER"));
    assertEquals(RedisDeploymentMode.STANDALONE, RedisDeploymentMode.fromCode(null));
    assertEquals(RedisDeploymentMode.STANDALONE, RedisDeploymentMode.fromCode("nope"));
  }
}
