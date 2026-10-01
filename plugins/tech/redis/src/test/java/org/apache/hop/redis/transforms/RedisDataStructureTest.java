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

package org.apache.hop.redis.transforms;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RedisDataStructureTest {

  @Test
  void getNamesReturnsAllValues() {
    assertArrayEquals(
        new String[] {"STRING", "HASH", "SET", "LIST"}, RedisDataStructure.getNames());
  }

  @Test
  void fromCodeParsesAndFallsBack() {
    assertEquals(RedisDataStructure.HASH, RedisDataStructure.fromCode("hash"));
    assertEquals(RedisDataStructure.LIST, RedisDataStructure.fromCode(" LIST "));
    assertEquals(RedisDataStructure.STRING, RedisDataStructure.fromCode(null));
    assertEquals(RedisDataStructure.STRING, RedisDataStructure.fromCode("  "));
    assertEquals(RedisDataStructure.STRING, RedisDataStructure.fromCode("unknown"));
  }
}
