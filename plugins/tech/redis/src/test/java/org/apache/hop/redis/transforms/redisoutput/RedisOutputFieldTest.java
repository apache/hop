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

package org.apache.hop.redis.transforms.redisoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.redis.transforms.RedisDataStructure;
import org.junit.jupiter.api.Test;

class RedisOutputFieldTest {

  @Test
  void resolveKeyPrefersKeyThenRedisNameThenStreamField() {
    RedisOutputField field = new RedisOutputField();
    field.setStreamField("stream");
    assertEquals("stream", field.resolveKey());

    field.setRedisName("legacy");
    assertEquals("legacy", field.resolveKey());

    field.setKey("explicit");
    assertEquals("explicit", field.resolveKey());
  }

  @Test
  void resolveDataStructureDefaultsToString() {
    RedisOutputField field = new RedisOutputField();
    field.setDataStructure(null);
    assertEquals(RedisDataStructure.STRING, field.resolveDataStructure());
  }

  @Test
  void cloneCopiesFields() {
    RedisOutputField field = new RedisOutputField();
    field.setStreamField("s");
    field.setKey("k");
    field.setHashKey("h");
    field.setTtlSeconds("9");
    field.setDataStructure(RedisDataStructure.HASH);

    RedisOutputField copy = field.clone();
    assertEquals("s", copy.getStreamField());
    assertEquals("k", copy.getKey());
    assertEquals("h", copy.getHashKey());
    assertEquals("9", copy.getTtlSeconds());
    assertEquals(RedisDataStructure.HASH, copy.getDataStructure());
  }
}
