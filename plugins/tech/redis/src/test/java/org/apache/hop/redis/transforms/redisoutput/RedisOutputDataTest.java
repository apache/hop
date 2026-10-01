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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.hop.redis.codec.RedisCodecType;
import org.apache.hop.redis.codec.RedisCodecs;
import org.apache.hop.redis.transforms.RedisDataStructure;
import org.apache.hop.redis.transforms.redisoutput.RedisOutputData.StreamMapping;
import org.junit.jupiter.api.Test;

class RedisOutputDataTest {

  @Test
  void defaultsAndAccessors() {
    RedisOutputData data = new RedisOutputData();
    assertEquals(-1, data.getKeyFieldIndex());
    assertEquals(-1, data.getValueFieldIndex());
    assertEquals(-1, data.getHashKeyFieldIndex());
    assertEquals(-1, data.getHashValueFieldIndex());
    assertNull(data.getTtlSeconds());
    assertNull(data.getStreamMappings());

    RedisCodecs codecs =
        RedisCodecs.of(
            RedisCodecType.STRING,
            RedisCodecType.STRING,
            RedisCodecType.STRING,
            RedisCodecType.STRING);
    data.setCodecs(codecs);
    data.setKeyFieldIndex(0);
    data.setTtlSeconds(60L);
    assertSame(codecs, data.getCodecs());
    assertEquals(0, data.getKeyFieldIndex());
    assertEquals(60L, data.getTtlSeconds());
  }

  @Test
  void streamMappingDefaults() {
    StreamMapping mapping = new StreamMapping();
    assertEquals(-1, mapping.getKeyFieldIndex());
    assertEquals(-1, mapping.getHashKeyFieldIndex());
    assertNull(mapping.getTtlSeconds());

    mapping.setStructure(RedisDataStructure.HASH);
    mapping.setStreamFieldIndex(2);
    mapping.setTtlSeconds(15L);
    assertEquals(RedisDataStructure.HASH, mapping.getStructure());
    assertEquals(2, mapping.getStreamFieldIndex());
    assertEquals(15L, mapping.getTtlSeconds());
  }
}
