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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RedisOutputWriteModeTest {

  @Test
  void getNamesAndFromCode() {
    assertArrayEquals(new String[] {"KEY_VALUE", "STREAM_FIELDS"}, RedisOutputWriteMode.getNames());
    assertEquals(
        RedisOutputWriteMode.STREAM_FIELDS, RedisOutputWriteMode.fromCode("stream_fields"));
    assertEquals(RedisOutputWriteMode.KEY_VALUE, RedisOutputWriteMode.fromCode(null));
    assertEquals(RedisOutputWriteMode.KEY_VALUE, RedisOutputWriteMode.fromCode(" "));
    assertEquals(RedisOutputWriteMode.KEY_VALUE, RedisOutputWriteMode.fromCode("other"));
  }
}
