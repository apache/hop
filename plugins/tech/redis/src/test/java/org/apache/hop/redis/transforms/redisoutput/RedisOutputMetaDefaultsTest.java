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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.redis.codec.RedisCodecType;
import org.apache.hop.redis.transforms.RedisDataStructure;
import org.apache.hop.redis.transforms.RedisListPushDirection;
import org.junit.jupiter.api.Test;

class RedisOutputMetaDefaultsTest {

  @Test
  void setDefaultResetsConfiguration() {
    RedisOutputMeta meta = new RedisOutputMeta();
    meta.setConnectionName("c1");
    meta.setWriteMode(RedisOutputWriteMode.STREAM_FIELDS);
    meta.setFields(List.of(new RedisOutputField()));
    meta.setDefault();

    assertNull(meta.getConnectionName());
    assertEquals(RedisOutputWriteMode.KEY_VALUE, meta.getWriteMode());
    assertEquals(RedisDataStructure.STRING, meta.getDataStructure());
    assertEquals(RedisCodecType.STRING, meta.getKeyCodec());
    assertEquals(RedisCodecType.STRING, meta.getValueCodec());
    assertEquals("", meta.getKeyField());
    assertEquals("", meta.getValueField());
    assertEquals(RedisListPushDirection.RPUSH, meta.getListPushDirection());
    assertNotNull(meta.getFields());
    assertTrue(meta.getFields().isEmpty());
  }

  @Test
  void setFieldsNullBecomesEmptyList() {
    RedisOutputMeta meta = new RedisOutputMeta();
    meta.setFields(null);
    assertNotNull(meta.getFields());
    assertTrue(meta.getFields().isEmpty());

    List<RedisOutputField> fields = new ArrayList<>();
    fields.add(new RedisOutputField());
    meta.setFields(fields);
    assertEquals(1, meta.getFields().size());
  }
}
