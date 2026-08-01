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

package org.apache.hop.redis.transforms.redisinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.hop.core.row.RowMeta;
import org.apache.hop.redis.transforms.RedisDataStructure;
import org.apache.hop.redis.transforms.redisinput.RedisInputData.Mapping;
import org.junit.jupiter.api.Test;

class RedisInputDataTest {

  @Test
  void defaultsAndAccessors() {
    RedisInputData data = new RedisInputData();
    assertNull(data.getOutputRowMeta());
    assertNull(data.getSession());
    assertNull(data.getMappings());
    assertEquals(0, data.getFirstValueIndex());

    RowMeta rowMeta = new RowMeta();
    Mapping mapping = new Mapping();
    Mapping[] mappings = new Mapping[] {mapping};
    data.setOutputRowMeta(rowMeta);
    data.setMappings(mappings);
    data.setFirstValueIndex(3);

    assertSame(rowMeta, data.getOutputRowMeta());
    assertSame(mappings, data.getMappings());
    assertEquals(3, data.getFirstValueIndex());
  }

  @Test
  void mappingDefaults() {
    Mapping mapping = new Mapping();
    assertEquals(-1, mapping.getKeyFieldIndex());
    assertEquals(-1, mapping.getHashFieldIndex());
    assertEquals(0, mapping.getListStart());
    assertEquals(0, mapping.getListStop());
    assertNull(mapping.getStructure());

    mapping.setStructure(RedisDataStructure.LIST);
    mapping.setListStart(1);
    mapping.setListStop(10);
    assertEquals(RedisDataStructure.LIST, mapping.getStructure());
    assertEquals(1, mapping.getListStart());
    assertEquals(10, mapping.getListStop());
  }
}
