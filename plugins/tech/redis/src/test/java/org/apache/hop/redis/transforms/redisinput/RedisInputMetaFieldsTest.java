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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.redis.codec.RedisCodecType;
import org.apache.hop.redis.transforms.RedisDataStructure;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RedisInputMetaFieldsTest {

  @BeforeAll
  static void initEnv() throws Exception {
    HopEnvironment.init();
  }

  @AfterAll
  static void resetEnv() {
    HopEnvironment.reset();
  }

  @Test
  void getFieldsUsesBinaryForByteStringAndHash() throws Exception {
    RedisInputMeta meta = new RedisInputMeta();
    RedisInputField stringByte = field("k1", "v1", RedisDataStructure.STRING, RedisCodecType.BYTE);
    RedisInputField hashByte = field("k2", "v2", RedisDataStructure.HASH, RedisCodecType.BYTE);
    meta.setFields(List.of(stringByte, hashByte));

    RowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "RedisInput", null, null, new Variables(), null);

    assertEquals(2, rowMeta.size());
    assertInstanceOf(ValueMetaBinary.class, rowMeta.getValueMeta(0));
    assertInstanceOf(ValueMetaBinary.class, rowMeta.getValueMeta(1));
    assertEquals(IValueMeta.TYPE_BINARY, rowMeta.getValueMeta(0).getType());
  }

  @Test
  void getFieldsKeepsStringForSetAndListEvenWithByteCodec() throws Exception {
    RedisInputMeta meta = new RedisInputMeta();
    meta.setFields(
        List.of(
            field("k1", "setOut", RedisDataStructure.SET, RedisCodecType.BYTE),
            field("k2", "listOut", RedisDataStructure.LIST, RedisCodecType.BYTE)));

    RowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "RedisInput", null, null, new Variables(), null);

    assertEquals(2, rowMeta.size());
    assertInstanceOf(ValueMetaString.class, rowMeta.getValueMeta(0));
    assertInstanceOf(ValueMetaString.class, rowMeta.getValueMeta(1));
  }

  private static RedisInputField field(
      String key, String valueField, RedisDataStructure structure, RedisCodecType valueCodec) {
    RedisInputField field = new RedisInputField();
    field.setRedisKey(key);
    field.setValueField(valueField);
    field.setDataStructure(structure);
    field.setValueCodec(valueCodec);
    return field;
  }
}
