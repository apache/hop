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
package org.apache.hop.pipeline.transforms.randomvalue;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class RandomValueMetaTest {

  @Test
  void testSerialization() throws Exception {
    RandomValueMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/generate-random-values-transform.xml", RandomValueMeta.class);
    assertEquals("12345", meta.getSeed());
    assertEquals(7, meta.getFields().size());

    List<RandomValueMeta.RVField> fields = meta.getFields();

    assertEquals(RandomValueMeta.RandomType.NUMBER, fields.get(0).getType());
    assertEquals("num", fields.get(0).getName());
    assertEquals(RandomValueMeta.RandomType.INTEGER, fields.get(1).getType());
    assertEquals("int", fields.get(1).getName());
    assertEquals(RandomValueMeta.RandomType.STRING, fields.get(2).getType());
    assertEquals("str", fields.get(2).getName());
    assertEquals(RandomValueMeta.RandomType.UUID, fields.get(3).getType());
    assertEquals("uuid", fields.get(3).getName());
    assertEquals(RandomValueMeta.RandomType.UUID4, fields.get(4).getType());
    assertEquals("uuid4", fields.get(4).getName());
    assertEquals(RandomValueMeta.RandomType.HMAC_MD5, fields.get(5).getType());
    assertEquals("hmac_md5", fields.get(5).getName());
    assertEquals(RandomValueMeta.RandomType.HMAC_SHA1, fields.get(6).getType());
    assertEquals("hmac_sha1", fields.get(6).getName());
  }
}
