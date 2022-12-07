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

import java.util.List;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class RandomValueMetaTest {

  @Test
  public void testSerialization() throws Exception {
    RandomValueMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/generate-random-values-transform.xml", RandomValueMeta.class);
    Assert.assertEquals("12345", meta.getSeed());
    Assert.assertEquals(7, meta.getFields().size());

    List<RandomValueMeta.RVField> fields = meta.getFields();

    Assert.assertEquals(RandomValueMeta.RandomType.NUMBER, fields.get(0).getType());
    Assert.assertEquals("num", fields.get(0).getName());
    Assert.assertEquals(RandomValueMeta.RandomType.INTEGER, fields.get(1).getType());
    Assert.assertEquals("int", fields.get(1).getName());
    Assert.assertEquals(RandomValueMeta.RandomType.STRING, fields.get(2).getType());
    Assert.assertEquals("str", fields.get(2).getName());
    Assert.assertEquals(RandomValueMeta.RandomType.UUID, fields.get(3).getType());
    Assert.assertEquals("uuid", fields.get(3).getName());
    Assert.assertEquals(RandomValueMeta.RandomType.UUID4, fields.get(4).getType());
    Assert.assertEquals("uuid4", fields.get(4).getName());
    Assert.assertEquals(RandomValueMeta.RandomType.HMAC_MD5, fields.get(5).getType());
    Assert.assertEquals("hmac_md5", fields.get(5).getName());
    Assert.assertEquals(RandomValueMeta.RandomType.HMAC_SHA1, fields.get(6).getType());
    Assert.assertEquals("hmac_sha1", fields.get(6).getName());
  }
}
