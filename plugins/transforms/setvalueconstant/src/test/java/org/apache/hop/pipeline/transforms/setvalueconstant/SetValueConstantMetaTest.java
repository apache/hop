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
package org.apache.hop.pipeline.transforms.setvalueconstant;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SetValueConstantMetaTest {
  @Test
  void testSerializationRoundTrip() throws Exception {
    SetValueConstantMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/set-constant-values.xml", SetValueConstantMeta.class);

    Assertions.assertTrue(meta.isUsingVariables());
    Assertions.assertEquals(3, meta.getFields().size());
    SetValueConstantMeta.Field f = meta.getFields().getFirst();
    Assertions.assertEquals("f1", f.getFieldName());
    Assertions.assertEquals("const1", f.getReplaceValue());
    Assertions.assertFalse(f.isEmptyString());
    f = meta.getFields().get(1);
    Assertions.assertEquals("f2", f.getFieldName());
    Assertions.assertEquals("2026/03/25", f.getReplaceValue());
    Assertions.assertEquals("yyyy/MM/dd", f.getReplaceMask());
    Assertions.assertFalse(f.isEmptyString());
    f = meta.getFields().getLast();
    Assertions.assertEquals("f3", f.getFieldName());
    Assertions.assertTrue(StringUtils.isEmpty(f.getReplaceValue()));
    Assertions.assertTrue(StringUtils.isEmpty(f.getReplaceMask()));
    Assertions.assertTrue(f.isEmptyString());
  }
}
