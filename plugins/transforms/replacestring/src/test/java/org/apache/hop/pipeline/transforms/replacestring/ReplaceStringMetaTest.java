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

package org.apache.hop.pipeline.transforms.replacestring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class ReplaceStringMetaTest {
  @Test
  void testSerialization() throws Exception {
    ReplaceStringMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/replace-in-string-transform.xml", ReplaceStringMeta.class);

    assertEquals(6, meta.getFields().size());
    ReplaceStringMeta.RSField field = meta.getFields().get(5);
    assertEquals("strD", field.getFieldInStream());
    assertTrue(field.isUsingRegEx());
    assertEquals("[CIOKSX]", field.getReplaceString());
  }
}
