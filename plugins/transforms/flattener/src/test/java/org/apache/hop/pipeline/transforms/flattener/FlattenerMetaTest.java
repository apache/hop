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

package org.apache.hop.pipeline.transforms.flattener;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class FlattenerMetaTest {
  @Test
  void testSerialization() throws Exception {
    FlattenerMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/flattener-transform.xml", FlattenerMeta.class);

    assertEquals("flatten", meta.getFieldName());
    assertEquals(2, meta.getTargetFields().size());
    assertEquals("target1", meta.getTargetFields().get(0).getName());
    assertEquals("target2", meta.getTargetFields().get(1).getName());
  }
}
