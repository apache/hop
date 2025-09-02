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

package org.apache.hop.pipeline.transforms.execprocess;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class ExecProcessMetaTest {
  @Test
  void testSerialization() throws Exception {
    ExecProcessMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/execute-process-transform.xml", ExecProcessMeta.class);

    assertEquals("script", meta.getProcessField());
    assertEquals("output", meta.getResultFieldName());
    assertEquals("error", meta.getErrorFieldName());
    assertEquals("exit", meta.getExitValueFieldName());
    assertTrue(meta.isFailWhenNotSuccess());
    assertEquals(2, meta.getArgumentFields().size());
    assertEquals("value1", meta.getArgumentFields().get(0).getName());
    assertEquals("value2", meta.getArgumentFields().get(1).getName());
  }
}
