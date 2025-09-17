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

package org.apache.hop.pipeline.transforms.detectlastrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class DetectLastRowMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void testTransformMeta() throws HopException {
    List<String> attributes = Arrays.asList("ResultFieldName");

    LoadSaveTester<DetectLastRowMeta> loadSaveTester =
        new LoadSaveTester<>(DetectLastRowMeta.class, attributes);
    loadSaveTester.testSerialization();
  }

  @Test
  void testDefault() {
    DetectLastRowMeta meta = new DetectLastRowMeta();
    meta.setDefault();
    assertEquals("result", meta.getResultFieldName());
  }

  @Test
  void testGetData() {
    DetectLastRowMeta meta = new DetectLastRowMeta();
    assertTrue(meta.createTransformData() instanceof DetectLastRowData);
  }

  @Test
  void testGetFields() throws HopTransformException {
    DetectLastRowMeta meta = new DetectLastRowMeta();
    meta.setDefault();
    meta.setResultFieldName("The Result");
    RowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "this transform", null, null, new Variables(), null);

    assertEquals(1, rowMeta.size());
    assertEquals("The Result", rowMeta.getValueMeta(0).getName());
    assertEquals(IValueMeta.TYPE_BOOLEAN, rowMeta.getValueMeta(0).getType());
  }

  @Test
  void testSupportedPipelineTypes() {
    DetectLastRowMeta meta = new DetectLastRowMeta();
    assertEquals(1, meta.getSupportedPipelineTypes().length);
    assertEquals(PipelineType.Normal, meta.getSupportedPipelineTypes()[0]);
  }
}
