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

package org.apache.hop.pipeline.transforms.cubeinput;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class CubeInputMetaTest {

  @Test
  void testRoundTrip() throws Exception {
    CubeInputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/de-serialize-transform.xml", CubeInputMeta.class);
    assertNotNull(meta.getFile());
    assertNotNull(meta.getFile().getName());
  }

  /**
   * Issue #3861: with no cube file configured, getFields() dereferenced the file name and threw a
   * raw NullPointerException out of prepareExecution instead of reporting the misconfiguration.
   */
  @Test
  void getFieldsWithoutFilenameReportsTheMisconfiguration() {
    CubeInputMeta meta = new CubeInputMeta();

    HopTransformException e =
        assertThrows(
            HopTransformException.class,
            () -> meta.getFields(new RowMeta(), "cube input", null, null, new Variables(), null));
    assertTrue(e.getMessage().contains("cube file name"), e.getMessage());
  }
}
