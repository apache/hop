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

package org.apache.hop.pipeline.transforms.jsoninput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit test for {@link JsonInputData} */
class JsonInputDataTests {

  @Test
  void constructorInitializesTransformDataDefaults() {
    JsonInputData data = new JsonInputData();

    assertEquals(0, data.nr_repeats);
    assertNull(data.previousRow);
    assertEquals(0, data.filenr);
    assertEquals(-1, data.indexSourceField);
    assertEquals(-1, data.nrInputFields);
    assertNull(data.readrow);
    assertEquals(0, data.totalpreviousfields);
    assertFalse(data.hasFirstRow);
    assertEquals(0L, data.rownr);
  }

  @Test
  void skipEmptyFileDefaultsToFalse() {
    JsonInputData data = new JsonInputData();
    assertFalse(data.skipEmptyFile);
  }

  @Test
  void skipEmptyFileIsMutableFlagForFileIterator() {
    JsonInputData data = new JsonInputData();
    data.skipEmptyFile = true;
    assertTrue(data.skipEmptyFile);
    data.skipEmptyFile = false;
    assertFalse(data.skipEmptyFile);
  }
}
