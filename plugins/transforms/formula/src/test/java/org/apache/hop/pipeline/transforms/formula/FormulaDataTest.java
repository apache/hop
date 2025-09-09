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

package org.apache.hop.pipeline.transforms.formula;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class FormulaDataTest {

  @Test
  void testConstantsValues() {
    assertEquals(0, FormulaData.RETURN_TYPE_STRING);
    assertEquals(1, FormulaData.RETURN_TYPE_NUMBER);
    assertEquals(2, FormulaData.RETURN_TYPE_INTEGER);
    assertEquals(3, FormulaData.RETURN_TYPE_LONG);
    assertEquals(4, FormulaData.RETURN_TYPE_DATE);
    assertEquals(5, FormulaData.RETURN_TYPE_BIGDECIMAL);
    assertEquals(6, FormulaData.RETURN_TYPE_BYTE_ARRAY);
    assertEquals(7, FormulaData.RETURN_TYPE_BOOLEAN);
    assertEquals(9, FormulaData.RETURN_TYPE_TIMESTAMP);
  }

  @Test
  void testConstructor() {
    FormulaData data = new FormulaData();
    assertNotNull(data);
  }
}
