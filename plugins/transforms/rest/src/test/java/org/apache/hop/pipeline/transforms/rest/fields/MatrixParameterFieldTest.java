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

package org.apache.hop.pipeline.transforms.rest.fields;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class MatrixParameterFieldTest {

  @Test
  void testDefaultConstructor() {
    MatrixParameterField field = new MatrixParameterField();
    assertNotNull(field);
    assertNull(field.getHeaderField());
    assertNull(field.getName());
  }

  @Test
  void testParameterizedConstructor() {
    MatrixParameterField field = new MatrixParameterField("fieldValue", "matrixParam");
    assertEquals("fieldValue", field.getHeaderField());
    assertEquals("matrixParam", field.getName());
  }

  @Test
  void testSettersAndGetters() {
    MatrixParameterField field = new MatrixParameterField();
    field.setHeaderField("testField");
    field.setName("testName");

    assertEquals("testField", field.getHeaderField());
    assertEquals("testName", field.getName());
  }

  @Test
  void testSetHeaderField() {
    MatrixParameterField field = new MatrixParameterField("initial", "name");
    field.setHeaderField("updated");
    assertEquals("updated", field.getHeaderField());
  }

  @Test
  void testSetName() {
    MatrixParameterField field = new MatrixParameterField("field", "initial");
    field.setName("updated");
    assertEquals("updated", field.getName());
  }
}
