/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.parquet.transforms.output;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Unit test for {@link ParquetField} */
class ParquetFieldTest {

  @Test
  void testDefaultConstructor() {
    ParquetField field = new ParquetField();
    assertNull(field.getSourceFieldName());
    assertNull(field.getTargetFieldName());
  }

  @Test
  void testConstructorWithNames() {
    ParquetField field = new ParquetField("source", "target");
    assertEquals("source", field.getSourceFieldName());
    assertEquals("target", field.getTargetFieldName());
  }

  @Test
  void testCopyConstructor() {
    ParquetField original = new ParquetField("source", "target");
    ParquetField copy = new ParquetField(original);
    assertEquals("source", copy.getSourceFieldName());
    assertEquals("target", copy.getTargetFieldName());
  }

  @Test
  void testSetters() {
    ParquetField field = new ParquetField();
    field.setSourceFieldName("in");
    field.setTargetFieldName("out");
    assertEquals("in", field.getSourceFieldName());
    assertEquals("out", field.getTargetFieldName());
  }
}
