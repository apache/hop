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
package org.apache.hop.pipeline.transforms.jdbcmetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class OutputFieldTest {

  @Test
  void noArgConstructorLeavesFieldsNull() {
    OutputField field = new OutputField();
    assertNull(field.getName());
    assertNull(field.getRename());
  }

  @Test
  void valueConstructorAssignsBothFields() {
    OutputField field = new OutputField("TABLE_NAME", "table");
    assertEquals("TABLE_NAME", field.getName());
    assertEquals("table", field.getRename());
  }

  @Test
  void settersUpdateFields() {
    OutputField field = new OutputField();
    field.setName("COLUMN_NAME");
    field.setRename("col");
    assertEquals("COLUMN_NAME", field.getName());
    assertEquals("col", field.getRename());
  }
}
