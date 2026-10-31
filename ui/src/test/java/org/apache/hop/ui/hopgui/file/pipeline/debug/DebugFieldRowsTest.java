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

package org.apache.hop.ui.hopgui.file.pipeline.debug;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DebugFieldRowsTest {

  @BeforeAll
  static void setUp() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void cellsCarryEveryFieldAttribute() {
    ValueMetaString name = new ValueMetaString("name");
    name.setOrigin("customers");
    name.setComments("the label");
    String[] cells = DebugFieldRows.cells(name);

    assertEquals(DebugFieldRows.COLUMNS.length, cells.length);
    assertEquals("name", cells[0]);
    assertEquals("String", cells[1]);
    assertEquals("-", cells[3]);
    assertEquals("customers", cells[5]);
    assertEquals("the label", cells[11]);
  }

  @Test
  void cellsShowLengthAndPrecision() {
    ValueMetaInteger id = new ValueMetaInteger("id");
    id.setLength(9);
    id.setPrecision(0);
    String[] cells = DebugFieldRows.cells(id);

    assertEquals("id", cells[0]);
    assertEquals("Integer", cells[1]);
    assertEquals("9", cells[3]);
    assertEquals("0", cells[4]);
  }
}
