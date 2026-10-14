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

package org.apache.hop.pipeline.transforms.openformula;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.openformula.EvaluationException;
import org.junit.jupiter.api.Test;

class RowFormulaContextTest {

  @Test
  void resolvesNumericAndStringFields() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("qty"));
    rowMeta.addValueMeta(new ValueMetaNumber("price"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    RowFormulaContext context = new RowFormulaContext(rowMeta);
    context.setRowData(new Object[] {3L, 1.5d, "box"});
    assertEquals(3L, context.resolveReference("qty"));
    assertEquals(1.5d, context.resolveReference("price"));
    assertEquals("box", context.resolveReference("name"));
  }

  @Test
  void missingFieldThrows() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("qty"));
    RowFormulaContext context = new RowFormulaContext(rowMeta);
    context.setRowData(new Object[] {1L});
    assertThrows(EvaluationException.class, () -> context.resolveReference("missing"));
  }
}
