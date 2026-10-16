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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** The output field type is applied on every row, and each copy keeps its own conversion flag. */
class OpenFormulaResultTypeTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void initEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void integerFieldStaysALongWhenLaterRowsReturnABigDecimal() throws Exception {
    Prepared copy = prepare(integerIfMeta(), 0);
    Object[] positive = copy.transform.calcFields(copy.input, new Object[] {5L});
    Object[] negative = copy.transform.calcFields(copy.input, new Object[] {-3L});
    Object[] positiveAgain = copy.transform.calcFields(copy.input, new Object[] {2L});

    assertInstanceOf(Long.class, positive[1]);
    assertEquals(5L, positive[1]);
    assertInstanceOf(Long.class, negative[1]);
    assertEquals(0L, negative[1]);
    assertInstanceOf(Long.class, positiveAgain[1]);
    assertEquals(2L, positiveAgain[1]);
  }

  @Test
  void eachCopyKeepsItsOwnConversionFlag() throws Exception {
    OpenFormulaMeta meta = integerIfMeta();
    Prepared positiveCopy = prepare(meta, 0);
    Prepared negativeCopy = prepare(meta, 1);

    Object[] positive = positiveCopy.transform.calcFields(positiveCopy.input, new Object[] {5L});
    Object[] negative = negativeCopy.transform.calcFields(negativeCopy.input, new Object[] {-3L});

    assertInstanceOf(Long.class, positive[1]);
    assertEquals(5L, positive[1]);
    assertInstanceOf(Long.class, negative[1]);
    assertEquals(0L, negative[1]);
    // A Long already matches the Integer field. The literal 0 is a BigDecimal and does not.
    // Those flags used to live on the shared metadata, so the second copy overwrote the first.
    assertFalse(positiveCopy.data.needDataConversion[0]);
    assertTrue(negativeCopy.data.needDataConversion[0]);

    Object[] positiveAgain =
        positiveCopy.transform.calcFields(positiveCopy.input, new Object[] {2L});
    Object[] negativeAgain =
        negativeCopy.transform.calcFields(negativeCopy.input, new Object[] {-1L});
    assertInstanceOf(Long.class, positiveAgain[1]);
    assertEquals(2L, positiveAgain[1]);
    assertInstanceOf(Long.class, negativeAgain[1]);
    assertEquals(0L, negativeAgain[1]);
    assertFalse(positiveCopy.data.needDataConversion[0]);
    assertTrue(negativeCopy.data.needDataConversion[0]);
  }

  private static OpenFormulaMeta integerIfMeta() {
    OpenFormulaMeta meta = new OpenFormulaMeta();
    meta.getFormulas()
        .add(
            new OpenFormulaMetaFunction(
                "out", "IF([a]>0;[a];0)", IValueMeta.TYPE_INTEGER, -1, -1, ""));
    return meta;
  }

  private static Prepared prepare(OpenFormulaMeta meta, int copyNr) throws Exception {
    OpenFormulaData data = new OpenFormulaData();
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("OpenFormula");
    OpenFormula transform = new OpenFormula(transformMeta, meta, data, copyNr, null, null);

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaInteger("a"));
    data.outputRowMeta = input.clone();
    meta.getFields(
        data.outputRowMeta,
        "OpenFormula",
        null,
        null,
        new Variables(),
        new MemoryMetadataProvider());
    data.context = new RowFormulaContext(data.outputRowMeta);
    data.returnType = new int[] {-1};
    data.needDataConversion = new boolean[] {false};
    data.replaceIndex = new int[] {-1};
    return new Prepared(transform, data, input);
  }

  private record Prepared(OpenFormula transform, OpenFormulaData data, RowMeta input) {}
}
