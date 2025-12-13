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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit test for {@link Formula} */
class FormulaTests {
  private TransformMockHelper<FormulaMeta, FormulaData> transformMockHelper;

  @BeforeEach
  void setUp() {
    transformMockHelper =
        new TransformMockHelper<>("Formula", FormulaMeta.class, FormulaData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
    when(transformMockHelper.transformMeta.getName()).thenReturn("Formula");
  }

  @Test
  void processRow() throws Exception {
    Formula formula =
        new Formula(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    // init success.
    boolean result = formula.init();
    assertTrue(result);

    // Set up input row meta
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("Card"));
    inputRowMeta.addValueMeta(new ValueMetaString("Privacy"));
    formula.setInputRowMeta(inputRowMeta);

    // spy
    formula = spy(formula);
    doReturn(new Object[] {29, null})
        .doReturn(new Object[] {0, "Bob"})
        .doReturn(null)
        .when(formula)
        .getRow();

    List<Object[]> execCount = PipelineTestingUtil.execute(formula, 2, false);
    assertEquals(2, execCount.size());

    IRowMeta outputRowMeta = transformMockHelper.iTransformData.outputRowMeta;
    assertEquals(2, outputRowMeta.size());

    formula.dispose();
  }

  @AfterEach
  void tearDown() {
    transformMockHelper.cleanUp();
  }
}
