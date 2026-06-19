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

package org.apache.hop.pipeline.transforms.javascript;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ScriptValuesTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  /**
   * Build a ScriptValues with the given meta wired into BaseTransform.meta (not the mock the {@link
   * TransformMockUtil} would give us). Returns a spy so tests can stub {@code getRow()}.
   */
  private static ScriptValues newTransform(ScriptValuesMeta meta, RowMeta inputRowMeta)
      throws Exception {
    TransformMockHelper<ScriptValuesMeta, ScriptValuesData> mockHelper =
        new TransformMockHelper<>("test", ScriptValuesMeta.class, ScriptValuesData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);

    ScriptValues real =
        new ScriptValues(
            mockHelper.transformMeta,
            meta,
            new ScriptValuesData(),
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    real.setInputRowMeta(inputRowMeta);
    return spy(real);
  }

  @Test
  void bigNumberAreNotTrimmedToInt() throws Exception {
    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaBigNumber("value_int"));
    input.addValueMeta(new ValueMetaBigNumber("value_double"));

    ScriptValuesMeta meta = new ScriptValuesMeta();
    ScriptValuesMeta.ScriptField f1 = new ScriptValuesMeta.ScriptField();
    f1.setName("value_int");
    f1.setType(IValueMeta.TYPE_BIGNUMBER);
    f1.setReplace(true);
    meta.getScriptFields().add(f1);
    ScriptValuesMeta.ScriptField f2 = new ScriptValuesMeta.ScriptField();
    f2.setName("value_double");
    f2.setType(IValueMeta.TYPE_BIGNUMBER);
    f2.setReplace(true);
    meta.getScriptFields().add(f2);

    meta.getJsScripts()
        .add(
            new ScriptValuesScript(
                ScriptValuesScript.TRANSFORM_SCRIPT,
                "script",
                "value_int = 10.00;\nvalue_double = 10.50"));

    ScriptValues transform = newTransform(meta, input);
    doReturn(new Object[] {BigDecimal.ONE, BigDecimal.ONE}).when(transform).getRow();

    transform.init();

    Object[] expectedRow = {BigDecimal.TEN, new BigDecimal("10.5")};
    Object[] row = PipelineTestingUtil.execute(transform, 1, false).get(0);
    PipelineTestingUtil.assertResult(expectedRow, row);
  }

  @Test
  void variableIsSetInScopeOfTransform() throws Exception {
    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("str"));

    ScriptValuesMeta meta = new ScriptValuesMeta();
    ScriptValuesMeta.ScriptField f1 = new ScriptValuesMeta.ScriptField();
    f1.setName("str");
    f1.setType(IValueMeta.TYPE_STRING);
    f1.setReplace(true);
    meta.getScriptFields().add(f1);

    meta.getJsScripts()
        .add(
            new ScriptValuesScript(
                ScriptValuesScript.TRANSFORM_SCRIPT,
                "script",
                "setVariable('temp', 'pass', 'r');\nstr = getVariable('temp', 'fail');"));

    ScriptValues transform = newTransform(meta, input);
    doReturn(new Object[] {""}).when(transform).getRow();

    transform.init();

    Object[] expectedRow = {"pass"};
    Object[] row = PipelineTestingUtil.execute(transform, 1, false).get(0);
    PipelineTestingUtil.assertResult(expectedRow, row);
  }
}
