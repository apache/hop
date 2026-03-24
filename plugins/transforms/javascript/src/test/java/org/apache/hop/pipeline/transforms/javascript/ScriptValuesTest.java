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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.math.BigDecimal;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ScriptValuesTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void bigNumberAreNotTrimmedToInt() throws Exception {
    ScriptValues transform =
        TransformMockUtil.getTransform(
            ScriptValues.class, ScriptValuesMeta.class, ScriptValuesData.class, "test");

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaBigNumber("value_int"));
    input.addValueMeta(new ValueMetaBigNumber("value_double"));
    transform.setInputRowMeta(input);

    transform = spy(transform);
    doReturn(new Object[] {BigDecimal.ONE, BigDecimal.ONE}).when(transform).getRow();

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

    transform.init();

    Object[] expectedRow = {BigDecimal.TEN, new BigDecimal("10.5")};
    Object[] row = PipelineTestingUtil.execute(transform, /*meta, data,*/ 1, false).get(0);
    PipelineTestingUtil.assertResult(expectedRow, row);
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void variableIsSetInScopeOfTransform() throws Exception {
    ScriptValues transform =
        TransformMockUtil.getTransform(
            ScriptValues.class, ScriptValuesMeta.class, ScriptValuesData.class, "test");

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("str"));
    transform.setInputRowMeta(input);

    transform = spy(transform);
    doReturn(new Object[] {""}).when(transform).getRow();

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

    transform.init();

    Object[] expectedRow = {"pass"};
    Object[] row = PipelineTestingUtil.execute(transform, /*meta, data,*/ 1, false).get(0);
    PipelineTestingUtil.assertResult(expectedRow, row);
  }
}
