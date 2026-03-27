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

package org.apache.hop.pipeline.transforms.regexeval;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class RegexEval_EmptyStringVsNull_Test {
  private TransformMockHelper<RegexEvalMeta, ITransformData> helper;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        TransformMockUtil.getTransformMockHelper(
            RegexEvalMeta.class, "RegexEval_EmptyStringVsNull_Test");
  }

  @AfterEach
  void cleanUp() {
    helper.cleanUp();
  }

  @Test
  void emptyAndNullsAreNotDifferent() throws Exception {
    System.setProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "N");
    List<Object[]> expected =
        Arrays.asList(
            new Object[] {false, ""}, new Object[] {false, ""}, new Object[] {false, null});
    executeAndAssertResults(expected);
  }

  @Test
  void emptyAndNullsAreDifferent() throws Exception {
    System.setProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "Y");
    List<Object[]> expected =
        Arrays.asList(
            new Object[] {false, ""}, new Object[] {false, ""}, new Object[] {false, null});
    executeAndAssertResults(expected);
  }

  private void executeAndAssertResults(List<Object[]> expected) throws Exception {
    RegexEvalMeta meta = new RegexEvalMeta();

    RegexEvalMeta.RegexField f1 = new RegexEvalMeta.RegexField();
    f1.setFieldName("string");
    f1.setFieldTrimType(IValueMeta.TYPE_STRING);
    meta.getRegexFields().add(f1);

    RegexEvalMeta.RegexField f2 = new RegexEvalMeta.RegexField();
    f2.setFieldName("matcher");
    f2.setFieldTrimType(IValueMeta.TYPE_STRING);
    meta.getRegexFields().add(f2);

    meta.setResultFieldName("string");
    meta.setReplacingFields(true);
    meta.setMatcher("matcher");
    meta.setAllowingCaptureGroups(true);

    RegexEvalData data = new RegexEvalData();
    RegexEval transform = createAndInitTransform(meta, data);

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("string"));
    input.addValueMeta(new ValueMetaString("matcher"));
    transform.setInputRowMeta(input);

    transform = spy(transform);
    doReturn(new String[] {" ", " "})
        .doReturn(new String[] {"", ""})
        .doReturn(new String[] {null, null})
        .when(transform)
        .getRow();

    // dummy pattern, just to contain two groups
    // needed to activate a branch with conversion from string
    data.pattern = Pattern.compile("(a)(a)");

    List<Object[]> actual = PipelineTestingUtil.execute(transform, 3, false);
    PipelineTestingUtil.assertResult(expected, actual);
  }

  private RegexEval createAndInitTransform(RegexEvalMeta meta, RegexEvalData data)
      throws Exception {
    when(helper.transformMeta.getTransform()).thenReturn(meta);

    RegexEval transform =
        new RegexEval(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    transform.init();
    return transform;
  }
}
