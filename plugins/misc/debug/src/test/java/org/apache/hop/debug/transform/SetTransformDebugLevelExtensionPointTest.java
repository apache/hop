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

package org.apache.hop.debug.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Condition;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.debug.util.DebugLevelUtil;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Runs a real pipeline with a pipeline log level that logs nothing and custom logging configured on
 * one transform, to verify that the selected rows -- and only those -- end up in the log.
 */
class SetTransformDebugLevelExtensionPointTest {

  private static final String INPUT = "input";
  private static final String DUMMY = "dummy";
  private static final int NR_ROWS = 5;

  @BeforeAll
  static void beforeAll() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void onlyTheRowsInTheRangeAreLogged() throws Exception {
    TransformDebugLevel debugLevel = new TransformDebugLevel(LogLevel.ROWLEVEL);
    debugLevel.setStartRow(2);
    debugLevel.setEndRow(4);

    assertEquals(List.of(2L, 3L, 4L), runAndCollectLoggedRows(debugLevel));
  }

  @Test
  void onlyTheRowsMatchingTheConditionAreLogged() throws Exception {
    TransformDebugLevel debugLevel = new TransformDebugLevel(LogLevel.ROWLEVEL);
    debugLevel.setCondition(
        new Condition(
            "seq",
            Condition.Function.LARGER,
            null,
            new ValueMetaAndData(new ValueMetaInteger("constant"), 3L)));

    assertEquals(List.of(4L, 5L), runAndCollectLoggedRows(debugLevel));
  }

  @Test
  void withoutARangeOrConditionEveryRowIsLogged() throws Exception {
    assertEquals(
        List.of(1L, 2L, 3L, 4L, 5L),
        runAndCollectLoggedRows(new TransformDebugLevel(LogLevel.ROWLEVEL)));
  }

  /**
   * Run an "injector -> dummy" pipeline with the pipeline log level set to nothing and the given
   * custom logging configuration on the dummy transform.
   *
   * @param debugLevel the custom logging configuration for the dummy transform
   * @return the numbers of the rows the dummy transform logged, in order
   */
  private List<Long> runAndCollectLoggedRows(TransformDebugLevel debugLevel) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("custom logging");

    TransformMeta input = new TransformMeta(INPUT, new InjectorMeta());
    TransformMeta dummy = new TransformMeta(DUMMY, new DummyMeta());
    pipelineMeta.addTransform(input);
    pipelineMeta.addTransform(dummy);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, dummy));

    Map<String, String> debugGroup = new HashMap<>();
    DebugLevelUtil.storeTransformDebugLevel(debugGroup, DUMMY, debugLevel);
    pipelineMeta.getAttributesMap().put(Defaults.DEBUG_GROUP, debugGroup);

    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipelineMeta, new Variables(), null);
    pipeline.setLogLevel(LogLevel.NOTHING);
    pipeline.prepareExecution();

    String dummyLogChannelId = pipeline.getComponentCopies(DUMMY).get(0).getLogChannelId();

    RowProducer rowProducer = pipeline.addRowProducer(INPUT, 0);
    pipeline.startThreads();

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("seq"));
    for (long i = 1; i <= NR_ROWS; i++) {
      rowProducer.putRow(rowMeta, new Object[] {i});
    }
    rowProducer.finished();
    pipeline.waitUntilFinished();

    assertTrue(pipeline.getErrors() == 0, "The pipeline reported errors");

    return loggedRowNumbers(HopLogStore.getAppender().getBuffer(dummyLogChannelId, false));
  }

  /** Pick the row numbers out of the "Wrote row #N : ..." lines the dummy transform logged. */
  private static List<Long> loggedRowNumbers(CharSequence logText) {
    List<Long> rowNumbers = new ArrayList<>();
    for (String line : logText.toString().split("\n")) {
      int index = line.indexOf("Wrote row #");
      if (index >= 0) {
        String number = line.substring(index + "Wrote row #".length()).split(" ")[0];
        rowNumbers.add(Long.valueOf(number));
      }
    }
    return rowNumbers;
  }
}
