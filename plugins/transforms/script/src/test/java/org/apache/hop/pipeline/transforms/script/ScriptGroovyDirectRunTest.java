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

package org.apache.hop.pipeline.transforms.script;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * A Groovy row script is run directly, not through the script engine's eval. Every test here runs
 * the same transform both ways and wants the same rows, errors and side effects, next to the values
 * it expects.
 */
class ScriptGroovyDirectRunTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<ScriptMeta, ScriptData> mockHelper;

  @BeforeAll
  static void initPlugins() throws HopException {
    ValueMetaPluginType.getInstance().searchPlugins();
  }

  @BeforeEach
  void setUp() {
    mockHelper = new TransformMockHelper<>("Script", ScriptMeta.class, ScriptData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void outputFieldsAreSetFromInputFields() throws Exception {
    Outcome outcome =
        bothWays(
            meta(
                "gross = price * qty\nstatus_lower = status.toLowerCase()",
                field("gross", "Number"),
                field("status_lower", "String")));

    assertEquals(3, outcome.rows.size());
    assertEquals(List.of(1L, 2.5d, 4L, "NEW", 10.0d, "new"), outcome.row(0));
    assertEquals(List.of(3L, 1.5d, 2L, "DONE", 3.0d, "done"), outcome.row(2));
  }

  @Test
  void aFunctionOfTheStartScriptCanBeCalled() throws Exception {
    ScriptMeta meta = meta("doubled = twice(qty)", field("doubled", "Integer"));
    meta.getScripts().add(start("def twice(x) { x * 2 }"));

    Outcome outcome = bothWays(meta);

    assertEquals(List.of(8L, 6L, 4L), outcome.column(4));
  }

  @Test
  void aVariableOfTheStartScriptCarriesOverFromRowToRow() throws Exception {
    ScriptMeta meta = meta("counter = counter + qty\ntotal = counter", field("total", "Integer"));
    meta.getScripts().add(start("counter = 100"));

    Outcome outcome = bothWays(meta);

    assertEquals(List.of(104L, 107L, 109L), outcome.column(4));
  }

  @Test
  void theScriptCanDefineAndCallItsOwnFunction() throws Exception {
    Outcome outcome =
        bothWays(meta("def inc(x) { x + 1 }\nnext = inc(id)", field("next", "Integer")));

    assertEquals(List.of(2L, 3L, 4L), outcome.column(4));
  }

  /** One script object for all rows would count 1, 2, 3 here. */
  @Test
  void aFieldAnnotatedVariableStartsFreshForEveryRow() throws Exception {
    Outcome outcome =
        bothWays(
            meta(
                "import groovy.transform.Field\n@Field int n = 0\nn++\ncount = n",
                field("count", "Integer")));

    assertEquals(List.of(1L, 1L, 1L), outcome.column(4));
  }

  @Test
  void theRowVariablesOfTheTransformAreThere() throws Exception {
    Outcome outcome =
        bothWays(
            meta(
                "nr = rowNumber\n"
                    + "prev = previousRow == null ? -1 : previousRow[0]\n"
                    + "fields = rowMeta.size()\n"
                    + "name = transformName + '/' + (context != null) + '/' + (row[3] == status)",
                field("nr", "Integer"),
                field("prev", "Integer"),
                field("fields", "Integer"),
                field("name", "String")));

    assertEquals(List.of(1L, 2L, 3L), outcome.column(4));
    assertEquals(List.of(-1L, 1L, 2L), outcome.column(5));
    assertEquals(List.of(4L, 4L, 4L), outcome.column(6));
    assertEquals("Script/true/true", outcome.rows.get(0)[7]);
  }

  @Test
  void aClosureKeptInAVariableWorksOnLaterRows() throws Exception {
    Outcome outcome =
        bothWays(
            meta(
                "if (!binding.hasVariable('addTen')) { addTen = { it + 10 } }\nplus = addTen(id)",
                field("plus", "Integer")));

    assertEquals(List.of(11L, 12L, 13L), outcome.column(4));
  }

  @Test
  void theScriptResultCanBeAnOutputField() throws Exception {
    ScriptMeta.SField result = field("gross", "Number");
    result.setScriptResult(true);

    Outcome outcome = bothWays(meta("price * qty", result));

    assertEquals(List.of(10.0d, 9.0d, 3.0d), outcome.column(4));
  }

  @Test
  void aRowCanBeSkippedWithThePipelineStatus() throws Exception {
    Outcome outcome =
        bothWays(
            meta(
                "pipeline_status = id == 2 ? SKIP_PIPELINE : CONTINUE_PIPELINE\nseen = id",
                field("seen", "Integer")));

    assertEquals(List.of(1L, 3L), outcome.column(4));
    assertEquals(0, outcome.errors);
  }

  @Test
  void aFailingRowStopsTheTransformWithAnError() throws Exception {
    Outcome outcome = bothWays(meta("ratio = 10 / (id - 2)", field("ratio", "Integer")));

    // the first row passes, the second divides by zero
    assertEquals(1, outcome.rows.size());
    assertEquals(1, outcome.errors);
  }

  @Test
  void aCallToAFunctionThatDoesNotExistFails() throws Exception {
    Outcome outcome = bothWays(meta("x = noSuchFunction(id)", field("x", "Integer")));

    assertEquals(0, outcome.rows.size());
    assertEquals(1, outcome.errors);
  }

  @Test
  void aScriptThatDoesNotCompileFails() throws Exception {
    Outcome outcome = bothWays(meta("x = (", field("x", "Integer")));

    assertEquals(0, outcome.rows.size());
    assertEquals(1, outcome.errors);
  }

  @Test
  void theEndScriptCanCallAFunctionOfTheRowScript() throws Exception {
    ScriptMeta meta = meta("def inc(x) { x + 1 }\nnext = inc(id)", field("next", "Integer"));
    meta.getScripts()
        .add(
            new ScriptMeta.SScript(
                ScriptMeta.ScriptType.END_SCRIPT,
                "end",
                "transform.setVariable('END_RESULT', String.valueOf(inc(41)))"));

    Outcome outcome = bothWays(meta);

    assertEquals("42", outcome.endResult);
    assertEquals(0, outcome.errors);
  }

  @Test
  void aFunctionOfTheStartScriptCanCallOneOfTheRowScript() throws Exception {
    ScriptMeta meta =
        meta("def timesTen(x) { x * 10 }\nresult = viaStart(id)", field("result", "Integer"));
    meta.getScripts().add(start("def viaStart(x) { timesTen(x) + 1 }"));

    Outcome outcome = bothWays(meta);

    assertEquals(List.of(11L, 21L, 31L), outcome.column(4));
  }

  /** The functions of the row script do not show up as variables. */
  @Test
  void theScriptSeesTheSameVariables() throws Exception {
    Outcome outcome =
        bothWays(
            meta(
                "def inc(x) { x + 1 }\nnames = binding.variables.keySet().sort().join(',')",
                field("names", "String")));

    assertEquals(3, outcome.rows.size());
  }

  /**
   * Several copies of the transform run the same script at the same time, each on its own rows:
   * every copy must get its own results, with a start-script function, a function of its own, a
   * variable that carries over and a variable that starts fresh for every row.
   */
  @Test
  void copiesRunningAtTheSameTimeDoNotMixTheirRows() throws Exception {
    ScriptMeta meta =
        meta(
            "import groovy.transform.Field\n"
                + "@Field int fresh = 0\n"
                + "def plusOne(x) { x + 1 }\n"
                + "fresh++\n"
                + "seen = seen + 1\n"
                + "result = twice(plusOne(id)) + fresh\n"
                + "count = seen\n"
                + "label = status + '-' + qty",
            field("result", "Integer"),
            field("count", "Integer"),
            field("label", "String"));
    meta.getScripts().add(start("def twice(x) { x * 2 }\nseen = 0"));

    int copies = 8;
    int rowsPerCopy = 500;
    stubTransformMeta(meta);
    for (boolean direct : new boolean[] {false, true}) {
      ExecutorService pool = Executors.newFixedThreadPool(copies);
      try {
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Outcome>> futures = new ArrayList<>();
        for (int copy = 0; copy < copies; copy++) {
          Object[][] input = new Object[rowsPerCopy][];
          for (int r = 0; r < rowsPerCopy; r++) {
            // ids and quantities differ per copy
            input[r] = new Object[] {copy * 100_000L + r, 1.0d, (long) copy, "C" + copy};
          }
          futures.add(
              pool.submit(
                  () -> {
                    start.await();
                    return run(meta, direct, input);
                  }));
        }
        start.countDown();

        for (int copy = 0; copy < copies; copy++) {
          Outcome outcome = futures.get(copy).get(2, TimeUnit.MINUTES);
          assertEquals(direct, outcome.direct);
          assertEquals(0, outcome.errors);
          assertEquals(rowsPerCopy, outcome.rows.size());
          for (int r = 0; r < rowsPerCopy; r++) {
            Object[] row = outcome.rows.get(r);
            long id = copy * 100_000L + r;
            assertEquals(id, row[0]);
            assertEquals((id + 1) * 2 + 1, row[4], "copy " + copy + " row " + r);
            assertEquals((long) r + 1, row[5], "copy " + copy + " row " + r);
            assertEquals("C" + copy + "-" + copy, row[6]);
          }
        }
      } finally {
        pool.shutdownNow();
      }
    }
  }

  /** Other languages keep going through the script engine. */
  @Test
  void aJavaScriptRowScriptIsNotRunDirectly() throws Exception {
    ScriptMeta meta = meta("var doubled = qty * 2;", field("doubled", "Integer"));
    meta.setLanguageName("JavaScript");

    Outcome outcome = run(meta, true);

    assertTrue(!outcome.direct);
    assertEquals(List.of(8L, 6L, 4L), outcome.column(4));
  }

  // --- helpers ---

  private static ScriptMeta meta(String rowScript, ScriptMeta.SField... fields) {
    ScriptMeta meta = new ScriptMeta();
    meta.setLanguageName("Groovy");
    meta.getScripts()
        .add(new ScriptMeta.SScript(ScriptMeta.ScriptType.TRANSFORM_SCRIPT, "script", rowScript));
    meta.getFields().addAll(Arrays.asList(fields));
    return meta;
  }

  private static ScriptMeta.SScript start(String script) {
    return new ScriptMeta.SScript(ScriptMeta.ScriptType.START_SCRIPT, "start", script);
  }

  private static ScriptMeta.SField field(String name, String type) {
    ScriptMeta.SField field = new ScriptMeta.SField();
    field.setName(name);
    field.setRename(name);
    field.setType(type);
    field.setLength(-1);
    field.setPrecision(-1);
    return field;
  }

  /** Runs the transform through the engine and directly, and wants the same outcome. */
  private Outcome bothWays(ScriptMeta meta) throws Exception {
    Outcome engine = run(meta, false);
    Outcome direct = run(meta, true);

    assertTrue(!engine.direct, "the engine run must not use the direct runner");
    assertEquals(engine.rowTexts(), direct.rowTexts());
    assertEquals(engine.errors, direct.errors);
    assertEquals(engine.endResult, direct.endResult);
    if (engine.errors == 0 || !direct.rows.isEmpty()) {
      // a script that does not compile never gets a runner
      assertTrue(direct.direct, "the direct run must use the direct runner");
    }
    return direct;
  }

  private Outcome run(ScriptMeta meta, boolean direct) throws Exception {
    Object[][] input = {
      {1L, 2.5d, 4L, "NEW"},
      {2L, 3.0d, 3L, "OPEN"},
      {3L, 1.5d, 2L, "DONE"}
    };
    stubTransformMeta(meta);
    return run(meta, direct, input);
  }

  /** Not from several threads at once: a mock cannot be stubbed while others use it. */
  private void stubTransformMeta(ScriptMeta meta) {
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    when(mockHelper.transformMeta.getName()).thenReturn("Script");
  }

  /** Call {@link #stubTransformMeta} first. */
  private Outcome run(ScriptMeta meta, boolean direct, Object[][] input) throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaNumber("price"));
    rowMeta.addValueMeta(new ValueMetaInteger("qty"));
    rowMeta.addValueMeta(new ValueMetaString("status"));

    BlockingRowSet inputSet = new BlockingRowSet(input.length + 10);
    inputSet.setThreadNameFromToCopy("previous", 0, "Script", 0);
    inputSet.setRowMeta(rowMeta);
    for (Object[] row : input) {
      inputSet.putRow(rowMeta, row);
    }
    inputSet.setDone();
    BlockingRowSet outputSet = new BlockingRowSet(input.length + 10);
    outputSet.setThreadNameFromToCopy("Script", 0, "next", 0);

    ScriptData data = new ScriptData();
    Script transform =
        new Script(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    transform.directGroovyRowScript = direct;
    transform.addRowSetToInputRowSets(inputSet);
    transform.addRowSetToOutputRowSets(outputSet);

    assertTrue(transform.init());
    data.hasPreviousTransforms = true;
    for (int i = 0; i < input.length + 1 && transform.processRow(); i++) {
      // next row
    }

    Outcome outcome = new Outcome();
    outcome.direct = data.groovyRowScript != null;
    outcome.errors = transform.getErrors();
    outcome.endResult = transform.getVariable("END_RESULT");
    for (Object[] row = outputSet.getRowWait(10, TimeUnit.MILLISECONDS);
        row != null;
        row = outputSet.getRowWait(10, TimeUnit.MILLISECONDS)) {
      outcome.rows.add(row);
    }
    if (outcome.rows.isEmpty()) {
      assertNull(outputSet.getRowWait(10, TimeUnit.MILLISECONDS));
    } else {
      assertNotNull(outcome.rows.get(0));
    }
    return outcome;
  }

  private static final class Outcome {
    final List<Object[]> rows = new ArrayList<>();
    long errors;
    String endResult;
    boolean direct;

    List<Object> row(int index) {
      return Arrays.asList(rows.get(index)).subList(0, 6);
    }

    List<Object> column(int index) {
      return rows.stream().map(row -> row[index]).toList();
    }

    List<String> rowTexts() {
      return rows.stream().map(Arrays::deepToString).toList();
    }
  }
}
