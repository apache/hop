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

package org.apache.hop.pipeline.transforms.joinrows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Runs Join Rows through the {@link SingleThreadedPipelineExecutor}: the executor behind the single
 * threaded pipeline engine and single threaded sub-pipelines (#2353).
 *
 * <p>The executor calls processRow() once per row waiting on the input row sets, and for as long as
 * the main stream (an info stream of Join Rows) holds rows. The rows on the input row sets are the
 * complete input, but the row sets are not flagged as done, so any read beyond them would wait
 * forever.
 */
class JoinRowsSingleThreadedTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(20);

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (ITransformMeta meta : List.of(new JoinRowsMeta(), new InjectorMeta(), new DummyMeta())) {
      if (registry.getPluginId(TransformPluginType.class, meta) == null) {
        registry.registerPluginClass(
            meta.getClass().getName(), TransformPluginType.class, Transform.class);
      }
    }
  }

  @Test
  void cartesianProductOfTwoInputs() throws Exception {
    List<Input> inputs = List.of(new Input("main", 3), new Input("other", 2));
    List<RowMetaAndData> rows =
        assertTimeoutPreemptively(TIMEOUT, () -> runJoin(inputs, "main", 500, false));

    assertEquals(6, rows.size());
    assertProduct(rows, inputs);
  }

  @Test
  void cartesianProductWhenInputRowSetsAreFinished() throws Exception {
    List<Input> inputs = List.of(new Input("main", 3), new Input("other", 2));
    List<RowMetaAndData> rows =
        assertTimeoutPreemptively(TIMEOUT, () -> runJoin(inputs, "main", 500, true));

    assertEquals(6, rows.size());
    assertProduct(rows, inputs);
  }

  @Test
  void cartesianProductSpillingToTemporaryFile() throws Exception {
    // A cache size below the number of rows makes the transform read the rows back from disk.
    List<Input> inputs = List.of(new Input("main", 4), new Input("other", 5));
    List<RowMetaAndData> rows =
        assertTimeoutPreemptively(TIMEOUT, () -> runJoin(inputs, "main", 2, false));

    assertEquals(20, rows.size());
    assertProduct(rows, inputs);
  }

  @Test
  void cartesianProductOfThreeInputsWithMainStreamLast() throws Exception {
    // The main stream is the last hop into Join Rows: its row set has to move to the front while
    // the other streams keep their hop order, the order in which JoinRowsMeta.getFields() lists
    // their fields.
    List<Input> inputs = List.of(new Input("a", 2), new Input("b", 3), new Input("main", 2));
    List<RowMetaAndData> rows =
        assertTimeoutPreemptively(TIMEOUT, () -> runJoin(inputs, "main", 500, false));

    assertEquals(12, rows.size());
    assertProduct(rows, List.of(inputs.get(2), inputs.get(0), inputs.get(1)));
  }

  @Test
  void cartesianProductOfThreeInputsWithMainStreamInTheMiddle() throws Exception {
    List<Input> inputs = List.of(new Input("a", 2), new Input("main", 3), new Input("b", 2));
    List<RowMetaAndData> rows =
        assertTimeoutPreemptively(TIMEOUT, () -> runJoin(inputs, "main", 1, false));

    assertEquals(12, rows.size());
    assertProduct(rows, List.of(inputs.get(1), inputs.get(0), inputs.get(2)));
  }

  @Test
  void noOutputWhenOneInputIsEmpty() throws Exception {
    List<Input> inputs = List.of(new Input("main", 3), new Input("other", 0));
    List<RowMetaAndData> rows =
        assertTimeoutPreemptively(TIMEOUT, () -> runJoin(inputs, "main", 500, false));

    assertTrue(rows.isEmpty());
  }

  @Test
  void singleInputPassesRowsThrough() throws Exception {
    // This is how the Beam engine used to feed the transform: all inputs flattened into one row
    // set, one row per iteration. The old batchComplete() hung on the row that was left behind.
    List<Input> inputs = List.of(new Input("main", 3));
    List<RowMetaAndData> rows =
        assertTimeoutPreemptively(TIMEOUT, () -> runJoin(inputs, "main", 500, false));

    assertEquals(3, rows.size());
    assertProduct(rows, inputs);
  }

  /**
   * An input of Join Rows: an injector with a single String field named after the input, holding
   * the values {@code <name>1 .. <name><nrRows>}.
   */
  private record Input(String name, int nrRows) {}

  /**
   * Asserts the complete cartesian product, with the fields in the given order of the inputs. The
   * values of the last input vary fastest.
   */
  private static void assertProduct(List<RowMetaAndData> rows, List<Input> fieldOrder) {
    List<String> expectedFields = fieldOrder.stream().map(Input::name).toList();
    List<String> expected = List.of("");
    for (Input input : fieldOrder) {
      List<String> combined = new ArrayList<>();
      for (String prefix : expected) {
        for (int n = 1; n <= input.nrRows(); n++) {
          combined.add(prefix + (prefix.isEmpty() ? "" : "|") + input.name() + n);
        }
      }
      expected = combined;
    }

    List<String> actual = new ArrayList<>();
    for (RowMetaAndData row : rows) {
      assertEquals(expectedFields, List.of(row.getRowMeta().getFieldNames()));
      List<String> values = new ArrayList<>();
      for (int i = 0; i < row.size(); i++) {
        values.add((String) row.getData()[i]);
      }
      actual.add(String.join("|", values));
    }
    assertEquals(expected, actual);
  }

  private static List<RowMetaAndData> runJoin(
      List<Input> inputs, String mainName, int cacheSize, boolean finishInputs) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("join-rows-single-threaded");

    List<TransformMeta> inputTransforms = new ArrayList<>();
    for (Input input : inputs) {
      inputTransforms.add(addTransform(pipelineMeta, input.name(), new InjectorMeta()));
    }

    JoinRowsMeta joinRowsMeta = new JoinRowsMeta();
    joinRowsMeta.setDefault();
    joinRowsMeta.setCacheSize(cacheSize);
    joinRowsMeta.setMainTransformName(mainName);
    joinRowsMeta.setDirectory(System.getProperty("java.io.tmpdir"));
    TransformMeta join = addTransform(pipelineMeta, "join", joinRowsMeta);

    TransformMeta output = addTransform(pipelineMeta, "output", new DummyMeta());

    for (TransformMeta inputTransform : inputTransforms) {
      pipelineMeta.addPipelineHop(new PipelineHopMeta(inputTransform, join));
    }
    pipelineMeta.addPipelineHop(new PipelineHopMeta(join, output));

    // Like loading the pipeline from a file: the main transform is an info stream of Join Rows,
    // which the executor feeds with processRow() calls for as long as it holds rows.
    joinRowsMeta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    pipeline.prepareExecution();

    Map<String, RowProducer> producers = new HashMap<>();
    for (Input input : inputs) {
      producers.put(input.name(), pipeline.addRowProducer(input.name(), 0));
    }

    ITransform joinTransform = pipeline.getTransform("join", 0);
    TransformRowsCollector collector = new TransformRowsCollector();
    joinTransform.addRowListener(collector);

    pipeline.startThreads();

    for (Input input : inputs) {
      IRowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaString(input.name()));
      RowProducer producer = producers.get(input.name());
      for (int n = 1; n <= input.nrRows(); n++) {
        producer.putRow(rowMeta, new Object[] {input.name() + n});
      }
    }
    if (finishInputs) {
      producers.values().forEach(RowProducer::finished);
    }

    SingleThreadedPipelineExecutor executor = new SingleThreadedPipelineExecutor(pipeline);
    assertTrue(executor.init());
    try {
      executor.oneIteration();
    } finally {
      executor.dispose();
    }
    assertEquals(0, joinTransform.getErrors());

    return collector.getRowsWritten();
  }

  private static TransformMeta addTransform(
      PipelineMeta pipelineMeta, String name, ITransformMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    TransformMeta transformMeta = new TransformMeta(pluginId, name, meta);
    pipelineMeta.addTransform(transformMeta);
    return transformMeta;
  }
}
