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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The rows Join Rows keeps in memory are used for many output rows. A row of the last stream is
 * read again for every output row, so whatever a transform downstream does to its values, the next
 * output row has to come out with the values that went in.
 *
 * <p>Every input has a String, an Integer, a Date and a Binary field, with nulls in its second row.
 * The transform that reads the output overwrites the bytes of the Binary value of the last stream,
 * as a transform that changes a value in place would. A Serializable field is left out: Join Rows
 * writes the rows of every stream but the main one to a temporary file, and a Serializable value
 * can not be written there.
 *
 * <p>The values of the main stream, and of a stream between the main and the last one, are shared
 * by the output rows they are part of. That is how it always was, and it is not changed here.
 */
class JoinRowsCachedValuesTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(20);
  private static final String[] FIELDS = {"name", "nr", "day", "bytes"};

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
  void rowsKeptInMemoryKeepTheirValues() {
    List<Input> inputs = List.of(new Input("main", 4), new Input("other", 3));
    Output output = assertTimeoutPreemptively(TIMEOUT, () -> runJoin(inputs, "main", 500));

    assertEquals(product(inputs), output.rows);
    assertEveryBinaryValueOfTheLastStreamIsItsOwnCopy(output);
  }

  @Test
  void rowsKeptInMemoryKeepTheirValuesWithThreeInputs() {
    // The main stream sits in the middle: the two other streams are the ones kept in memory.
    List<Input> inputs = List.of(new Input("a", 3), new Input("main", 3), new Input("b", 2));
    Output output = assertTimeoutPreemptively(TIMEOUT, () -> runJoin(inputs, "main", 500));

    assertEquals(product(List.of(inputs.get(1), inputs.get(0), inputs.get(2))), output.rows);
    assertEveryBinaryValueOfTheLastStreamIsItsOwnCopy(output);
  }

  @Test
  void rowsReadBackFromTheTemporaryFileKeepTheirValues() {
    // A cache size below the number of rows: the rows of the other stream are read from disk.
    List<Input> inputs = List.of(new Input("main", 4), new Input("other", 5));
    Output output = assertTimeoutPreemptively(TIMEOUT, () -> runJoin(inputs, "main", 2));

    assertEquals(product(inputs), output.rows);
    assertEveryBinaryValueOfTheLastStreamIsItsOwnCopy(output);
  }

  @Test
  void oneStreamInMemoryAndOneOnDisk() {
    // "a" fits in the cache, "b" does not.
    List<Input> inputs = List.of(new Input("main", 2), new Input("a", 3), new Input("b", 4));
    Output output = assertTimeoutPreemptively(TIMEOUT, () -> runJoin(inputs, "main", 3));

    assertEquals(product(inputs), output.rows);
    assertEveryBinaryValueOfTheLastStreamIsItsOwnCopy(output);
  }

  /** An input of Join Rows with the four fields, named after the input, and this many rows. */
  private record Input(String name, int nrRows) {}

  /** The output rows as text, and the Binary values of the last stream. */
  private static final class Output {
    final List<String> rows = new ArrayList<>();
    final List<byte[]> lastStreamBinaryValues = new ArrayList<>();
  }

  private static Object[] row(Input input, int n) {
    if (n == 2) {
      // nulls in every field that could need a copy
      return new Object[] {input.name() + n, (long) n, null, null};
    }
    return new Object[] {
      input.name() + n,
      (long) n,
      new Date(1_700_000_000_000L + n * 86_400_000L),
      (input.name() + "-bytes-" + n).getBytes()
    };
  }

  private static String text(Object[] row, int offset) {
    Object[] values = Arrays.copyOfRange(row, offset, offset + FIELDS.length);
    if (values[3] != null) {
      values[3] = new String((byte[]) values[3]);
    }
    if (values[2] != null) {
      values[2] = ((Date) values[2]).getTime();
    }
    return Arrays.toString(values);
  }

  /** The complete cartesian product as text. The values of the last input vary fastest. */
  private static List<String> product(List<Input> fieldOrder) {
    List<String> expected = List.of("");
    for (Input input : fieldOrder) {
      List<String> combined = new ArrayList<>();
      for (String prefix : expected) {
        for (int n = 1; n <= input.nrRows(); n++) {
          combined.add(prefix + text(row(input, n), 0));
        }
      }
      expected = combined;
    }
    return expected;
  }

  /**
   * No two output rows share the Binary value of the last stream: otherwise a change to one output
   * row shows up in another.
   */
  private static void assertEveryBinaryValueOfTheLastStreamIsItsOwnCopy(Output output) {
    Map<byte[], Boolean> seen = new IdentityHashMap<>();
    for (byte[] value : output.lastStreamBinaryValues) {
      assertNull(seen.put(value, Boolean.TRUE), "a Binary value is in two output rows");
    }
    assertTrue(seen.size() > 1, "there were Binary values to check");
  }

  private static Output runJoin(List<Input> inputs, String mainName, int cacheSize)
      throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("join-rows-cached-values");

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
    TransformMeta dummy = addTransform(pipelineMeta, "output", new DummyMeta());

    for (TransformMeta inputTransform : inputTransforms) {
      pipelineMeta.addPipelineHop(new PipelineHopMeta(inputTransform, join));
    }
    pipelineMeta.addPipelineHop(new PipelineHopMeta(join, dummy));
    joinRowsMeta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    pipeline.prepareExecution();

    Map<String, RowProducer> producers = new HashMap<>();
    for (Input input : inputs) {
      producers.put(input.name(), pipeline.addRowProducer(input.name(), 0));
    }

    // The main stream comes first in the output row, the other streams follow in hop order.
    int nrStreams = inputs.size();
    Output output = new Output();
    ITransform joinTransform = pipeline.getTransform("join", 0);
    joinTransform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            StringBuilder text = new StringBuilder();
            for (int stream = 0; stream < nrStreams; stream++) {
              text.append(text(row, stream * FIELDS.length));
            }
            output.rows.add(text.toString());

            // Now do what a transform downstream may do: change a Binary value in place.
            byte[] bytes = (byte[]) row[(nrStreams - 1) * FIELDS.length + 3];
            if (bytes != null) {
              output.lastStreamBinaryValues.add(bytes);
              Arrays.fill(bytes, (byte) '#');
            }
          }
        });

    pipeline.startThreads();

    for (Input input : inputs) {
      IRowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaString(input.name() + "_" + FIELDS[0]));
      rowMeta.addValueMeta(new ValueMetaInteger(input.name() + "_" + FIELDS[1]));
      rowMeta.addValueMeta(new ValueMetaDate(input.name() + "_" + FIELDS[2]));
      rowMeta.addValueMeta(new ValueMetaBinary(input.name() + "_" + FIELDS[3]));
      RowProducer producer = producers.get(input.name());
      for (int n = 1; n <= input.nrRows(); n++) {
        producer.putRow(rowMeta, row(input, n));
      }
    }

    SingleThreadedPipelineExecutor executor = new SingleThreadedPipelineExecutor(pipeline);
    assertTrue(executor.init());
    try {
      executor.oneIteration();
    } finally {
      executor.dispose();
    }
    assertEquals(0, joinTransform.getErrors());
    return output;
  }

  private static TransformMeta addTransform(
      PipelineMeta pipelineMeta, String name, ITransformMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    TransformMeta transformMeta = new TransformMeta(pluginId, name, meta);
    pipelineMeta.addTransform(transformMeta);
    return transformMeta;
  }
}
