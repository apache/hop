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

package org.apache.hop.beam.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.junit.jupiter.api.Test;

/**
 * Proves the helpers in {@link SingleTransformPipelineTestBase} actually work, using the plain Beam
 * input to output pipeline.
 *
 * <p>Without this, the base class would ship unexercised and every Wave 2 and Wave 3 test would be
 * built on helpers that may silently return the wrong thing.
 */
class SingleTransformPipelineTestBaseTest extends SingleTransformPipelineTestBase {

  /** The customers-100.txt fixture holds 100 rows. */
  private static final int EXPECTED_ROWS = 100;

  private PipelineMeta simplePipeline() throws Exception {
    return BeamPipelineMetaUtil.generateBeamInputOutputPipelineMeta(
        "base-class-selftest", "INPUT", "OUTPUT", metadataProvider);
  }

  @Test
  void readsEveryInputRowBackFromTheOutputFiles() throws Exception {
    PipelineMeta pipelineMeta = simplePipeline();

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    assertEquals(
        EXPECTED_ROWS,
        lines.size(),
        "expected one output line per input row across all shards, got " + lines.size());
  }

  @Test
  void outputIsShardedAndEveryShardIsVisible() throws Exception {
    PipelineMeta pipelineMeta = simplePipeline();

    List<File> files = runAndGetOutputFiles(pipelineMeta);

    // Beam's file sink shards its output, so more than one file is normal.  A helper that assumed
    // a single file would silently miss data.
    assertTrue(files.size() > 1, "expected Beam to shard the output, got a single file: " + files);
    for (File file : files) {
      assertTrue(
          file.getName().matches("customers-\\d{5}-of-\\d{5}\\.csv"),
          "unexpected shard name: " + file.getName());
      assertTrue(file.length() > 0, "empty shard: " + file.getName());
    }
  }

  @Test
  void everyRowKeepsTheInputFieldLayout() throws Exception {
    PipelineMeta pipelineMeta = simplePipeline();

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    // The Beam output transform uses the default file definition, which separates with a comma,
    // while the customers file is semicolon separated, so each line splits into 10 columns.
    for (String line : lines) {
      assertEquals(
          10, line.split(",", -1).length, "every row should carry all 10 fields, got: " + line);
    }
  }

  @Test
  void allTheCustomerIdsComeThroughExactlyOnce() throws Exception {
    PipelineMeta pipelineMeta = simplePipeline();

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    // The first column is the id.  Collect them into a set to prove no row was lost or
    // duplicated, without depending on the order the shards happen to be written in.
    Set<String> ids = new TreeSet<>();
    for (String line : lines) {
      ids.add(line.split(",", -1)[0].trim());
    }
    assertEquals(
        EXPECTED_ROWS, ids.size(), "expected 100 distinct customer ids, got " + ids.size());
    assertTrue(ids.contains("1"), "customer 1 is missing from " + ids);
    assertTrue(ids.contains("100"), "customer 100 is missing from " + ids);
  }

  @Test
  void staleOutputFromAPreviousRunIsClearedFirst() throws Exception {
    // Simulate leftovers from an earlier run, including a Beam staging folder.
    //
    File outputFolder = new File(OUTPUT_FOLDER);
    outputFolder.mkdirs();
    File stale = new File(outputFolder, "stale-from-a-previous-run.csv");
    assertTrue(stale.createNewFile());
    File staleStaging = new File(outputFolder, ".temp-beam-leftover");
    assertTrue(staleStaging.mkdirs());

    PipelineMeta pipelineMeta = simplePipeline();
    List<File> files = runAndGetOutputFiles(pipelineMeta);

    assertFalse(
        files.stream().anyMatch(f -> f.getName().startsWith("stale")),
        "the stale file from the previous run must be gone: " + files);
    assertFalse(
        files.stream().anyMatch(f -> f.getName().startsWith(".")),
        "hidden Beam staging folders must not be reported as output: " + files);
  }

  @Test
  void rawBytesAreAvailablePerShardForCompressedOutputTests() throws Exception {
    PipelineMeta pipelineMeta = simplePipeline();

    // One run only.  A second run* helper here would execute the pipeline again, and Beam picks
    // a different shard count per run, so the two file lists would not line up.
    Map<String, byte[]> bytesByFile = runAndGetOutputBytesByFile(pipelineMeta);

    assertTrue(bytesByFile.size() > 1, "expected sharded output, got " + bytesByFile.keySet());
    for (Map.Entry<String, byte[]> entry : bytesByFile.entrySet()) {
      byte[] bytes = entry.getValue();
      assertTrue(bytes.length > 0, "empty shard " + entry.getKey());

      // The point of this helper is to let a compression test (#2337) tell plain output from
      // compressed output, so assert exactly that: the gzip magic number 0x1f 0x8b must be
      // absent.  Do not assert on the first character, the id field carries a " #" conversion
      // mask and is written space padded.
      //
      boolean gzipped = bytes.length >= 2 && bytes[0] == (byte) 0x1f && bytes[1] == (byte) 0x8b;
      assertFalse(
          gzipped, "uncompressed output should not carry the gzip magic: " + entry.getKey());
    }
  }

  @Test
  void inputTransformReportsAnInputCounter() throws Exception {
    PipelineMeta pipelineMeta = simplePipeline();

    long input = runAndGetMetric(pipelineMeta, "INPUT", Pipeline.METRIC_INPUT);

    assertEquals(EXPECTED_ROWS, input, "the input transform should have counted 100 input rows");
  }

  @Test
  void outputTransformReportsAnOutputCounter() throws Exception {
    PipelineMeta pipelineMeta = simplePipeline();

    // The Beam file output counts 'read' and 'output'; it never increments 'written'.  Asking for
    // the right counter is exactly what runAndGetMetric exists for.
    //
    long output = runAndGetMetric(pipelineMeta, "OUTPUT", Pipeline.METRIC_OUTPUT);

    assertEquals(EXPECTED_ROWS, output, "the output transform should have counted 100 output rows");
  }

  @Test
  void askingForACounterATransformNeverIncrementsFailsLoudly() throws Exception {
    PipelineMeta pipelineMeta = simplePipeline();

    // The Beam file output counts 'read' and 'output'; it never increments 'written'.  Wait for a
    // counter that does exist so the engine has definitely published its metrics, then look up
    // the absent one on that same engine.  A second run starts with no metrics at all.
    //
    IPipelineEngine<PipelineMeta> engine = runPipeline(pipelineMeta);
    assertNotNull(
        awaitMetric(engine, "OUTPUT", Pipeline.METRIC_OUTPUT, 30_000L),
        "the output transform should report an 'output' counter");
    assertNull(
        findMetric(engine, "OUTPUT", Pipeline.METRIC_WRITTEN),
        "the file output transform should not report a 'written' counter");

    HopException exception = describeMissingMetric(engine, "OUTPUT", Pipeline.METRIC_WRITTEN);
    assertTrue(
        exception.getMessage().contains("written"),
        "the error should name the missing counter, was: " + exception.getMessage());
  }

  @Test
  void anUnknownTransformNameFailsLoudly() throws Exception {
    PipelineMeta pipelineMeta = simplePipeline();

    // Give the engine a chance to publish metrics at all, then ask for a transform that is not
    // in the pipeline.
    //
    IPipelineEngine<PipelineMeta> engine = runPipeline(pipelineMeta);
    assertNotNull(
        awaitMetric(engine, "INPUT", Pipeline.METRIC_INPUT, 30_000L),
        "the input transform should report an 'input' counter");

    HopException exception =
        describeMissingMetric(engine, "NO-SUCH-TRANSFORM", Pipeline.METRIC_INPUT);
    assertTrue(
        exception.getMessage().contains("NO-SUCH-TRANSFORM"),
        "the error should name the transform, was: " + exception.getMessage());
  }

  @Test
  void transformFieldsAreReachableFromTheBase() throws Exception {
    PipelineMeta pipelineMeta = simplePipeline();

    IRowMeta rowMeta = getTransformFields(pipelineMeta, "INPUT");

    assertEquals(10, rowMeta.size(), "the customers file definition has 10 fields");
    assertEquals("id", rowMeta.getValueMeta(0).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(0).getType());
    assertEquals("name", rowMeta.getValueMeta(1).getName());
  }

  @Test
  void outputTextJoinsTheLinesAcrossShards() throws Exception {
    PipelineMeta pipelineMeta = simplePipeline();

    String text = runAndGetOutputText(pipelineMeta);

    List<String> lines = List.of(text.split("\n"));
    assertEquals(EXPECTED_ROWS, lines.size(), "expected 100 lines, got " + lines.size());
    assertTrue(
        lines.stream().anyMatch(l -> l.contains("ALASKA")),
        "the first customer is from ALASKA and should be in the output");
  }
}
