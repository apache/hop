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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IEngineMetric;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;

/**
 * Base for Beam pipeline tests that need to assert on what a transform actually produced.
 *
 * <p>{@link PipelineTestBase} runs a pipeline and prints the metrics, but it discards the engine
 * and leaves the output folder untouched, so a test extending it can neither read the output nor
 * check a row count. Every new Beam transform handler (umbrella #8708) needs exactly those two
 * assertions, so they live here.
 *
 * <p>Use it together with {@link org.apache.hop.beam.util.BeamPipelineMetaUtil}, which builds the
 * pipeline shapes, for example {@code GroupByPipelineTest}.
 */
public abstract class SingleTransformPipelineTestBase extends PipelineTestBase {

  /**
   * Where {@link org.apache.hop.beam.transforms.io.BeamOutputMeta} writes in the test utilities.
   */
  public static final String OUTPUT_FOLDER = "/tmp/customers/output";

  /**
   * Delete everything the previous run left behind.
   *
   * <p>Without this, output files accumulate across tests in the same JVM and a test cannot tell
   * which rows its own run produced. Beam also leaves {@code .temp-beam-*} staging folders behind,
   * so hidden entries are removed too.
   */
  protected void clearOutputFolder() {
    File outputFolder = new File(OUTPUT_FOLDER);
    if (!outputFolder.exists()) {
      outputFolder.mkdirs();
      return;
    }
    File[] entries = outputFolder.listFiles();
    if (entries == null) {
      return;
    }
    for (File entry : entries) {
      deleteRecursively(entry);
    }
  }

  private void deleteRecursively(File file) {
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      if (children != null) {
        Arrays.stream(children).forEach(this::deleteRecursively);
      }
    }
    // A leftover staging folder is best effort; a test failing to delete it is not the point.
    file.delete();
  }

  /** Run the pipeline and return the output files it produced, in a stable order. */
  protected List<File> runAndGetOutputFiles(PipelineMeta pipelineMeta) throws Exception {
    clearOutputFolder();
    createRunPipeline(variables, pipelineMeta);

    File outputFolder = new File(OUTPUT_FOLDER);
    File[] files = outputFolder.listFiles((dir, name) -> !name.startsWith("."));
    if (files == null || files.length == 0) {
      throw new HopException(
          "The pipeline '"
              + pipelineMeta.getName()
              + "' produced no output files in "
              + OUTPUT_FOLDER);
    }
    List<File> result = new ArrayList<>(Arrays.asList(files));
    result.sort(Comparator.comparing(File::getName));
    return result;
  }

  /**
   * Run the pipeline and return the output as a list of lines, in file order.
   *
   * <p>Only valid for uncompressed output. A test for compressed output (#2337) should use {@link
   * #runAndGetOutputFiles} and decompress the bytes itself.
   */
  protected List<String> runAndGetOutputLines(PipelineMeta pipelineMeta) throws Exception {
    List<String> lines = new ArrayList<>();
    for (File file : runAndGetOutputFiles(pipelineMeta)) {
      for (String line : Files.readAllLines(file.toPath(), StandardCharsets.UTF_8)) {
        if (!line.isBlank()) {
          lines.add(line);
        }
      }
    }
    return lines;
  }

  /**
   * Run the pipeline and return the raw bytes of every output file, keyed by file name.
   *
   * <p>Beam shards file output, so a 100-row batch arrives as several {@code name-00000-of-000NN}
   * files. A compression test (#2337) must therefore inspect all of them, not assume one file.
   */
  protected Map<String, byte[]> runAndGetOutputBytesByFile(PipelineMeta pipelineMeta)
      throws Exception {
    Map<String, byte[]> bytes = new LinkedHashMap<>();
    for (File file : runAndGetOutputFiles(pipelineMeta)) {
      bytes.put(file.getName(), Files.readAllBytes(file.toPath()));
    }
    return bytes;
  }

  /**
   * Run the pipeline to completion and return the engine, so a test can read its metrics.
   *
   * <p>This is the engine-owning counterpart of {@link #createRunPipeline}, which discards the
   * engine after printing. Callers must not call both for the same pipeline.
   */
  protected IPipelineEngine<PipelineMeta> runPipeline(PipelineMeta pipelineMeta) throws Exception {
    // For safety, look up info and target transforms, if this is not yet done.
    // It doesn't hurt to run it again.
    //
    pipelineMeta.lookupReferencesAfterLoading();

    IPipelineEngine<PipelineMeta> engine =
        PipelineEngineFactory.createPipelineEngine(
            variables, NAME_RUN_CONFIG, metadataProvider, pipelineMeta);
    engine.execute();
    engine.waitUntilFinished();
    return engine;
  }

  /**
   * Run the pipeline and return a counter the named transform reported, for example {@link
   * Pipeline#METRIC_NAME_INPUT} or {@link Pipeline#METRIC_NAME_OUTPUT}.
   *
   * <p>Beam counters are namespaced by transform name; {@link
   * org.apache.hop.beam.engines.BeamPipelineEngine#populateEngineMetrics()} maps them onto engine
   * components.
   *
   * <p>Which counter a transform reports depends on its role, so this takes the metric name rather
   * than hard-coding one: the Beam file input counts {@code input} and {@code written}, while the
   * Beam file output counts {@code read} and {@code output}. Asking for a counter the transform
   * never increments throws, because a silently missing counter is a handler that forgot to report.
   *
   * <p>The engine swaps in fresh metrics from a background timer, so the value is not necessarily
   * there the instant {@code waitUntilFinished} returns. Poll until it appears rather than racing.
   */
  protected long runAndGetMetric(
      PipelineMeta pipelineMeta, String transformName, IEngineMetric metric) throws Exception {
    IPipelineEngine<PipelineMeta> engine = runPipeline(pipelineMeta);
    Long value = awaitMetric(engine, transformName, metric, 30_000L);
    if (value != null) {
      return value;
    }
    throw describeMissingMetric(engine, transformName, metric);
  }

  /**
   * Wait up to {@code timeoutMs} for a counter to show up, returning null if it never does.
   *
   * <p>Pair this with {@link #findMetric} on the <em>same</em> engine. The engine publishes metrics
   * from a background timer, so an immediate read can come back empty even on a pipeline that has
   * finished. Running the pipeline a second time to get a fresh engine does not help, a new engine
   * starts out with no metrics at all.
   */
  protected Long awaitMetric(
      IPipelineEngine<PipelineMeta> engine,
      String transformName,
      IEngineMetric metric,
      long timeoutMs)
      throws InterruptedException {
    // The refresh timer ticks every second; give it a few ticks to land the final counters.
    //
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (true) {
      Long value = findMetric(engine, transformName, metric);
      if (value != null) {
        return value;
      }
      if (System.currentTimeMillis() >= deadline) {
        return null;
      }
      Thread.sleep(250L);
    }
  }

  /**
   * Read a counter without waiting for it to appear.
   *
   * <p>Use this for the negative cases, asserting that a transform does <em>not</em> report
   * something, where waiting for a value that will never arrive would just waste the timeout.
   */
  protected Long findMetric(
      IPipelineEngine<PipelineMeta> engine, String transformName, IEngineMetric metric) {
    IEngineComponent component = engine.findComponent(transformName, 0);
    if (component == null) {
      return null;
    }
    return engine.getEngineMetrics().getComponentMetric(component, metric);
  }

  /** Build the error for a transform that reported no component, or no such counter. */
  protected HopException describeMissingMetric(
      IPipelineEngine<PipelineMeta> engine, String transformName, IEngineMetric metric) {
    EngineMetrics metrics = engine.getEngineMetrics();
    List<String> componentsSeen =
        metrics.getComponents().stream().map(IEngineComponent::getName).toList();

    if (!componentsSeen.contains(transformName)) {
      return new HopException(
          "No metrics were reported for transform '"
              + transformName
              + "'.  Components seen: "
              + componentsSeen);
    }
    return new HopException(
        "Transform '"
            + transformName
            + "' reported no '"
            + metric.getCode()
            + "' counter.  Its Beam handler has to increment that counter.");
  }

  /** The row layout a transform produces, for asserting on field types. */
  protected IRowMeta getTransformFields(PipelineMeta pipelineMeta, String transformName)
      throws HopTransformException {
    return pipelineMeta.getTransformFields(variables, pipelineMeta.findTransform(transformName));
  }

  /** Convenience for tests that just want the file contents as one string. */
  protected String runAndGetOutputText(PipelineMeta pipelineMeta) throws Exception {
    return String.join("\n", runAndGetOutputLines(pipelineMeta));
  }
}
