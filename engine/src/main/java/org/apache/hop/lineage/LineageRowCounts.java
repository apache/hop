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

package org.apache.hop.lineage;

import java.util.List;
import java.util.Map;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;

/**
 * The row counters of a run, as attributes of its terminal lifecycle event.
 *
 * <p>A transform's {@code FINISHED} or {@code FAILED} event carries the counters of that copy,
 * final by then. A pipeline's terminal event carries the totals of all its transforms and copies
 * for the counters that add up: rows input from and output to datasets, updated and rejected. Rows
 * read and written between transforms don't add up across transforms, so a pipeline has none.
 *
 * <p>Only the local pipeline engine has final counters when its terminal event is sent; other
 * engines send none rather than numbers that are still being filled in.
 */
public final class LineageRowCounts {

  /** Rows a transform read from the transforms before it. */
  public static final String LINES_READ = "linesRead";

  /** Rows a transform wrote to the transforms after it. */
  public static final String LINES_WRITTEN = "linesWritten";

  /** Rows a transform read from a dataset: a file, a table, ... */
  public static final String LINES_INPUT = "linesInput";

  /** Rows a transform wrote to a dataset. */
  public static final String LINES_OUTPUT = "linesOutput";

  /** Rows a transform updated in a dataset. */
  public static final String LINES_UPDATED = "linesUpdated";

  /** Rows a transform rejected, such as rows sent to its error handling. */
  public static final String LINES_REJECTED = "linesRejected";

  /** Every counter, in the order a sink shows them. */
  public static final List<String> COUNTERS =
      List.of(LINES_READ, LINES_WRITTEN, LINES_INPUT, LINES_OUTPUT, LINES_UPDATED, LINES_REJECTED);

  private LineageRowCounts() {}

  /** Adds the counters of one transform copy. */
  static void putTransformCounts(LineageContext.Builder ctx, IEngineComponent component) {
    if (component == null) {
      return;
    }
    ctx.putAttribute(LINES_READ, Long.toString(component.getLinesRead()));
    ctx.putAttribute(LINES_WRITTEN, Long.toString(component.getLinesWritten()));
    ctx.putAttribute(LINES_INPUT, Long.toString(component.getLinesInput()));
    ctx.putAttribute(LINES_OUTPUT, Long.toString(component.getLinesOutput()));
    ctx.putAttribute(LINES_UPDATED, Long.toString(component.getLinesUpdated()));
    ctx.putAttribute(LINES_REJECTED, Long.toString(component.getLinesRejected()));
  }

  /** Adds the totals of all transforms and copies of a pipeline run on the local engine. */
  static void putPipelineTotals(
      LineageContext.Builder ctx, IPipelineEngine<PipelineMeta> pipeline) {
    if (!(pipeline instanceof Pipeline)) {
      return;
    }
    long input = 0;
    long output = 0;
    long updated = 0;
    long rejected = 0;
    List<IEngineComponent> components = pipeline.getComponents();
    if (components == null) {
      return;
    }
    for (IEngineComponent component : components) {
      if (component != null) {
        input += component.getLinesInput();
        output += component.getLinesOutput();
        updated += component.getLinesUpdated();
        rejected += component.getLinesRejected();
      }
    }
    ctx.putAttribute(LINES_INPUT, Long.toString(input));
    ctx.putAttribute(LINES_OUTPUT, Long.toString(output));
    ctx.putAttribute(LINES_UPDATED, Long.toString(updated));
    ctx.putAttribute(LINES_REJECTED, Long.toString(rejected));
  }

  /**
   * A counter from the attributes of an event.
   *
   * @return null when the event doesn't have it
   */
  public static Long get(Map<String, String> attributes, String counter) {
    if (attributes == null) {
      return null;
    }
    String value = attributes.get(counter);
    if (value == null) {
      return null;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
