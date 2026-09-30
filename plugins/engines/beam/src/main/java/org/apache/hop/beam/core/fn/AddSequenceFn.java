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

package org.apache.hop.beam.core.fn;

import java.io.Serial;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.pipeline.Pipeline;

/**
 * Issue #2379: appends a sequence number to every incoming row, with one counter shared by every
 * worker.
 *
 * <p>The counter lives in Beam state rather than in a field, because on a distributed runner each
 * worker holds its own instance of this class: a plain field would restart at the same value on
 * every worker and emit duplicates. The handler keys every row on the same {@code null} key, so
 * there is exactly one state cell for the whole pipeline.
 *
 * <p>The input is a {@code KV} because a stateful {@code ParDo} requires one; the key is always
 * null and carries no information, only the fact that everything belongs to one key.
 *
 * <p>{@code maxValue} is applied here rather than by truncating the output, so a row past the limit
 * is dropped, which is what the local transform does.
 */
public class AddSequenceFn extends DoFn<KV<Void, HopRow>, HopRow> {

  @Serial private static final long serialVersionUID = 1L;

  private final String transformName;
  private final String rowMetaJson;
  private final String valueName;
  private final long startAt;
  private final long incrementBy;
  private final long maxValue;

  // Not final: they are initialised from transformName, which the constructor only assigns in
  // its body, so a field initialiser would still see null.  The repo's other DoFns do the same.
  private final Counter numErrors = Metrics.counter("main", "BeamAddSequenceErrors");
  private final Counter inputCounter;
  private final Counter writtenCounter;
  private final Counter droppedCounter;

  private transient IRowMeta rowMeta;
  private transient IValueMeta valueMeta;

  /**
   * The spec for the counter. {@code @StateId} is field-only and the annotated field has to be a
   * final {@code StateSpec}, which is the form Beam's own javadoc prescribes.
   */
  @StateId("counter")
  private final StateSpec<ValueState<Long>> counterSpec = StateSpecs.value(VarLongCoder.of());

  public AddSequenceFn(
      String transformName,
      String rowMetaJson,
      String valueName,
      long startAt,
      long incrementBy,
      long maxValue) {
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.valueName = valueName;
    this.startAt = startAt;
    this.incrementBy = incrementBy;
    this.maxValue = maxValue;
    this.inputCounter = Metrics.counter(Pipeline.METRIC_NAME_INPUT, transformName);
    this.writtenCounter = Metrics.counter(Pipeline.METRIC_NAME_WRITTEN, transformName);
    this.droppedCounter = Metrics.counter(Pipeline.METRIC_NAME_REJECTED, transformName);
  }

  @Setup
  public void setUp() throws HopException {
    BeamHop.init();
    // The row meta handed to us already carries the sequence field: the converter built it from
    // AddSequenceMeta.getFields(). Adding another one here would grow the row to 12 slots while
    // the output transform only reads 11, which silently drops the value.
    rowMeta = JsonRowMeta.fromJson(rowMetaJson);
    valueMeta = rowMeta.searchValueMeta(valueName);
    if (valueMeta == null) {
      throw new HopException(
          "The row layout handed to the Add Sequence function has no field '" + valueName + "'");
    }
    Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
  }

  @ProcessElement
  public void processElement(ProcessContext context, @StateId("counter") ValueState<Long> counter) {
    try {
      inputCounter.inc();

      Long current = counter.read();
      long value = (current == null) ? startAt : current + incrementBy;
      counter.write(value);

      if (maxValue > 0 && value > maxValue) {
        // The local transform stops emitting once the maximum is reached.
        droppedCounter.inc();
        return;
      }

      HopRow row = context.element().getValue();
      Object[] output = new Object[rowMeta.size()];
      System.arraycopy(row.getRow(), 0, output, 0, row.getRow().length);
      output[rowMeta.size() - 1] = value;

      context.output(new HopRow(output));
      writtenCounter.inc();

    } catch (Exception e) {
      numErrors.inc();
      throw new RuntimeException("Error adding a sequence value in " + transformName, e);
    }
  }
}
