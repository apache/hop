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

package org.apache.hop.beam.transforms.window;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.hop.beam.core.BeamDefaults;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.joda.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A keyed global window has to carry the trigger before GroupByKey, not only the WindowFn. */
class BeamWindowKeyedTriggerTest {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void keyedGlobalWindowKeepsTriggerLatenessAndDiscarding() throws Exception {
    PCollection<HopRow> rows =
        handle(WindowTriggerType.RepeatedlyForeverAfterWatermarkPastEndOfWindow);

    WindowingStrategy<?, ?> strategy = rows.getWindowingStrategy();
    assertTrue(strategy.isTriggerSpecified(), strategy.toString());
    assertTrue(
        strategy.getTrigger().toString().contains("Repeatedly"), strategy.getTrigger().toString());
    assertEquals(Duration.standardSeconds(30), strategy.getAllowedLateness());
    assertEquals(AccumulationMode.DISCARDING_FIRED_PANES, strategy.getMode());
  }

  @Test
  void keyedGlobalWindowWithoutATriggerIsRejected() {
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> handle(WindowTriggerType.None));

    assertTrue(exception.getMessage().contains("trigger"), exception.getMessage());
  }

  private static PCollection<HopRow> handle(WindowTriggerType trigger) throws Exception {
    BeamWindowMeta meta = new BeamWindowMeta();
    meta.setWindowType(BeamDefaults.WINDOW_TYPE_GLOBAL);
    meta.setTriggeringType(trigger);
    meta.setAllowedLateness("30");
    meta.setDiscardingFiredPanes(true);
    meta.setKeyField("id");

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));

    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> input =
        pipeline
            .apply(GenerateSequence.from(0))
            .apply(ParDo.of(new LongToHopRowFn()))
            .setCoder(new HopRowCoder());

    Map<String, PCollection<HopRow>> collections = new HashMap<>();
    meta.handleTransform(
        LogChannel.GENERAL,
        new Variables(),
        "direct",
        null,
        null,
        null,
        new PipelineMeta(),
        new TransformMeta("BeamWindow", "window", meta),
        collections,
        pipeline,
        rowMeta,
        List.of(),
        input,
        null);
    return collections.get("window");
  }

  private static class LongToHopRowFn extends DoFn<Long, HopRow> {
    @ProcessElement
    public void process(ProcessContext context) {
      context.output(new HopRow(new Object[] {context.element().toString()}));
    }
  }
}
