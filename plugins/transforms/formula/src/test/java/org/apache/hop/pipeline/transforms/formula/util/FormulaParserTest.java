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

package org.apache.hop.pipeline.transforms.formula.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogChannelFactory;
import org.apache.hop.core.logging.ILogMessage;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.SupplementalPlugin;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.PipelineEngineCapabilities;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.formula.Formula;
import org.apache.hop.pipeline.transforms.formula.FormulaData;
import org.apache.hop.pipeline.transforms.formula.FormulaMeta;
import org.apache.hop.pipeline.transforms.formula.FormulaMetaFunction;
import org.junit.jupiter.api.Test;

class FormulaParserTest {
  static {
    HopLogStore.setLogChannelFactory(new ConsoleLogChannelFactory());

    try {
      final var plugin = new SupplementalPlugin(ValueMetaPluginType.class, "2");
      plugin.addFactory(IValueMeta.class, ValueMetaString::new);
      PluginRegistry.getInstance().registerPlugin(ValueMetaPluginType.class, plugin);
    } catch (final HopPluginException e) {
      throw new IllegalStateException(e);
    }
  }

  // mainly a perf test since originally it was very slow
  @Test
  void formula() throws HopException {
    final var rowMeta = new RowMeta();
    rowMeta.setValueMetaList(
        List.of(
            new ValueMetaBigNumber("FOO"),
            new ValueMetaString("BAR"),
            new ValueMetaString("DUMMY"),
            new ValueMetaString("LAST")));

    final int maxSize =
        Integer.getInteger(getClass().getName() + ".formula.maxSize", /*1_000_000*/ 1_000);
    final var row = new BlockingRowSet(maxSize);
    for (var i = 0; i < maxSize; i++) {
      row.putRow(rowMeta, new Object[] {new BigDecimal(-i), "A", "B", "B"});
    }
    row.setDone();

    final var meta = new FormulaMeta();
    meta.setFormulas(
        List.of(
            new FormulaMetaFunction(
                "EVAL1",
                "IF(" + "[BAR] = \"A\", " + "IF(ISBLANK([DUMMY]), [LAST], [DUMMY])," + " [LAST])",
                IValueMeta.TYPE_STRING,
                -1,
                -1,
                "",
                false),
            new FormulaMetaFunction(
                "EVAL2",
                "TEXT(ABS([FOO]) , \"#.#######\")",
                IValueMeta.TYPE_STRING,
                -1,
                -1,
                "",
                false)));

    final var transformMeta = new BaseTransformMeta<>();
    final var transformMetadata = new TransformMeta("1", "test", transformMeta);

    final var pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(transformMetadata);

    final var pipeline =
        new Pipeline() {
          private final PipelineEngineCapabilities pipelineEngineCapabilities =
              new PipelineEngineCapabilities();
          private final PipelineRunConfiguration pipelineRunConfiguration =
              new PipelineRunConfiguration();

          @Override
          public PipelineEngineCapabilities getEngineCapabilities() {
            return pipelineEngineCapabilities;
          }

          @Override
          public PipelineRunConfiguration getPipelineRunConfiguration() {
            return pipelineRunConfiguration;
          }

          @Override
          public String getStatusDescription() {
            return "";
          }
        };
    pipeline.setRunning(true);

    final var data = new FormulaData();
    data.outputRowMeta = new RowMeta();

    final var formula = new Formula(transformMetadata, meta, data, 1, pipelineMeta, pipeline);

    final var counter = new AtomicInteger();
    formula.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(final IRowMeta rowMeta, final Object[] row) {
            assertEquals("B", row[4]);
            assertEquals(Integer.toString(counter.getAndIncrement()), row[5]);
          }
        });

    formula.init();
    formula.startBundle();
    formula.setInputRowSets(new ArrayList<>(List.of(row)));
    while (formula.processRow())
      ;
    formula.finishBundle();
    formula.setOutputDone();
    pipeline.setRunning(false);

    assertEquals(maxSize, counter.get());
  }

  private static class ConsoleLogChannelFactory implements ILogChannelFactory {
    private final ILogChannel simpleLog =
        new LogChannel("test") {
          @Override
          public void println(final ILogMessage logMessage, final LogLevel channelLogLevel) {
            System.out.println("[" + channelLogLevel + "] " + logMessage);
          }
        };

    @Override
    public ILogChannel create(final Object subject) {
      return simpleLog;
    }

    @Override
    public ILogChannel create(final Object subject, final boolean gatheringMetrics) {
      return simpleLog;
    }

    @Override
    public ILogChannel create(final Object subject, final ILoggingObject parentObject) {
      return simpleLog;
    }

    @Override
    public ILogChannel create(
        final Object subject, final ILoggingObject parentObject, final boolean gatheringMetrics) {
      return simpleLog;
    }
  }
}
