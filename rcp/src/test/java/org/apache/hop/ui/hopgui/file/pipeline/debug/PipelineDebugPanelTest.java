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

package org.apache.hop.ui.hopgui.file.pipeline.debug;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.CursorRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.debug.DebugController;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.layout.FillLayout;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The debugger metadata table lists every field of the selected hop. The same selection is what a
 * click on the canvas pill performs.
 */
@Tag("uitest")
class PipelineDebugPanelTest extends SwtBotTestBase {

  @Test
  void metadataTableFollowsTheSelectedHopAndCanShowEveryHop() throws Exception {
    LocalPipelineEngine engine = engine();
    DebugController controller = engine.getDebugController();
    CursorRowSet first = hop(controller, "a", "b");
    CursorRowSet second = hop(controller, "b", "c");
    describe(first, "name");
    describe(second, "city");

    AtomicReference<PipelineDebugPanel> panelRef = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          shell.setSize(1100, 600);
          PipelineDebugPanel panel =
              new PipelineDebugPanel(
                  shell, new Variables(), () -> controller, name -> {}, () -> {}, () -> {});
          panel.refresh();
          panelRef.set(panel);
        },
        bot -> {
          onUi(
              () -> {
                assertEquals("name", fieldName(panelRef.get()));
                panelRef.get().selectHop("b", "c");
                assertEquals("city", fieldName(panelRef.get()));
                assertEquals(1, panelRef.get().getMetadataView().getTable().getItemCount());
                return null;
              });
          engine.cleanup();
        });
  }

  private static LocalPipelineEngine engine() throws HopException {
    PipelineMeta meta = new PipelineMeta();
    meta.setName("debug-panel");
    TransformMeta a = new TransformMeta("a", new DummyMeta());
    TransformMeta b = new TransformMeta("b", new DummyMeta());
    TransformMeta c = new TransformMeta("c", new DummyMeta());
    meta.addTransform(a);
    meta.addTransform(b);
    meta.addTransform(c);
    meta.addPipelineHop(new PipelineHopMeta(a, b));
    meta.addPipelineHop(new PipelineHopMeta(b, c));
    LocalPipelineEngine engine =
        new LocalPipelineEngine(meta, new Variables(), new LoggingObject("debug-panel"));
    engine.setDebugController(new DebugController(engine));
    engine.prepareExecution();
    return engine;
  }

  private static CursorRowSet hop(DebugController controller, String from, String to) {
    for (CursorRowSet rowSet : controller.getCursorRowSets()) {
      if (from.equals(rowSet.getOriginTransformName())
          && to.equals(rowSet.getDestinationTransformName())) {
        return rowSet;
      }
    }
    throw new AssertionError("Missing hop " + from + " -> " + to);
  }

  private static void describe(CursorRowSet rowSet, String field) {
    IRowMeta rowMeta = new RowMeta();
    ValueMetaString value = new ValueMetaString(field);
    value.setOrigin(rowSet.getOriginTransformName());
    rowMeta.addValueMeta(value);
    rowSet.setRowMeta(rowMeta);
  }

  /** Field-name column. TableView column 0 is the row number. */
  private static String fieldName(PipelineDebugPanel panel) {
    return panel.getMetadataView().getTable().getItem(0).getText(1);
  }

  private static <T> T onUi(java.util.function.Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    AtomicReference<RuntimeException> failure = new AtomicReference<>();
    display.syncExec(
        () -> {
          try {
            result.set(supplier.get());
          } catch (RuntimeException e) {
            failure.set(e);
          }
        });
    if (failure.get() != null) {
      throw failure.get();
    }
    return result.get();
  }
}
