/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.beam.transforms.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("uitest")
class BeamElasticsearchDialogTest extends SwtBotTestBase {
  @BeforeAll
  static void registerWidgets() {
    for (Class<?> type :
        new Class<?>[] {BeamElasticsearchInputMeta.class, BeamElasticsearchOutputMeta.class}) {
      for (Field field : type.getDeclaredFields()) {
        GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
        if (widget != null) {
          GuiRegistry.getInstance().addGuiWidgetElement(type.getName(), widget, field);
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"Input", "Output"})
  void okPersistsConnectionAndDocumentOptionsThroughGroupedWidgets(String kind) throws Exception {
    BaseTransformMeta<?, ?> meta = meta(kind);
    Class<?> dialogClass = Class.forName(meta.getDialogClassName());
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.addTransform(new TransformMeta("BeamElasticsearch" + kind, "elastic", meta));
    withDialog(
        parent -> open(dialogClass, parent, meta, pipeline),
        bot -> {
          SWTBot dialog = bot.shell("Beam Elasticsearch " + kind.toLowerCase()).activate().bot();
          dialog.text(1).setText("https://localhost:9200");
          dialog.text(2).setText("docs");
          dialog.text(3).setText("legacy");
          dialog.text(4).setText("reader");
          dialog.text(5).setText("test-password");
          display.syncExec(
              () ->
                  assertNotEquals(
                      0, dialog.text(5).widget.getEchoChar(), "password must be masked"));
          dialog.cTabItem("Documents").activate();
          // SWTBot filters out fields on the now-hidden Connection tab.
          dialog.text(1).setText("payload");
          if (kind.equals("Input")) {
            dialog.text(2).setText("{\"query\":{\"match_all\":{}}}");
            dialog.text(3).setText("2m");
          } else {
            dialog.text(2).setText("17");
            dialog.text(3).setText("4096");
          }
          display.syncExec(
              () -> {
                Shell shell = dialog.text(0).widget.getShell();
                shell.setSize(620, 390);
                shell.layout(true, true);
                org.eclipse.swt.widgets.Button ok =
                    dialog.button(buttonLabel("System.Button.OK")).widget;
                assertTrue(ok.getBounds().height > 0);
                assertTrue(
                    ok.getParent().toDisplay(ok.getLocation()).y + ok.getSize().y
                        <= shell.toDisplay(
                                shell.getClientArea().x,
                                shell.getClientArea().y + shell.getClientArea().height)
                            .y);
              });
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals("https://localhost:9200", meta.getClass().getMethod("getHosts").invoke(meta));
    assertEquals("docs", meta.getClass().getMethod("getIndex").invoke(meta));
    assertEquals("reader", meta.getClass().getMethod("getUsername").invoke(meta));
    assertEquals("test-password", meta.getClass().getMethod("getPassword").invoke(meta));
    assertEquals("payload", meta.getClass().getMethod("getJsonField").invoke(meta));
    if (kind.equals("Input")) {
      assertEquals("2m", ((BeamElasticsearchInputMeta) meta).getScrollKeepalive());
    } else {
      assertEquals("17", ((BeamElasticsearchOutputMeta) meta).getMaxBatchSize());
      assertEquals("4096", ((BeamElasticsearchOutputMeta) meta).getMaxBatchBytes());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"Input", "Output"})
  void cancelDoesNotPersistEdits(String kind) throws Exception {
    BaseTransformMeta<?, ?> meta = meta(kind);
    Class<?> dialogClass = Class.forName(meta.getDialogClassName());
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.addTransform(new TransformMeta("BeamElasticsearch" + kind, "elastic", meta));
    withDialog(
        parent -> open(dialogClass, parent, meta, pipeline),
        bot -> {
          SWTBot dialog = bot.shell("Beam Elasticsearch " + kind.toLowerCase()).activate().bot();
          dialog.text(1).setText("discarded");
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertNull(meta.getClass().getMethod("getHosts").invoke(meta));
    assertFalse(meta.hasChanged());
  }

  static BaseTransformMeta<?, ?> meta(String kind) {
    return kind.equals("Input")
        ? new BeamElasticsearchInputMeta()
        : new BeamElasticsearchOutputMeta();
  }

  static void open(
      Class<?> dialogClass, Shell shell, BaseTransformMeta<?, ?> meta, PipelineMeta pipeline) {
    try {
      ((BaseTransformDialog)
              dialogClass
                  .getConstructor(
                      Shell.class, IVariables.class, meta.getClass(), PipelineMeta.class)
                  .newInstance(shell, new Variables(), meta, pipeline))
          .open();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
