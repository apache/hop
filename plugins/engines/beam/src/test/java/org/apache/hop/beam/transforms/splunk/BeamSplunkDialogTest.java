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
package org.apache.hop.beam.transforms.splunk;

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
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("uitest")
class BeamSplunkDialogTest extends SwtBotTestBase {
  @BeforeAll
  static void registerWidgets() {
    for (Field field : BeamSplunkOutputMeta.class.getDeclaredFields()) {
      GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
      if (widget != null)
        GuiRegistry.getInstance()
            .addGuiWidgetElement(BeamSplunkOutputMeta.class.getName(), widget, field);
    }
  }

  @Test
  void okPersistsConnectionAndEventOptionsThroughGroupedWidgets() {
    var meta = new BeamSplunkOutputMeta();
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.addTransform(new TransformMeta("BeamSplunkOutput", "splunk", meta));
    withDialog(
        parent -> open(parent, meta, pipeline),
        bot -> {
          SWTBot dialog = bot.shell("Beam Splunk output").activate().bot();
          dialog.text(1).setText("http://127.0.0.1:8088");
          dialog.text(2).setText("hec-secret");
          dialog.text(3).setText("1");
          display.syncExec(
              () ->
                  assertNotEquals(0, dialog.text(2).widget.getEchoChar(), "token must be masked"));
          dialog.cTabItem("Event").activate();
          dialog.text(1).setText("body");
          dialog.text(2).setText("main");
          display.syncExec(
              () -> {
                Shell shell = dialog.text(0).widget.getShell();
                shell.setSize(640, 420);
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
    assertEquals("http://127.0.0.1:8088", meta.getHecUrl());
    assertEquals("hec-secret", meta.getToken());
    assertEquals("1", meta.getBatchCount());
    assertEquals("body", meta.getEventField());
    assertEquals("main", meta.getIndex());
    assertTrue(meta.isEnableGzip());
    assertFalse(meta.isDisableCertificateValidation());
  }

  @Test
  void cancelDoesNotPersistEdits() {
    var meta = new BeamSplunkOutputMeta();
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.addTransform(new TransformMeta("BeamSplunkOutput", "splunk", meta));
    withDialog(
        parent -> open(parent, meta, pipeline),
        bot -> {
          SWTBot dialog = bot.shell("Beam Splunk output").activate().bot();
          dialog.text(1).setText("http://discarded");
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertNull(meta.getHecUrl());
    assertFalse(meta.hasChanged());
  }

  static void open(Shell shell, BeamSplunkOutputMeta meta, PipelineMeta pipeline) {
    try {
      ((BaseTransformDialog)
              BeamSplunkOutputDialog.class
                  .getConstructor(
                      Shell.class, IVariables.class, BeamSplunkOutputMeta.class, PipelineMeta.class)
                  .newInstance(shell, new Variables(), meta, pipeline))
          .open();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
