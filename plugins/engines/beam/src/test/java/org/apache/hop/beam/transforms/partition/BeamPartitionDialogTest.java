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

package org.apache.hop.beam.transforms.partition;

import static org.eclipse.swtbot.swt.finder.matchers.WidgetMatcherFactory.widgetOfType;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import org.apache.hop.beam.core.BeamDefaults;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("uitest")
class BeamPartitionDialogTest extends SwtBotTestBase {

  @BeforeAll
  static void registerWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.findGuiElements(
            BeamPartitionMeta.class.getName(), BeamPartitionMeta.GUI_PLUGIN_ELEMENT_PARENT_ID)
        != null) {
      return;
    }
    for (Field field : BeamPartitionMeta.class.getDeclaredFields()) {
      GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
      if (widget != null) {
        registry.addGuiWidgetElement(BeamPartitionMeta.class.getName(), widget, field);
      }
    }
  }

  @Test
  void okPersistsPartitionOptionsThroughGroupedWidgets() throws Exception {
    BeamPartitionMeta meta = new BeamPartitionMeta();
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.addTransform(new TransformMeta("BeamPartition", "partition", meta));

    withDialog(
        parent -> open(parent, meta, pipeline),
        bot -> {
          SWTBot dialog = bot.shell("Beam partition").activate().bot();
          display.syncExec(
              () -> {
                ComboVar combo = (ComboVar) dialog.widget(widgetOfType(ComboVar.class));
                assertArrayEquals(
                    new String[] {
                      BeamDefaults.PARTITION_TYPE_SINGLE, BeamDefaults.PARTITION_TYPE_KEY
                    },
                    combo.getItems());
                combo.setText(BeamDefaults.PARTITION_TYPE_KEY);
              });
          dialog.textWithLabel("Key field").setText("customerId");
          dialog.textWithLabel("Number of partitions").setText("4");

          display.syncExec(
              () -> {
                Shell shell = dialog.text(0).widget.getShell();
                shell.setSize(500, 300);
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

    assertEquals(BeamDefaults.PARTITION_TYPE_KEY, meta.getPartitionType());
    assertEquals("customerId", meta.getKeyField());
    assertEquals("4", meta.getNumPartitions());
  }

  @Test
  void cancelDoesNotPersistEdits() throws Exception {
    BeamPartitionMeta meta = new BeamPartitionMeta();
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.addTransform(new TransformMeta("BeamPartition", "partition", meta));

    withDialog(
        parent -> open(parent, meta, pipeline),
        bot -> {
          SWTBot dialog = bot.shell("Beam partition").activate().bot();
          dialog.textWithLabel("Key field").setText("discarded");
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals("", meta.getKeyField());
    assertFalse(meta.hasChanged());
  }

  private static void open(Shell shell, BeamPartitionMeta meta, PipelineMeta pipeline) {
    new BeamPartitionDialog(shell, new Variables(), meta, pipeline).open();
  }
}
