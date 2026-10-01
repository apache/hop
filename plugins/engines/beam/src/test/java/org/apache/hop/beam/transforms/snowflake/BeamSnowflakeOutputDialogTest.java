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

package org.apache.hop.beam.transforms.snowflake;

import static org.junit.jupiter.api.Assertions.*;

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
class BeamSnowflakeOutputDialogTest extends SwtBotTestBase {
  @BeforeAll
  static void registerWidgets() {
    for (Field field : BeamSnowflakeOutputMeta.class.getDeclaredFields()) {
      GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
      if (widget != null)
        GuiRegistry.getInstance()
            .addGuiWidgetElement(BeamSnowflakeOutputMeta.class.getName(), widget, field);
    }
  }

  @Test
  void okLeavesTheDefaultDispositionsWhenTheyAreNotChanged() {
    var meta = new BeamSnowflakeOutputMeta();
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.addTransform(new TransformMeta("BeamSnowflakeOutput", "snowflake", meta));
    withDialog(
        parent -> open(parent, meta, pipeline),
        bot -> {
          SWTBot dialog = bot.shell("Beam Snowflake output").activate().bot();
          dialog.text(1).setText("acct.snowflakecomputing.com");
          dialog.text(2).setText("hop");
          dialog.text(3).setText("secret-password");
          display.syncExec(
              () ->
                  assertNotEquals(
                      0, dialog.text(3).widget.getEchoChar(), "password must be masked"));
          dialog.cTabItem("Staging").activate();
          dialog.text(1).setText("gs://hop-stage/out/");
          dialog.text(2).setText("HOP_INT");
          dialog.cTabItem("Write").activate();
          dialog.text(1).setText("CUSTOMERS");
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals("CUSTOMERS", meta.getTableName());
    assertEquals("APPEND", meta.getWriteDisposition());
    assertEquals("CREATE_NEVER", meta.getCreateDisposition());
    assertEquals("secret-password", meta.getPassword());
  }

  @Test
  void cancelDoesNotPersistEdits() {
    var meta = new BeamSnowflakeOutputMeta();
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.addTransform(new TransformMeta("BeamSnowflakeOutput", "snowflake", meta));
    withDialog(
        parent -> open(parent, meta, pipeline),
        bot -> {
          SWTBot dialog = bot.shell("Beam Snowflake output").activate().bot();
          dialog.text(1).setText("discarded.example");
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertNull(meta.getServerName());
    assertEquals("APPEND", meta.getWriteDisposition());
    assertFalse(meta.hasChanged());
  }

  static void open(Shell shell, BeamSnowflakeOutputMeta meta, PipelineMeta pipeline) {
    try {
      ((BaseTransformDialog)
              BeamSnowflakeOutputDialog.class
                  .getConstructor(
                      Shell.class,
                      IVariables.class,
                      BeamSnowflakeOutputMeta.class,
                      PipelineMeta.class)
                  .newInstance(shell, new Variables(), meta, pipeline))
          .open();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
