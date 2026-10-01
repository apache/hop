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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.lang.reflect.Field;
import java.util.List;
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
class BeamSnowflakeInputDialogTest extends SwtBotTestBase {
  @BeforeAll
  static void registerWidgets() {
    for (Field field : BeamSnowflakeInputMeta.class.getDeclaredFields()) {
      GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
      if (widget != null)
        GuiRegistry.getInstance()
            .addGuiWidgetElement(BeamSnowflakeInputMeta.class.getName(), widget, field);
    }
  }

  @Test
  void okKeepsTheFieldTableAndMasksThePassword() {
    var meta = new BeamSnowflakeInputMeta();
    meta.setFields(List.of(new SnowflakeField("id", "Integer")));
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.addTransform(new TransformMeta("BeamSnowflakeInput", "snowflake", meta));
    withDialog(
        parent -> open(parent, meta, pipeline),
        bot -> {
          SWTBot dialog = bot.shell("Beam Snowflake input").activate().bot();
          dialog.text(1).setText("acct.snowflakecomputing.com");
          dialog.text(2).setText("hop");
          dialog.text(3).setText("secret-password");
          display.syncExec(
              () ->
                  assertNotEquals(
                      0, dialog.text(3).widget.getEchoChar(), "password must be masked"));
          dialog.cTabItem("Staging").activate();
          dialog.text(1).setText("gs://hop-stage/in/");
          dialog.text(2).setText("HOP_INT");
          dialog.cTabItem("Read").activate();
          dialog.text(1).setText("CUSTOMERS");
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals("acct.snowflakecomputing.com", meta.getServerName());
    assertEquals("hop", meta.getUsername());
    assertEquals("secret-password", meta.getPassword());
    assertEquals("gs://hop-stage/in/", meta.getStagingBucket());
    assertEquals("HOP_INT", meta.getStorageIntegration());
    assertEquals("CUSTOMERS", meta.getTableName());
    assertEquals(1, meta.getFields().size());
    assertEquals("id", meta.getFields().get(0).getName());
    assertEquals("Integer", meta.getFields().get(0).getType());
  }

  @Test
  void cancelRestoresTheServerAndTheChangedFlag() {
    var meta = new BeamSnowflakeInputMeta();
    meta.setServerName("acct.snowflakecomputing.com");
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.addTransform(new TransformMeta("BeamSnowflakeInput", "snowflake", meta));
    withDialog(
        parent -> open(parent, meta, pipeline),
        bot -> {
          SWTBot dialog = bot.shell("Beam Snowflake input").activate().bot();
          dialog.text(1).setText("discarded.example");
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertEquals("acct.snowflakecomputing.com", meta.getServerName());
    assertFalse(meta.hasChanged());
  }

  static void open(Shell shell, BeamSnowflakeInputMeta meta, PipelineMeta pipeline) {
    try {
      ((BaseTransformDialog)
              BeamSnowflakeInputDialog.class
                  .getConstructor(
                      Shell.class,
                      IVariables.class,
                      BeamSnowflakeInputMeta.class,
                      PipelineMeta.class)
                  .newInstance(shell, new Variables(), meta, pipeline))
          .open();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
