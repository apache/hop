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

package org.apache.hop.pipeline.transforms.mergeintotable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.function.Consumer;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The Merge into table dialog writes the transform only on OK. Get fields and Browse go through the
 * metadata, because the widgets are read back from it after a button: they must do that on a copy,
 * or Cancel keeps what they wrote.
 *
 * <p>Tagged {@code uitest} so it is skipped when there is no display; wrap Maven with {@code
 * tools/with-isolated-display.sh} so the dialog does not steal focus.
 */
@Tag("uitest")
class MergeIntoTableDialogTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "merge";
  private static final String DIALOG_TITLE = "Merge into table";
  private static final String CONNECTION_NAME = "test-db";

  /** The plugin scan of a test JVM doesn't register the annotated widgets: do it here. */
  @BeforeAll
  static void registerWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    String dataClass = MergeIntoTableMeta.class.getName();
    if (registry.findGuiElements(dataClass, MergeIntoTableMeta.GUI_PLUGIN_ELEMENT_PARENT_ID)
        != null) {
      return;
    }
    for (Field field : MergeIntoTableMeta.class.getDeclaredFields()) {
      GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
      if (widget != null) {
        registry.addGuiWidgetElement(dataClass, widget, field);
      }
    }
    for (Method method : MergeIntoTableMeta.class.getDeclaredMethods()) {
      GuiWidgetElement widget = method.getAnnotation(GuiWidgetElement.class);
      if (widget != null) {
        registry.addGuiWidgetElement(
            widget, method, dataClass, MergeIntoTableMeta.class.getClassLoader());
      }
    }
  }

  @Test
  void cancelAfterGetFieldsLeavesTheTransformAlone() throws Exception {
    MergeIntoTableMeta meta = meta();

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = bot.shell(DIALOG_TITLE).activate().bot();
          dialog.text("TARGET").setText("OTHER");
          dialog.text("2000").setText("50");
          // Get fields reads every widget into the metadata before it asks for the fields.
          dialog.button(buttonLabel("System.Button.GetFields")).click();
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals("TARGET", meta.getTableName(), "Cancel must not keep the edited table");
    assertEquals("2000", meta.getCommitSize(), "Cancel must not keep the edited commit size");
    assertEquals(1, meta.getKeyFields().size());
    assertFalse(meta.hasChanged(), "Cancel leaves the transform unchanged");
  }

  @Test
  void okAfterGetFieldsWritesTheEdits() throws Exception {
    MergeIntoTableMeta meta = meta();

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = bot.shell(DIALOG_TITLE).activate().bot();
          dialog.text("TARGET").setText("OTHER");
          dialog.button(buttonLabel("System.Button.GetFields")).click();
          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals("OTHER", meta.getTableName());
    assertEquals(1, meta.getKeyFields().size());
    assertEquals("ID", meta.getKeyFields().get(0).getTableColumn());
  }

  private static MergeIntoTableMeta meta() {
    MergeIntoTableMeta meta = new MergeIntoTableMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION_NAME);
    meta.setTableName("TARGET");
    meta.setCommitSize("2000");
    meta.getKeyFields().add(new MergeKeyField("id", "ID"));
    meta.getValueFields().add(new MergeValueField("name", "NAME", true));
    meta.setChanged(false);
    return meta;
  }

  private static Consumer<Shell> openerFor(MergeIntoTableMeta meta) throws HopException {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName(CONNECTION_NAME);
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);

    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(pluginId, "the transform must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setMetadataProvider(metadataProvider);
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return parent -> {
      MergeIntoTableDialog dialog =
          new MergeIntoTableDialog(parent, new Variables(), meta, pipelineMeta);
      dialog.setMetadataProvider(metadataProvider);
      dialog.open();
    };
  }
}
