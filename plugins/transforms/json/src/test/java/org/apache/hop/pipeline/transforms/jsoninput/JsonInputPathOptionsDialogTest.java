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

package org.apache.hop.pipeline.transforms.jsoninput;

import static org.eclipse.swtbot.swt.finder.matchers.WidgetMatcherFactory.widgetOfType;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.utils.SWTBotPreferences;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("uitest")
class JsonInputPathOptionsDialogTest extends SwtBotTestBase {
  /**
   * JSON Input builds several tables before its event loop starts. SWTBot's 5 s default expires
   * while that construction is still on the UI thread, so the JSONPath tab is missed.
   */
  private static final long DIALOG_TIMEOUT_MS = 30_000L;

  private static long defaultTimeout;

  @BeforeAll
  static void slowDownSwtBot() {
    defaultTimeout = SWTBotPreferences.TIMEOUT;
    SWTBotPreferences.TIMEOUT = DIALOG_TIMEOUT_MS;
  }

  @AfterAll
  static void restoreSwtBotTimeout() {
    SWTBotPreferences.TIMEOUT = defaultTimeout;
  }

  @BeforeAll
  static void registerWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    for (Field field : JsonInputMeta.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(JsonInputMeta.class.getName(), element, field);
      }
    }
  }

  private PipelineMeta pipeline(JsonInputMeta meta) {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setMetadataProvider(new MemoryMetadataProvider());
    pipeline.addTransform(new TransformMeta("JsonInput", "json", meta));
    return pipeline;
  }

  @Test
  void okPersistsLiteralPathMode() {
    JsonInputMeta meta = new JsonInputMeta();
    PipelineMeta pipeline = pipeline(meta);
    withDialog(
        parent -> new JsonInputDialog(parent, new Variables(), meta, pipeline).open(),
        bot -> {
          SWTBot dialog =
              bot.shell(BaseMessages.getString(JsonInputMeta.class, "JsonInputDialog.DialogTitle"))
                  .activate()
                  .bot();
          activateTab(dialog, "JSONPath");
          setResolveJsonPaths(dialog, false);
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertFalse(meta.isResolveJsonPaths());
  }

  @Test
  void cancelDoesNotPersistLiteralPathMode() {
    JsonInputMeta meta = new JsonInputMeta();
    PipelineMeta pipeline = pipeline(meta);
    withDialog(
        parent -> new JsonInputDialog(parent, new Variables(), meta, pipeline).open(),
        bot -> {
          SWTBot dialog =
              bot.shell(BaseMessages.getString(JsonInputMeta.class, "JsonInputDialog.DialogTitle"))
                  .activate()
                  .bot();
          activateTab(dialog, "JSONPath");
          setResolveJsonPaths(dialog, false);
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertTrue(meta.isResolveJsonPaths());
  }

  /**
   * SWTBot's finder walks controls and does not see the items of a {@code CTabFolder}. Select the
   * tab on the folder, the same way the other transform dialog tests do.
   */
  private void activateTab(SWTBot dialog, String title) {
    CTabFolder tabFolder = (CTabFolder) dialog.widget(widgetOfType(CTabFolder.class));
    display.syncExec(
        () -> {
          List<String> titles = new ArrayList<>();
          for (CTabItem item : tabFolder.getItems()) {
            String itemTitle = item.getText() == null ? "" : item.getText().trim();
            titles.add(itemTitle);
            if (title.equals(itemTitle)) {
              tabFolder.setSelection(item);
              tabFolder.layout();
              return;
            }
          }
          throw new AssertionError("no tab titled " + title + " in " + titles);
        });
  }

  /**
   * The annotated checkbox keeps its text on a separate label. The button itself has no title, so a
   * {@code checkBox("Resolve variables in JSONPath")} lookup does not see it.
   */
  private void setResolveJsonPaths(SWTBot dialog, boolean selected) {
    Label label = dialog.label("Resolve variables in JSONPath").widget;
    display.syncExec(
        () -> {
          for (Control child : label.getParent().getChildren()) {
            if (child instanceof Button button && (button.getStyle() & SWT.CHECK) != 0) {
              button.setSelection(selected);
              return;
            }
          }
          throw new AssertionError("no checkbox next to Resolve variables in JSONPath");
        });
  }
}
