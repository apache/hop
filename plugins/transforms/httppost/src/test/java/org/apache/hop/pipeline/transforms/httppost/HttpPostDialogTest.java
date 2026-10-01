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

package org.apache.hop.pipeline.transforms.httppost;

import static org.eclipse.swtbot.swt.finder.matchers.WidgetMatcherFactory.widgetOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.exceptions.WidgetNotFoundException;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotCCombo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * End-to-end SWTBot coverage for the HTTP Post transform's {@link HttpPostDialog}, focused on the
 * configurable Content type field added for issue #4769. The dialog runs its own blocking event
 * loop in {@code open()}, so {@link SwtBotTestBase#withDialog} pumps it on the UI thread while the
 * assertions drive it from a worker thread.
 *
 * <p>Tagged {@code uitest} so it is skipped when there is no display. The default reactor run still
 * includes it on a desktop; wrap Maven with {@code tools/with-isolated-display.sh} so the dialog
 * does not steal focus.
 */
@Tag("uitest")
class HttpPostDialogTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "httppost";
  private static final String SHELL_TITLE = "HTTP post";

  @Test
  void getDataShowsConfiguredContentType() {
    HttpPostMeta meta = new HttpPostMeta();
    meta.setDefault(); // contentType defaults to text/xml
    meta.setContentType("application/json");
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new HttpPostDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          assertEquals("application/json", contentTypeCombo(dialog).getText());
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  @Test
  void okWritesEditedContentTypeBackToMeta() {
    HttpPostMeta meta = new HttpPostMeta();
    meta.setDefault();
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new HttpPostDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          SWTBotCCombo contentType = contentTypeCombo(dialog);
          // the text/xml default round-trips into the dialog
          assertEquals("text/xml", contentType.getText());
          contentType.setText("application/json");
          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals("application/json", meta.getContentType());
  }

  @Test
  void cancelLeavesContentTypeUntouched() {
    HttpPostMeta meta = new HttpPostMeta();
    meta.setDefault();
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new HttpPostDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          contentTypeCombo(dialog).setText("application/json"); // edit then cancel
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(
        "text/xml", meta.getContentType(), "Cancel must not write the edited content type");
  }

  /**
   * Finds the content-type combo by content rather than a fixed widget index, so the test survives
   * layout reordering. It is the only {@code CCombo} in the dialog pre-populated with media types.
   */
  private static SWTBotCCombo contentTypeCombo(SWTBot dialog) {
    for (int i = 0; i < 16; i++) {
      try {
        SWTBotCCombo combo = dialog.ccomboBox(i);
        if (Arrays.asList(combo.items()).contains("application/json")) {
          return combo;
        }
      } catch (WidgetNotFoundException e) {
        break; // ran out of combos
      }
    }
    return fail("Content type combo not found in HttpPostDialog");
  }

  @Test
  void optionsAreSplitAcrossTabs() {
    HttpPostMeta meta = new HttpPostMeta();
    meta.setDefault();
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new HttpPostDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          List<String> tabs =
              List.of(
                  "General",
                  "Authentication",
                  "Proxy",
                  "SSL",
                  "Body parameters",
                  "Query parameters");
          assertEquals(tabs, tabTitles(dialog));
          // Selecting every tab proves each one has a control and laid out without error.
          tabs.forEach(tab -> activateTab(dialog, tab));
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  @Test
  void aSelectedConnectionDisablesTheFieldsItSupersedes() {
    HttpPostMeta meta = new HttpPostMeta();
    meta.setDefault();
    meta.setConnectionName("some-connection");
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new HttpPostDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          activateTab(dialog, "Proxy");
          assertFalse(
              dialog.textWithLabel("Proxy Host").isEnabled(),
              "a connection supplies the proxy, so the field must be disabled");
          activateTab(dialog, "SSL");
          assertFalse(
              dialog.checkBox().isEnabled(),
              "a connection supplies the TLS settings, so the checkbox must be disabled");
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  /**
   * The titles of the dialog's tabs. Read off the folder rather than through {@code
   * bot.cTabItem(...)}: SWTBot's widget finder only walks controls and never sees the tab items of
   * a CTabFolder.
   */
  private List<String> tabTitles(SWTBot dialog) {
    CTabFolder tabFolder = (CTabFolder) dialog.widget(widgetOfType(CTabFolder.class));
    AtomicReference<List<String>> titles = new AtomicReference<>();
    display.syncExec(
        () -> {
          List<String> found = new ArrayList<>();
          for (CTabItem item : tabFolder.getItems()) {
            // The look-and-feel pads tab labels with spaces, hence the trim().
            found.add(item.getText().trim());
          }
          titles.set(found);
        });
    return titles.get();
  }

  /** Brings one tab to the front, by title. */
  private void activateTab(SWTBot dialog, String title) {
    CTabFolder tabFolder = (CTabFolder) dialog.widget(widgetOfType(CTabFolder.class));
    display.syncExec(
        () -> {
          for (CTabItem item : tabFolder.getItems()) {
            if (title.equals(item.getText().trim())) {
              tabFolder.setSelection(item);
              tabFolder.layout();
              return;
            }
          }
          throw new AssertionError("no tab titled " + title);
        });
  }

  private static PipelineMeta pipelineWith(HttpPostMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(
        pluginId, "HTTP Post transform plugin must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
