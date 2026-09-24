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

package org.apache.hop.pipeline.transforms.http;

import static org.eclipse.swtbot.swt.finder.matchers.WidgetMatcherFactory.widgetOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * SWTBot coverage for {@link HttpDialog}: the tab layout, and the fields a selected REST connection
 * takes over.
 *
 * <p>Tagged {@code uitest} so it is skipped when there is no display. The default reactor run still
 * includes it on a desktop; wrap Maven with {@code tools/with-isolated-display.sh} so the dialog
 * does not steal focus.
 */
@Tag("uitest")
class HttpDialogTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "http";
  private static final String SHELL_TITLE = "HTTP client";

  @Test
  void optionsAreSplitAcrossTabs() {
    HttpMeta meta = new HttpMeta();
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new HttpDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          List<String> tabs =
              List.of("General", "Authentication", "Proxy", "SSL", "Parameters", "Headers");
          assertEquals(tabs, tabTitles(dialog));
          // Selecting every tab proves each one has a control and laid out without error.
          tabs.forEach(tab -> activateTab(dialog, tab));
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  @Test
  void proxyCredentialsRoundTripThroughTheDialog() {
    HttpMeta meta = new HttpMeta();
    meta.setProxyHost("proxy.example.com");
    meta.setProxyPort("3128");
    meta.setProxyUsername("proxyuser");
    meta.setNonProxyHosts("localhost|127.*");
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new HttpDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          activateTab(dialog, "Proxy");
          assertEquals("proxy.example.com", dialog.textWithLabel("Proxy Host").getText());
          assertEquals("3128", dialog.textWithLabel("Proxy Port").getText());
          assertEquals("proxyuser", dialog.textWithLabel("Proxy username").getText());
          assertEquals("localhost|127.*", dialog.textWithLabel("Ignore proxy for hosts").getText());

          dialog.textWithLabel("Proxy username").setText("someone-else");
          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals("someone-else", meta.getProxyUsername());
  }

  @Test
  void aSelectedConnectionDisablesTheFieldsItSupersedes() {
    HttpMeta meta = new HttpMeta();
    meta.setConnectionName("some-connection");
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new HttpDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          activateTab(dialog, "Proxy");
          assertFalse(
              dialog.textWithLabel("Proxy Host").isEnabled(),
              "a connection supplies the proxy, so the field must be disabled");
          activateTab(dialog, "Authentication");
          assertFalse(
              dialog.textWithLabel("Http Login").isEnabled(),
              "a connection supplies the credentials, so the field must be disabled");
          activateTab(dialog, "SSL");
          assertFalse(
              dialog.checkBox().isEnabled(),
              "a connection supplies the TLS settings, so the checkbox must be disabled");
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  @Test
  void withoutAConnectionTheOwnFieldsStayEditable() {
    HttpMeta meta = new HttpMeta();
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new HttpDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          activateTab(dialog, "Proxy");
          assertTrue(dialog.textWithLabel("Proxy Host").isEnabled());
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

  private static PipelineMeta pipelineWith(HttpMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(pluginId, "HTTP transform plugin must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
