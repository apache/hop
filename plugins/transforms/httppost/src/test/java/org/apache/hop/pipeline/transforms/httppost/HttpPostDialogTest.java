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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
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
 * <p>Tagged {@code uitest} so it is excluded from the normal build (it needs a display); run with
 * {@code mvn -pl plugins/transforms/httppost -Puitest test}.
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

  private static PipelineMeta pipelineWith(HttpPostMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(
        pluginId, "HTTP Post transform plugin must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
