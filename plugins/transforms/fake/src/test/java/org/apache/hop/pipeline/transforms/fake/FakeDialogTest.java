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

package org.apache.hop.pipeline.transforms.fake;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * End-to-end SWTBot coverage for the Fake transform's {@link FakeDialog}: the locale combo and the
 * generator grid must round-trip an existing {@link FakeMeta}, OK must write edits back and Cancel
 * must leave the meta untouched. The dialog runs its own blocking event loop in {@code open()}, so
 * {@link SwtBotTestBase#withDialog} pumps it on the UI thread while the assertions drive it from a
 * worker thread.
 *
 * <p>Tagged {@code uitest} so it is only run with a display; run with {@code mvn -pl
 * plugins/transforms/fake -Puitest test}.
 */
@Tag("uitest")
class FakeDialogTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "fake";
  private static final String SHELL_TITLE = "Fake data";

  @Test
  void okWritesEditedLocaleBackAndKeepsConfiguredFields() {
    FakeMeta meta = new FakeMeta();
    meta.setDefault(); // locale defaults to "en"
    meta.getFields().add(new FakeField("full_name", "name", "fullName"));
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new FakeDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          assertEquals(TRANSFORM_NAME, dialog.text(0).getText(), "transform name field");
          // The preconfigured locale round-trips into the only combo in the dialog.
          assertEquals("en", dialog.ccomboBox(0).getText());

          dialog.ccomboBox(0).setText("fr");
          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals("fr", meta.getLocale(), "OK must persist the edited locale");
    assertEquals(1, meta.getFields().size(), "the configured generator row must survive OK");
    FakeField field = meta.getFields().get(0);
    assertEquals("full_name", field.getName());
    assertEquals("name", field.getType());
    assertEquals("fullName", field.getTopic());
  }

  @Test
  void cancelLeavesMetaUntouched() {
    FakeMeta meta = new FakeMeta();
    meta.setDefault();
    meta.getFields().add(new FakeField("full_name", "name", "fullName"));
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new FakeDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          dialog.ccomboBox(0).setText("de"); // edit then cancel - must not persist
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals("en", meta.getLocale(), "Cancel must not write the edited locale");
    assertEquals(1, meta.getFields().size());
    assertEquals("full_name", meta.getFields().get(0).getName());
  }

  private static PipelineMeta pipelineWith(FakeMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(pluginId, "Fake transform plugin must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
