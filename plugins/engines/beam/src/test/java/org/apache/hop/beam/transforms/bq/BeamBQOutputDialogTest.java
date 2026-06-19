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

package org.apache.hop.beam.transforms.bq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * SWTBot coverage for {@link BeamBQOutputDialog}. The dialog runs its own blocking event loop in
 * {@code open()}, so {@link SwtBotTestBase#withDialog} pumps it on the UI thread while assertions
 * drive it from a worker. Tagged {@code uitest} so it is excluded from the normal build (needs a
 * display); run with {@code mvn -pl plugins/engines/beam -Puitest test}.
 */
@Tag("uitest")
class BeamBQOutputDialogTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "BigQuery Output";
  private static final String DIALOG_TITLE = "BigQuery Output";
  // The dialog renders the three flag rows as separate Label + Button(CHECK) pairs — the buttons
  // themselves carry no text. Address them by creation index instead of label.
  private static final int CHECK_CREATE = 0;
  private static final int CHECK_TRUNCATE = 1;
  private static final int CHECK_FAIL = 2;

  @Test
  void okWritesEditedFieldsBackToMeta() {
    BeamBQOutputMeta meta = new BeamBQOutputMeta();
    meta.setDefault(); // creatingIfNeeded = true by default
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new BeamBQOutputDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(DIALOG_TITLE).activate().bot();

          // text(0) = transform name (BaseDialog convention)
          // text(1) = project id, text(2) = dataset id, text(3) = table id
          assertEquals(TRANSFORM_NAME, dialog.text(0).getText(), "transform name field");

          dialog.text(1).setText("my-project");
          dialog.text(2).setText("my_dataset");
          dialog.text(3).setText("my_table");

          // Toggle every checkbox so we can observe all three flags change.
          dialog.checkBox(CHECK_CREATE).click(); // true -> false
          dialog.checkBox(CHECK_TRUNCATE).click(); // false -> true
          dialog.checkBox(CHECK_FAIL).click(); // false -> true

          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals("my-project", meta.getProjectId());
    assertEquals("my_dataset", meta.getDatasetId());
    assertEquals("my_table", meta.getTableId());
    assertFalse(meta.isCreatingIfNeeded(), "unchecking 'create if needed' must flip the flag");
    assertTrue(meta.isTruncatingTable(), "checking 'truncate' must flip the flag");
    assertTrue(meta.isFailingIfNotEmpty(), "checking 'fail if not empty' must flip the flag");
  }

  @Test
  void cancelLeavesMetaUntouched() {
    BeamBQOutputMeta meta = new BeamBQOutputMeta();
    meta.setDefault();
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new BeamBQOutputDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(DIALOG_TITLE).activate().bot();
          // Edit-then-cancel: nothing must persist.
          dialog.text(1).setText("discarded-project");
          dialog.text(2).setText("discarded_dataset");
          dialog.text(3).setText("discarded_table");
          dialog.checkBox(CHECK_CREATE).click();
          dialog.checkBox(CHECK_TRUNCATE).click();
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(null, meta.getProjectId(), "Cancel must not persist project id");
    assertEquals(null, meta.getDatasetId(), "Cancel must not persist dataset id");
    assertEquals(null, meta.getTableId(), "Cancel must not persist table id");
    assertTrue(meta.isCreatingIfNeeded(), "Cancel must keep the setDefault() value true");
    assertFalse(meta.isTruncatingTable(), "Cancel must keep the setDefault() value false");
  }

  private static PipelineMeta pipelineWith(BeamBQOutputMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(
        pluginId, "BigQuery Output transform plugin must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
