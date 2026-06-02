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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
 * SWTBot coverage for {@link BeamBQInputDialog}. Same harness pattern as the Output dialog — {@link
 * SwtBotTestBase#withDialog} pumps the blocking open() on the UI thread while assertions drive
 * widgets from a worker. The Fields TableView is intentionally not driven here (it would make
 * assertions fragile against column reorderings); the project/dataset/table/query path is what
 * matters for round-tripping user input back into the meta.
 */
@Tag("uitest")
class BeamBQInputDialogTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "BigQuery Input";
  private static final String DIALOG_TITLE = "BigQuery Input";

  @Test
  void okWritesEditedFieldsBackToMeta() {
    BeamBQInputMeta meta = new BeamBQInputMeta();
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new BeamBQInputDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(DIALOG_TITLE).activate().bot();

          // text(0) = transform name; text(1..3) = project / dataset / table; text(4) = query.
          assertEquals(TRANSFORM_NAME, dialog.text(0).getText(), "transform name field");

          dialog.text(1).setText("my-project");
          dialog.text(2).setText("my_dataset");
          dialog.text(3).setText("my_table");
          dialog.text(4).setText("SELECT * FROM `my-project.my_dataset.my_table`");

          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals("my-project", meta.getProjectId());
    assertEquals("my_dataset", meta.getDatasetId());
    assertEquals("my_table", meta.getTableId());
    assertEquals("SELECT * FROM `my-project.my_dataset.my_table`", meta.getQuery());
  }

  @Test
  void cancelLeavesMetaUntouched() {
    BeamBQInputMeta meta = new BeamBQInputMeta();
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new BeamBQInputDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell(DIALOG_TITLE).activate().bot();
          dialog.text(1).setText("discarded-project");
          dialog.text(2).setText("discarded_dataset");
          dialog.text(3).setText("discarded_table");
          dialog.text(4).setText("DROP TABLE discarded");
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertNull(meta.getProjectId(), "Cancel must not persist project id");
    assertNull(meta.getDatasetId(), "Cancel must not persist dataset id");
    assertNull(meta.getTableId(), "Cancel must not persist table id");
    assertNull(meta.getQuery(), "Cancel must not persist query");
  }

  private static PipelineMeta pipelineWith(BeamBQInputMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(
        pluginId, "BigQuery Input transform plugin must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
