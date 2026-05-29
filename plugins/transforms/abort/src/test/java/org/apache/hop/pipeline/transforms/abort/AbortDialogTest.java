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

package org.apache.hop.pipeline.transforms.abort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
 * End-to-end SWTBot coverage for the Abort transform's {@link AbortDialog}, kept next to the
 * transform it exercises. The dialog runs its own blocking event loop in {@code open()}, so {@link
 * SwtBotTestBase#withDialog} pumps it on the UI thread while the assertions drive it from a worker
 * thread.
 *
 * <p>Tagged {@code uitest} so it is excluded from the normal build (it needs a display); run with
 * {@code mvn -pl plugins/transforms/abort -Puitest test}.
 */
@Tag("uitest")
class AbortDialogTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "abort";
  private static final String SAFE_STOP_LABEL = "Stop input processing";
  private static final String ALWAYS_LOG_LABEL = "Always log rows";

  @Test
  void okWritesEditedOptionsBackToMeta() {
    AbortMeta meta = new AbortMeta(); // defaults to AbortOption.ABORT, alwaysLogRows = false
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new AbortDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell("Abort").activate().bot();

          // text(0)=transform name, text(1)=abort threshold, text(2)=abort message
          // (creation order in the dialog). Guard the indexing assumption up front.
          assertEquals(TRANSFORM_NAME, dialog.text(0).getText(), "transform name field");

          dialog.radio(SAFE_STOP_LABEL).click();
          dialog.text(1).setText("500");
          dialog.text(2).setText("Too many rows");
          dialog.checkBox(ALWAYS_LOG_LABEL).click(); // false -> true

          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertTrue(meta.isSafeStop(), "Safe-stop radio should map to AbortOption.SAFE_STOP");
    assertEquals("500", meta.getRowThreshold());
    assertEquals("Too many rows", meta.getMessage());
    assertTrue(meta.isAlwaysLogRows(), "checking the box should enable always-log-rows");
  }

  @Test
  void cancelLeavesMetaUntouched() {
    AbortMeta meta = new AbortMeta();
    PipelineMeta pipelineMeta = pipelineWith(meta);

    withDialog(
        parent -> new AbortDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialog = bot.shell("Abort").activate().bot();
          dialog.text(1).setText("999"); // edit then cancel - must not be persisted
          dialog.radio(SAFE_STOP_LABEL).click();
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertNull(meta.getRowThreshold(), "Cancel must not write the edited threshold");
    assertTrue(meta.isAbort(), "Cancel must keep the original AbortOption.ABORT");
  }

  private static PipelineMeta pipelineWith(AbortMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(pluginId, "Abort transform plugin must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
