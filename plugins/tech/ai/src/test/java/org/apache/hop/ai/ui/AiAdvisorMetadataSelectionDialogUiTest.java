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

package org.apache.hop.ai.ui;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Tree;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Select metadata to include: the checkbox tree. */
@Tag("uitest")
class AiAdvisorMetadataSelectionDialogUiTest extends SwtBotTestBase {

  @BeforeAll
  static void init() throws Exception {
    HopGuiEnvironment.init();
  }

  @Test
  void theCheckboxesHaveRoomFromTheBorder() throws Exception {
    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    AiProvider ollama = new AiProvider();
    ollama.setName("ollama");
    metadata.getSerializer(AiProvider.class).save(ollama);
    AtomicReference<Shell> dialog = new AtomicReference<>();
    AtomicReference<AiAdvisorMetadataSelectionDialog> selection = new AtomicReference<>();
    withScene(
        shell -> {
          selection.set(new AiAdvisorMetadataSelectionDialog(shell, metadata, List.of()));
          dialog.set(selection.get().createShell());
          dialog.get().open();
        },
        bot -> {
          Tree tree = onUi(() -> selection.get().getTree());
          Rectangle treeBounds = onUi(tree::getBounds);
          // The tree sits inside a bordered frame, a margin from its left edge.
          assertTrue(treeBounds.x > 0, "the tree starts " + treeBounds.x + "px into its frame");
          assertTrue(onUi(() -> tree.getItemCount()) > 0, "the AI Provider type is listed");
          onUi(
              () -> {
                dialog.get().dispose();
                return null;
              });
        });
  }

  private static <T> T onUi(Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    Display.getDefault().syncExec(() -> result.set(supplier.get()));
    return result.get();
  }
}
