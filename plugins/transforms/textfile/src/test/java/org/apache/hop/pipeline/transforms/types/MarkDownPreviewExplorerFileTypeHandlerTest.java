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
 *
 */

package org.apache.hop.pipeline.transforms.types;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MarkDownPreviewExplorerFileTypeHandlerTest {

  private ExplorerPerspective perspective;
  private MarkDownPreviewExplorerFileTypeHandler handler;

  @BeforeEach
  void setUp() {
    perspective = mock(ExplorerPerspective.class);
    handler =
        new MarkDownPreviewExplorerFileTypeHandler(
            null,
            perspective,
            new ExplorerFile("notes.md (preview)", null, new MarkDownPreviewExplorerFileType()));
  }

  @Test
  void testNotOpenBeforeTheTabIsAdded() {
    when(perspective.getItems()).thenReturn(List.of());

    assertFalse(handler.isOpen());
  }

  @Test
  void testOpenWhileThePerspectiveHasATabForIt() {
    when(perspective.getItems()).thenReturn(List.of(new TabItemHandler(null, handler)));

    assertTrue(handler.isOpen());
  }

  @Test
  void testClosedOnceThePerspectiveDropsTheTab() {
    // Closing a tab disposes the CTabItem but leaves the browser widget alive, so a widget check
    // would keep reporting the preview as open and previewing again would do nothing.
    when(perspective.getItems()).thenReturn(List.of(new TabItemHandler(null, handler)));
    assertTrue(handler.isOpen());

    when(perspective.getItems()).thenReturn(List.of());

    assertFalse(handler.isOpen());
  }

  @Test
  void testTheSourceFileTabIsTrackedTheSameWay() {
    // The markdown tab uses the same rule to tell "my tab was closed" (dropped from the list, which
    // happens just before the tab item is disposed) from "my tab was moved to another pane" (still
    // listed, tab item disposed and replaced).
    MarkDownExplorerFileTypeHandler markdownHandler =
        new MarkDownExplorerFileTypeHandler(
            null, perspective, new ExplorerFile("notes.md", null, new MarkDownExplorerFileType()));

    when(perspective.getItems()).thenReturn(List.of(new TabItemHandler(null, markdownHandler)));
    assertTrue(markdownHandler.hasOpenTab());

    when(perspective.getItems()).thenReturn(List.of());
    assertFalse(markdownHandler.hasOpenTab());
  }

  @Test
  void testOtherTabsDoNotCountAsThePreview() {
    MarkDownPreviewExplorerFileTypeHandler otherPreview =
        new MarkDownPreviewExplorerFileTypeHandler(
            null,
            perspective,
            new ExplorerFile("other.md (preview)", null, new MarkDownPreviewExplorerFileType()));
    when(perspective.getItems()).thenReturn(List.of(new TabItemHandler(null, otherPreview)));

    assertFalse(handler.isOpen());
  }
}
