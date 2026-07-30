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

package org.apache.hop.ui.core.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Restoring a remembered geometry with {@link WindowProperty#setShell(Shell)} has to leave the
 * widgets where their layout puts them.
 *
 * <p>This is the shape every Hop perspective with a toolbar over a tree uses - metadata, git and
 * execution information all build a {@link SashForm} pane holding a toolbar above a tree, and they
 * build it before the main shell is opened. {@code setShell} sets the shell's *outer* bounds, so
 * the window trim is only subtracted from the client area once the shell is on screen; without a
 * re-layout at that point every child of the pane stays shifted up by the trim height and the
 * toolbar is clipped against the top of the panel until the user resizes the window.
 */
@Tag("uitest")
class WindowPropertyLayoutTest extends SwtBotTestBase {

  private static final String SHELL_NAME = "WindowPropertyLayoutTest";
  private static final int REMEMBERED_WIDTH = 900;
  private static final int REMEMBERED_HEIGHT = 600;

  @Test
  void toolbarInASashPaneIsLaidOutAfterRestoringTheGeometry() {
    AtomicReference<ToolBar> toolBar = new AtomicReference<>();
    AtomicReference<Tree> tree = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setText(SHELL_NAME);
          buildPerspectiveLikeScene(shell, toolBar, tree);
          // What HopGui and every remembering dialog do just before open().
          new WindowProperty(SHELL_NAME, false, 100, 100, REMEMBERED_WIDTH, REMEMBERED_HEIGHT)
              .setShell(shell);
        },
        bot -> {
          // Deliberately no resize and no explicit layout: this is what the user sees on open.
          bot.sleep(400);

          org.eclipse.swt.graphics.Rectangle toolBarBounds = syncBounds(toolBar.get());
          org.eclipse.swt.graphics.Rectangle treeBounds = syncBounds(tree.get());

          assertEquals(
              0,
              toolBarBounds.y,
              "The toolbar should sit at the top of its panel, not above it: " + toolBarBounds);
          assertTrue(
              treeBounds.y >= toolBarBounds.y + toolBarBounds.height,
              "The tree should start below the toolbar, but tree="
                  + treeBounds
                  + " toolbar="
                  + toolBarBounds);
        });
  }

  /** Toolbar over a tree inside a SashForm pane: the layout the perspectives use. */
  private void buildPerspectiveLikeScene(
      Shell shell, AtomicReference<ToolBar> toolBarRef, AtomicReference<Tree> treeRef) {
    shell.setLayout(new FormLayout());

    SashForm sash = new SashForm(shell, SWT.HORIZONTAL);
    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment(0, 0);
    fdSash.top = new FormAttachment(0, 0);
    fdSash.right = new FormAttachment(100, 0);
    fdSash.bottom = new FormAttachment(100, 0);
    sash.setLayoutData(fdSash);

    Composite pane = new Composite(sash, SWT.BORDER);
    pane.setLayout(new FormLayout());

    ToolBar toolBar = new ToolBar(pane, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    for (int i = 0; i < 5; i++) {
      new ToolItem(toolBar, SWT.PUSH).setText("i" + i);
    }
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment(0, 0);
    fdToolBar.top = new FormAttachment(0, 0);
    fdToolBar.right = new FormAttachment(100, 0);
    toolBar.setLayoutData(fdToolBar);
    toolBar.pack();
    toolBarRef.set(toolBar);

    Tree tree = new Tree(pane, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
    FormData fdTree = new FormData();
    fdTree.left = new FormAttachment(0, 0);
    fdTree.top = new FormAttachment(toolBar, 0);
    fdTree.right = new FormAttachment(100, 0);
    fdTree.bottom = new FormAttachment(100, 0);
    tree.setLayoutData(fdTree);
    treeRef.set(tree);

    // The editor side of the perspective.
    new Composite(sash, SWT.NONE).setLayout(new FormLayout());
    sash.setWeights(25, 75);
  }

  private org.eclipse.swt.graphics.Rectangle syncBounds(org.eclipse.swt.widgets.Control control) {
    AtomicReference<org.eclipse.swt.graphics.Rectangle> bounds = new AtomicReference<>();
    control.getDisplay().syncExec(() -> bounds.set(control.getBounds()));
    return bounds.get();
  }
}
