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

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Folder tree items show the closed folder while collapsed and the open one while expanded. */
@Tag("uitest")
class FolderTreeIconsTest extends SwtBotTestBase {

  private static final String TREE_NAME = "FolderTreeIconsTest";

  private static Image closed() {
    return GuiResource.getInstance().getImageFolder();
  }

  private static Image open() {
    return GuiResource.getInstance().getImageFolderOpen();
  }

  /** root(folder) > child(folder) > leaf(file); plus a sibling category with a non-folder image. */
  private static Tree buildTree(org.eclipse.swt.widgets.Shell shell) {
    shell.setLayout(new FillLayout());
    Tree tree = new Tree(shell, SWT.SINGLE);
    TreeItem root = new TreeItem(tree, SWT.NONE);
    root.setText("root");
    root.setImage(closed());
    TreeItem child = new TreeItem(root, SWT.NONE);
    child.setText("child");
    child.setImage(closed());
    TreeItem leaf = new TreeItem(child, SWT.NONE);
    leaf.setText("leaf");
    leaf.setImage(GuiResource.getInstance().getImageFile());
    TreeItem other = new TreeItem(tree, SWT.NONE);
    other.setText("other");
    other.setImage(GuiResource.getInstance().getImageFile());
    new TreeItem(other, SWT.NONE).setText("other-child");
    FolderTreeIcons.install(tree);
    return tree;
  }

  private static void fire(Tree tree, int type, TreeItem item) {
    Event event = new Event();
    event.item = item;
    tree.notifyListeners(type, event);
  }

  @Test
  void userExpandAndCollapseSwapTheFolderIcon() {
    AtomicReference<Tree> treeRef = new AtomicReference<>();
    withScene(
        shell -> treeRef.set(buildTree(shell)),
        bot ->
            display.syncExec(
                () -> {
                  Tree tree = treeRef.get();
                  TreeItem root = tree.getItem(0);
                  TreeItem other = tree.getItem(1);

                  fire(tree, SWT.Expand, root);
                  assertSame(open(), root.getImage(), "expanded folder shows the open icon");
                  assertSame(closed(), root.getItem(0).getImage(), "children are not touched");

                  fire(tree, SWT.Collapse, root);
                  assertSame(closed(), root.getImage(), "collapsed folder shows the closed icon");

                  Image otherImage = other.getImage();
                  fire(tree, SWT.Expand, other);
                  assertSame(otherImage, other.getImage(), "non-folder images are left alone");
                }));
  }

  @Test
  void programmaticSetExpandedUpdatesStateAndIcon() {
    AtomicReference<Tree> treeRef = new AtomicReference<>();
    withScene(
        shell -> treeRef.set(buildTree(shell)),
        bot ->
            display.syncExec(
                () -> {
                  TreeItem root = treeRef.get().getItem(0);
                  TreeItem child = root.getItem(0);

                  FolderTreeIcons.setExpanded(root, true);
                  assertTrue(root.getExpanded());
                  assertSame(open(), root.getImage());

                  FolderTreeIcons.setExpanded(child, true);
                  assertSame(open(), child.getImage());

                  FolderTreeIcons.setExpanded(root, false);
                  assertFalse(root.getExpanded());
                  assertSame(closed(), root.getImage());
                  assertSame(open(), child.getImage(), "collapsing the parent leaves the child");
                }));
  }

  @Test
  void expandingAnEmptyFolderKeepsTheClosedIcon() {
    AtomicReference<Tree> treeRef = new AtomicReference<>();
    withScene(
        shell -> treeRef.set(buildTree(shell)),
        bot ->
            display.syncExec(
                () -> {
                  TreeItem empty = new TreeItem(treeRef.get(), SWT.NONE);
                  empty.setText("empty");
                  empty.setImage(closed());

                  FolderTreeIcons.setExpanded(empty, true);

                  assertFalse(empty.getExpanded(), "SWT ignores expanding a childless item");
                  assertSame(closed(), empty.getImage(), "so the icon must not pretend otherwise");
                }));
  }

  @Test
  void treeMemoryRestoreShowsTheOpenIcon() {
    AtomicReference<Tree> treeRef = new AtomicReference<>();
    withScene(
        shell -> treeRef.set(buildTree(shell)),
        bot ->
            display.syncExec(
                () -> {
                  Tree tree = treeRef.get();
                  TreeItem root = tree.getItem(0);
                  TreeMemory.getInstance().storeExpanded(TREE_NAME, root, true);

                  TreeMemory.setExpandedFromMemory(tree, TREE_NAME);

                  assertTrue(root.getExpanded());
                  assertSame(open(), root.getImage(), "memory-restored folders open their icon");
                  assertSame(closed(), root.getItem(0).getImage());
                }));
  }
}
