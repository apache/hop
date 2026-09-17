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

import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

/**
 * Shows a closed folder on collapsed tree items and an open folder on expanded ones.
 *
 * <p>Items keep using {@link GuiResource#getImageFolder()} as their image; this class only swaps it
 * with {@link GuiResource#getImageFolderOpen()} and back. Items carrying any other image are left
 * alone. {@link #install(Tree)} covers user expand/collapse. SWT fires no event for a programmatic
 * {@link TreeItem#setExpanded(boolean)}, so expand through {@link #setExpanded(TreeItem, boolean)}.
 */
public final class FolderTreeIcons {

  private FolderTreeIcons() {}

  /** Swaps the folder icon of the item the user expands or collapses. */
  public static void install(Tree tree) {
    tree.addListener(SWT.Expand, e -> setOpen((TreeItem) e.item, true));
    tree.addListener(SWT.Collapse, e -> setOpen((TreeItem) e.item, false));
  }

  /**
   * {@link TreeItem#setExpanded(boolean)} that also updates the folder icon. The icon follows the
   * state the item ends up in: expanding an item without children is a no-op on every platform.
   */
  public static void setExpanded(TreeItem item, boolean expanded) {
    if (item == null || item.isDisposed()) {
      return;
    }
    item.setExpanded(expanded);
    setOpen(item, item.getExpanded());
  }

  private static void setOpen(TreeItem item, boolean open) {
    if (item == null || item.isDisposed()) {
      return;
    }
    GuiResource guiResource = GuiResource.getInstance();
    Image closedImage = guiResource.getImageFolder();
    Image openImage = guiResource.getImageFolderOpen();
    Image current = item.getImage();
    if (open && current == closedImage) {
      item.setImage(openImage);
    } else if (!open && current == openImage) {
      item.setImage(closedImage);
    }
  }
}
