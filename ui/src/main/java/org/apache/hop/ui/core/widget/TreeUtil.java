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

import org.apache.hop.ui.core.PropsUi;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;

public class TreeUtil {
  public static final void setOptimalWidthOnColumns(final Tree tree) {
    if (tree.isDisposed()) return;
    // Compute size in UI Thread to avoid NPE
    tree.getDisplay()
        .asyncExec(
            () -> {
              if (tree.isDisposed()) return;
              tree.setRedraw(false);
              for (TreeColumn column : tree.getColumns()) {
                if (column.isDisposed()) break;
                column.pack();
                column.setWidth(
                    column.getWidth() + (int) (40 * PropsUi.getInstance().getZoomFactor()));
              }
              tree.setRedraw(true);
            });
  }

  public static final TreeItem findTreeItem(Tree tree, String[] path) {
    TreeItem[] items = tree.getItems();
    for (int i = 0; i < items.length; i++) {
      TreeItem treeItem = findTreeItem(items[i], path, 0);
      if (treeItem != null) {
        return treeItem;
      }
    }
    return null;
  }

  private static final TreeItem findTreeItem(TreeItem treeItem, String[] path, int level) {
    if (treeItem.getText().equals(path[level])) {
      if (level == path.length - 1) {
        return treeItem;
      }

      TreeItem[] items = treeItem.getItems();
      for (int i = 0; i < items.length; i++) {
        TreeItem found = findTreeItem(items[i], path, level + 1);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }
}
