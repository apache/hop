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

package org.apache.hop.ui.hopgui.perspective.explorer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.hop.core.search.SearchMatcher;
import org.apache.hop.core.util.Utils;

/**
 * In-memory project file tree used by the explorer filter. Built once from VFS (or a full walk) so
 * subsequent filter keystrokes match and render without re-reading the filesystem.
 */
public class ExplorerTreeModel {

  @Getter private Node root;
  @Getter private boolean fullyLoaded;

  public void clear() {
    root = null;
    fullyLoaded = false;
  }

  public boolean isEmpty() {
    return root == null;
  }

  /**
   * Whether this model can serve filter queries for the given project root without another VFS
   * walk.
   */
  public boolean isUsableFor(String rootPath) {
    return fullyLoaded
        && root != null
        && rootPath != null
        && Objects.equals(root.getPath(), rootPath);
  }

  public void setRoot(Node root, boolean fullyLoaded) {
    this.root = root;
    this.fullyLoaded = fullyLoaded && root != null;
  }

  /**
   * A node is visible under a filter when its name matches or any descendant is visible. With no
   * filter every node is visible.
   */
  public static boolean isVisible(Node node, SearchMatcher matcher, boolean filtering) {
    if (node == null) {
      return false;
    }
    if (!filtering || matcher == null) {
      return true;
    }
    if (matcher.matches(node.getName())) {
      return true;
    }
    if (!node.isFolder()) {
      return false;
    }
    for (Node child : node.getChildren()) {
      if (isVisible(child, matcher, true)) {
        return true;
      }
    }
    return false;
  }

  /** True when the filter string is active (non-empty after the UI threshold is applied). */
  public static boolean isFiltering(String filterText) {
    return !Utils.isEmpty(filterText);
  }

  /** One file or folder in the project tree. */
  @Getter
  public static final class Node {
    private final String path;
    private final String name;
    private final boolean folder;

    @Getter(AccessLevel.NONE)
    private final List<Node> children = new ArrayList<>();

    public Node(String path, String name, boolean folder) {
      this.path = path;
      this.name = name;
      this.folder = folder;
    }

    public void addChild(Node child) {
      if (child != null) {
        children.add(child);
      }
    }

    public List<Node> getChildren() {
      return children;
    }
  }
}
