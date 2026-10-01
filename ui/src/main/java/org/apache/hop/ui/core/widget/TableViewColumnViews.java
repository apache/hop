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

import java.util.ArrayList;
import java.util.List;

/**
 * Matching helpers for named {@link TableView} column views. Kept free of SWT so the logic can be
 * unit tested.
 */
public final class TableViewColumnViews {
  private TableViewColumnViews() {}

  /**
   * Resolve view column names to indices into {@code availableNames}.
   *
   * <p>Matching is case-sensitive (Hop field names). Duplicate available names take the first
   * unused match. Names that are not present are skipped.
   *
   * @param availableNames column names of the current table, in creation order
   * @param viewNames column names in the desired display order
   * @return matching indices into {@code availableNames}, in view order
   */
  public static List<Integer> resolveColumnIndices(
      String[] availableNames, List<String> viewNames) {
    List<Integer> indices = new ArrayList<>();
    if (availableNames == null || viewNames == null) {
      return indices;
    }
    boolean[] used = new boolean[availableNames.length];
    for (String viewName : viewNames) {
      if (viewName == null) {
        continue;
      }
      for (int i = 0; i < availableNames.length; i++) {
        if (used[i]) {
          continue;
        }
        if (viewName.equals(availableNames[i])) {
          used[i] = true;
          indices.add(i);
          break;
        }
      }
    }
    return indices;
  }
}
