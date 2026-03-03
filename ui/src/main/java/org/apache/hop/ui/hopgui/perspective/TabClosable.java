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

package org.apache.hop.ui.hopgui.perspective;

import java.util.ArrayList;
import java.util.List;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;

public interface TabClosable {

  /** Close the tab on the perspective */
  void closeTab(CTabFolderEvent event, CTabItem tabItem);

  /** Get all the tabs on the right-hand side of the selected one */
  default List<CTabItem> getTabsToRight(CTabItem selectedTabItem) {
    CTabFolder folder = selectedTabItem.getParent();
    List<CTabItem> items = new ArrayList<>();
    for (int i = folder.getItems().length - 1; i >= 0; i--) {
      if (selectedTabItem.equals(folder.getItems()[i])) {
        break;
      } else {
        items.add(folder.getItems()[i]);
      }
    }
    return items;
  }

  /** Get all the tabs on the left-hand side of the selected one */
  default List<CTabItem> getTabsToLeft(CTabItem selectedTabItem) {
    CTabFolder folder = selectedTabItem.getParent();
    List<CTabItem> items = new ArrayList<>();
    for (CTabItem item : folder.getItems()) {
      if (selectedTabItem.equals(item)) {
        break;
      } else {
        items.add(item);
      }
    }
    return items;
  }

  /** Get all the other tabs of the selected one */
  default List<CTabItem> getOtherTabs(CTabItem selectedTabItem) {
    CTabFolder folder = selectedTabItem.getParent();
    List<CTabItem> items = new ArrayList<>();
    for (CTabItem item : folder.getItems()) {
      if (!selectedTabItem.equals(item)) {
        items.add(item);
      }
    }
    return items;
  }

  /** Get the tabFolder of the perspective */
  CTabFolder getTabFolder();
}
