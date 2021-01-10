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

package org.apache.hop.ui.hopgui.perspective;

import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.eclipse.swt.custom.CTabItem;

import java.util.Objects;

public class TabItemHandler {
  private CTabItem tabItem;
  private IHopFileTypeHandler typeHandler;

  public TabItemHandler() {
  }

  public TabItemHandler( CTabItem tabItem, IHopFileTypeHandler typeHandler ) {
    this.tabItem = tabItem;
    this.typeHandler = typeHandler;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    TabItemHandler that = (TabItemHandler) o;
    return tabItem.equals( that.tabItem );
  }

  @Override public int hashCode() {
    return Objects.hash( tabItem );
  }

  /**
   * Gets tabItem
   *
   * @return value of tabItem
   */
  public CTabItem getTabItem() {
    return tabItem;
  }

  /**
   * @param tabItem The tabItem to set
   */
  public void setTabItem( CTabItem tabItem ) {
    this.tabItem = tabItem;
  }

  /**
   * Gets typeHandler
   *
   * @return value of typeHandler
   */
  public IHopFileTypeHandler getTypeHandler() {
    return typeHandler;
  }

  /**
   * @param typeHandler The typeHandler to set
   */
  public void setTypeHandler( IHopFileTypeHandler typeHandler ) {
    this.typeHandler = typeHandler;
  }
}
