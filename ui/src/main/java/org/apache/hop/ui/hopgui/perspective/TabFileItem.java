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

import org.eclipse.swt.custom.CTabItem;

import java.util.Objects;
import java.util.UUID;

public class TabFileItem {
  private String uuid;
  private String filename;
  private CTabItem tabItem;

  public TabFileItem() {
    uuid = UUID.randomUUID().toString();
  }

  public TabFileItem( String filename, CTabItem tabItem ) {
    this();
    this.filename = filename;
    this.tabItem = tabItem;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    TabFileItem that = (TabFileItem) o;
    return uuid.equals( that.uuid );
  }

  @Override public int hashCode() {
    return Objects.hash( uuid );
  }

  /**
   * Gets uuid
   *
   * @return value of uuid
   */
  public String getUuid() {
    return uuid;
  }

  /**
   * @param uuid The uuid to set
   */
  public void setUuid( String uuid ) {
    this.uuid = uuid;
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename The filename to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
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
}
