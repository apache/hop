/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.hopui;

import org.apache.xul.swt.tab.TabItem;

public class TabMapEntry {
  public enum ObjectType {
    TRANSFORMATION_GRAPH, JOB_GRAPH, SLAVE_SERVER, BROWSER, PLUGIN,
  }

  private TabItem tabItem;

  private String filename;

  private String objectName;

  private TabItemInterface object;

  private ObjectType objectType;

  private boolean showingLocation;

  /**
   * @param tabItem
   * @param filename
   * @param objectName
   * @param object
   * @param objectType
   */
  public TabMapEntry( TabItem tabItem, String filename, String objectName, TabItemInterface object, ObjectType objectType ) {
    this.tabItem = tabItem;
    this.filename = filename;
    this.objectName = objectName;
    this.object = object;
    this.objectType = objectType;
  }

  public boolean equals( Object obj ) {
    TabMapEntry entry = (TabMapEntry) obj;

    boolean sameType = objectType.equals( entry.objectType );
    boolean sameName = objectName != null && entry.objectName != null && objectName.equals( entry.objectName );
    boolean sameFile =
      ( filename == null && entry.filename == null ) || ( filename != null && filename.equals( entry.filename ) );

    return sameType && sameName && sameFile;
  }

  /**
   * @return the objectName
   */
  public String getObjectName() {
    return objectName;
  }

  /**
   * @param objectName the objectName to set
   */
  public void setObjectName( String objectName ) {
    this.objectName = objectName;
  }

  /**
   * @return the object
   */
  public TabItemInterface getObject() {
    return object;
  }

  /**
   * @param object the object to set
   */
  public void setObject( TabItemInterface object ) {
    this.object = object;
  }

  /**
   * @return the tabItem
   */
  public TabItem getTabItem() {
    return tabItem;
  }

  /**
   * @param tabItem the tabItem to set
   */
  public void setTabItem( TabItem tabItem ) {
    this.tabItem = tabItem;
  }

  /**
   * @return the objectType
   */
  public ObjectType getObjectType() {
    return objectType;
  }

  /**
   * @param objectType the objectType to set
   */
  public void setObjectType( ObjectType objectType ) {
    this.objectType = objectType;
  }

  /**
   * @return the filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename the filename to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }

  /**
   * @return the showingLocation
   */
  public boolean isShowingLocation() {
    return showingLocation;
  }

  /**
   * @param showingLocation the showingLocation to set
   */
  public void setShowingLocation( boolean showingLocation ) {
    this.showingLocation = showingLocation;
  }

}
