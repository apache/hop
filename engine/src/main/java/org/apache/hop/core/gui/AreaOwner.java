/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.core.gui;

import java.util.EnumSet;
import java.util.Set;

/**
 * When we draw something in HopGui (PipelinePainter) we keep a list of all the things we draw and the object that's behind
 * it. That should make it a lot easier to track what was drawn, setting tooltips, etc.
 *
 * @author Matt
 */
public class AreaOwner {

  public enum AreaType {
    NOTE, TRANSFORM_PARTITIONING, TRANSFORM_ICON, TRANSFORM_ERROR_ICON, TRANSFORM_ERROR_RED_ICON,
    TRANSFORM_INPUT_HOP_ICON, TRANSFORM_OUTPUT_HOP_ICON, TRANSFORM_INFO_HOP_ICON, TRANSFORM_ERROR_HOP_ICON, TRANSFORM_TARGET_HOP_ICON,
    HOP_COPY_ICON, ROW_DISTRIBUTION_ICON, HOP_ERROR_ICON, HOP_INFO_ICON, HOP_INFO_TRANSFORM_COPIES_ERROR,

    MINI_ICONS_BALLOON,

    TRANSFORM_TARGET_HOP_ICON_OPTION, TRANSFORM_EDIT_ICON, TRANSFORM_MENU_ICON, TRANSFORM_COPIES_TEXT, TRANSFORM_DATA_SERVICE,

    ACTION_ICON, WORKFLOW_HOP_ICON, WORKFLOW_HOP_PARALLEL_ICON, ACTION_MINI_ICON_INPUT, ACTION_MINI_ICON_OUTPUT,
    ACTION_MINI_ICON_CONTEXT, ACTION_MINI_ICON_EDIT,

    ACTION_BUSY, ACTION_RESULT_SUCCESS, ACTION_RESULT_FAILURE, ACTION_RESULT_CHECKPOINT,
    TRANSFORM_INJECT_ICON,

    TRANSFORM_OUTPUT_DATA,

    CUSTOM;

    private static final Set<AreaType> jobContextMenuArea = EnumSet.of( MINI_ICONS_BALLOON, ACTION_MINI_ICON_INPUT,
      ACTION_MINI_ICON_EDIT, ACTION_MINI_ICON_CONTEXT, ACTION_MINI_ICON_OUTPUT );

    private static final Set<AreaType> transformContextMenuArea = EnumSet.of( MINI_ICONS_BALLOON, TRANSFORM_INPUT_HOP_ICON,
      TRANSFORM_EDIT_ICON, TRANSFORM_MENU_ICON, TRANSFORM_OUTPUT_HOP_ICON, TRANSFORM_INJECT_ICON );

    public boolean belongsToJobContextMenu() {
      return jobContextMenuArea.contains( this );
    }

    public boolean belongsToPipelineContextMenu() {
      return transformContextMenuArea.contains( this );
    }

  }

  private Rectangle area;
  private Object parent;
  private Object owner;
  private AreaType areaType;
  private Object extensionAreaType;

  /**
   * @param x
   * @param y
   * @param width
   * @param heigth
   * @param owner
   */
  public AreaOwner( AreaType areaType, int x, int y, int width, int heigth, Point offset, Object parent,
                    Object owner ) {
    super();
    this.areaType = areaType;
    this.area = new Rectangle( x - offset.x, y - offset.y, width, heigth );
    this.parent = parent;
    this.owner = owner;
  }

  public AreaOwner( Object extensionAreaType, int x, int y, int width, int heigth, Point offset, Object parent,
                    Object owner ) {
    super();
    this.extensionAreaType = extensionAreaType;
    this.area = new Rectangle( x - offset.x, y - offset.y, width, heigth );
    this.parent = parent;
    this.owner = owner;
  }

  /**
   * Validate if a certain coordinate is contained in the area
   *
   * @param x x-coordinate
   * @param y y-coordinate
   * @return true if the specified coordinate is contained in the area
   */
  public boolean contains( int x, int y ) {
    return area.contains( x, y );
  }

  /**
   * @return the area
   */
  public Rectangle getArea() {
    return area;
  }

  /**
   * @param area the area to set
   */
  public void setArea( Rectangle area ) {
    this.area = area;
  }

  /**
   * @return the owner
   */
  public Object getOwner() {
    return owner;
  }

  /**
   * @param owner the owner to set
   */
  public void setOwner( Object owner ) {
    this.owner = owner;
  }

  /**
   * @return the parent
   */
  public Object getParent() {
    return parent;
  }

  /**
   * @param parent the parent to set
   */
  public void setParent( Object parent ) {
    this.parent = parent;
  }

  /**
   * @return the areaType
   */
  public AreaType getAreaType() {
    return areaType;
  }

  /**
   * @param areaType the areaType to set
   */
  public void setAreaType( AreaType areaType ) {
    this.areaType = areaType;
  }

  public Object getExtensionAreaType() {
    return extensionAreaType;
  }

  public void setExtensionAreaType( Object extensionAreaType ) {
    this.extensionAreaType = extensionAreaType;
  }
}
