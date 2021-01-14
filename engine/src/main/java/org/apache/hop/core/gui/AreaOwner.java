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

package org.apache.hop.core.gui;

import java.util.List;

/**
 * When we draw something in HopGui {@link org.apache.hop.pipeline.PipelinePainter} or {@link org.apache.hop.workflow.WorkflowPainter} we keep a list of all the things we draw and the object that's behind
 * it. That should make it a lot easier to track what was drawn, setting tooltips, etc.
 *
 * @author Matt
 */
public class AreaOwner<Parent, Owner> {

  public enum AreaType {
    NOTE, TRANSFORM_PARTITIONING, TRANSFORM_ICON, TRANSFORM_NAME, TRANSFORM_FAILURE_ICON,
    TRANSFORM_INPUT_HOP_ICON, TRANSFORM_OUTPUT_HOP_ICON, TRANSFORM_INFO_HOP_ICON,
    TRANSFORM_ERROR_HOP_ICON, TRANSFORM_TARGET_HOP_ICON,
    HOP_COPY_ICON, ROW_DISTRIBUTION_ICON, HOP_ERROR_ICON,
    HOP_INFO_ICON, HOP_INFO_TRANSFORM_COPIES_ERROR, HOP_INFO_TRANSFORMS_PARTITIONED,

    TRANSFORM_TARGET_HOP_ICON_OPTION, TRANSFORM_EDIT_ICON,
    TRANSFORM_MENU_ICON, TRANSFORM_COPIES_TEXT, TRANSFORM_DATA_SERVICE,

    ACTION_ICON, ACTION_NAME, WORKFLOW_HOP_ICON, WORKFLOW_HOP_PARALLEL_ICON,
    
    ACTION_BUSY, ACTION_RESULT_SUCCESS, ACTION_RESULT_FAILURE, ACTION_RESULT_CHECKPOINT,
    TRANSFORM_INJECT_ICON,

    TRANSFORM_OUTPUT_DATA,

    CUSTOM;

  }

  private Rectangle area;
  private Parent parent;
  private Owner owner;
  private AreaType areaType;

  /**
   * @param x
   * @param y
   * @param width
   * @param height
   * @param owner
   */
  public AreaOwner( AreaType areaType, int x, int y, int width, int height, Point offset,
                    Parent parent, Owner owner ) {
    super();
    this.areaType = areaType;
    this.area = new Rectangle( x - offset.x, y - offset.y, width, height );
    this.parent = parent;
    this.owner = owner;
  }

  public AreaOwner( AreaOwner<Parent, Owner> o ) {
    this.areaType = o.areaType;
    this.area = new Rectangle( o.area );
    this.parent = o.parent;
    this.owner = o.owner;
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

  public int getCentreX() {
    return area.x + area.width / 2;
  }

  public int getCentreY() {
    return area.y + area.height / 2;
  }

  /**
   * Calculate the distance between the centres of the areas.
   *
   * @param o The other area owner to calcualte the distance to
   * @return The distance.
   */
  public double distanceTo( AreaOwner o ) {
    int distX = getCentreX() - o.getCentreX();
    int distY = getCentreY() - o.getCentreY();
    return Math.sqrt( distX * distX + distY * distY );
  }

  /**
   * Find the last area owner, the one drawn last, for the given coordinate.
   * In other words, this searches the provided list back-to-front.
   *
   * @param areaOwners The list of area owners
   * @param x          The x coordinate
   * @param y          The y coordinate
   * @return The area owner or null if nothing could be found
   */
  public static synchronized <Owner, Parent> AreaOwner<Owner, Parent> getVisibleAreaOwner(
    List<AreaOwner<Owner, Parent>> areaOwners, int x, int y ) {

    for ( int i = areaOwners.size() - 1; i >= 0; i-- ) {
      AreaOwner<Owner, Parent> areaOwner = areaOwners.get( i );
      if ( areaOwner.contains( x, y ) ) {
        return areaOwner;
      }
    }
    return null;
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
  public Owner getOwner() {
    return owner;
  }

  /**
   * @param owner the owner to set
   */
  public void setOwner( Owner owner ) {
    this.owner = owner;
  }

  /**
   * @return the parent
   */
  public Parent getParent() {
    return parent;
  }

  /**
   * @param parent the parent to set
   */
  public void setParent( Parent parent ) {
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
}
