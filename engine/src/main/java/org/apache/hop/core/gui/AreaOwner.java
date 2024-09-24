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
 * When we draw something in HopGui {@link org.apache.hop.pipeline.PipelinePainter} or {@link
 * org.apache.hop.workflow.WorkflowPainter} we keep a list of all the things we draw and the object
 * that's behind it. That should make it a lot easier to track what was drawn, setting tooltips,
 * etc.
 */
public class AreaOwner {

  public enum AreaType {
    /** The note pad area. */
    NOTE(true),

    /** The transformation partitioning area. */
    TRANSFORM_PARTITIONING(false),
    /** The transformation icon area. */
    TRANSFORM_ICON(true),
    /** The transformation name area. */
    TRANSFORM_NAME(true),
    /**
     * The transformation decoration information icon in the top left-hand corner indicates that it
     * has a description.
     */
    TRANSFORM_INFO_ICON(true),
    /**
     * The transformation decoration failure icon area in top right corner to indicate an execution
     * error.
     */
    TRANSFORM_FAILURE_ICON(false),
    /** The transformation decoration area for the number of execution copies. */
    TRANSFORM_COPIES_TEXT(true),
    /** TODO: Not used yet */
    TRANSFORM_DATA_SERVICE(false),
    /**
     * The transformation decoration data icon in the bottom right-hand corner to indicate that
     * there are available output rows.
     */
    TRANSFORM_OUTPUT_DATA(true),
    /** The pipeline hop decoration */
    TRANSFORM_TARGET_HOP_ICON(true),

    /** The pipeline hop decoration copy icon that indicate that all rows. */
    HOP_COPY_ICON(true),
    /** The pipeline hop decoration icon for error handling. */
    HOP_ERROR_ICON(true),
    /**
     * The pipeline hop decoration icon indicates that additional information is being sent to a
     * transformation.
     */
    HOP_INFO_ICON(true),
    /** TODO: ? */
    HOP_INFO_TRANSFORM_COPIES_ERROR(false),
    /** TODO: ? */
    HOP_INFO_TRANSFORMS_PARTITIONED(false),

    /**
     * The workflow hop decoration icon indicates the condition of the following action:
     * true/false/unconditional.
     */
    WORKFLOW_HOP_ICON(true),
    /** The workflow hop decoration icon for parallel execution. */
    WORKFLOW_HOP_PARALLEL_ICON(true),

    /** The action icon area. */
    ACTION_ICON(true),
    /** The action name area. */
    ACTION_NAME(true),
    /**
     * The action decoration information icon in the top left-hand corner that indicates that it has
     * a description.
     */
    ACTION_INFO_ICON(true),
    /**
     * The action decoration busy icon at the top right that indicates that it is currently running.
     */
    ACTION_BUSY(false),
    /** The action decoration success icon in top right corner to indicate an execution success. */
    ACTION_RESULT_SUCCESS(true),
    /** The action decoration failure icon in top right corner to indicate an execution error. */
    ACTION_RESULT_FAILURE(true),
    /** TODO: Not used yet */
    ACTION_RESULT_CHECKPOINT(true),

    /** For custom row distribution plugin icon */
    ROW_DISTRIBUTION_ICON(false),

    /** A custom area for plugins. */
    CUSTOM(true);

    private final boolean hover;

    AreaType(boolean hover) {
      this.hover = hover;
    }

    public boolean isSupportHover() {
      return hover;
    }
  }

  private Rectangle area;
  private Object parent;
  private Object owner;
  private AreaType areaType;

  /**
   * @param x
   * @param y
   * @param width
   * @param height
   * @param owner
   */
  public AreaOwner(
      AreaType areaType,
      int x,
      int y,
      int width,
      int height,
      DPoint offset,
      Object parent,
      Object owner) {
    super();
    this.areaType = areaType;
    this.area = new Rectangle((int) (x - offset.x), (int) (y - offset.y), width, height);
    this.parent = parent;
    this.owner = owner;
  }

  public AreaOwner(AreaOwner o) {
    this.areaType = o.areaType;
    this.area = new Rectangle(o.area);
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
  public boolean contains(int x, int y) {
    return area.contains(x, y);
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
  public double distanceTo(AreaOwner o) {
    int distX = getCentreX() - o.getCentreX();
    int distY = getCentreY() - o.getCentreY();
    return Math.sqrt(distX * distX + distY * distY);
  }

  /**
   * Find the last area owner, the one drawn last, for the given coordinate. In other words, this
   * searches the provided list back-to-front.
   *
   * @param areaOwners The list of area owners
   * @param x The x coordinate
   * @param y The y coordinate
   * @return The area owner or null if nothing could be found
   */
  public static synchronized AreaOwner getVisibleAreaOwner(
      List<AreaOwner> areaOwners, int x, int y) {

    for (int i = areaOwners.size() - 1; i >= 0; i--) {
      AreaOwner areaOwner = areaOwners.get(i);
      if (areaOwner.contains(x, y)) {
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
  public void setArea(Rectangle area) {
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
  public void setOwner(Object owner) {
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
  public void setParent(Object parent) {
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
  public void setAreaType(AreaType areaType) {
    this.areaType = areaType;
  }
}
