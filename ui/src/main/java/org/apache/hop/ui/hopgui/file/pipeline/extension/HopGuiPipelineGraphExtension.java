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

package org.apache.hop.ui.hopgui.file.pipeline.extension;

import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.eclipse.swt.events.MouseEvent;

public class HopGuiPipelineGraphExtension {

  private HopGuiPipelineGraph pipelineGraph;
  private MouseEvent event;
  private Point point;
  private boolean preventingDefault;
  private AreaOwner areaOwner;

  public HopGuiPipelineGraphExtension( HopGuiPipelineGraph pipelineGraph, MouseEvent event, Point point, AreaOwner areaOwner ) {
    this.pipelineGraph = pipelineGraph;
    this.event = event;
    this.point = point;
    this.areaOwner = areaOwner;
  }

  /**
   * Gets pipelineGraph
   *
   * @return value of pipelineGraph
   */
  public HopGuiPipelineGraph getPipelineGraph() {
    return pipelineGraph;
  }

  /**
   * @param pipelineGraph The pipelineGraph to set
   */
  public void setPipelineGraph( HopGuiPipelineGraph pipelineGraph ) {
    this.pipelineGraph = pipelineGraph;
  }

  /**
   * Gets event
   *
   * @return value of event
   */
  public MouseEvent getEvent() {
    return event;
  }

  /**
   * @param event The event to set
   */
  public void setEvent( MouseEvent event ) {
    this.event = event;
  }

  /**
   * Gets point
   *
   * @return value of point
   */
  public Point getPoint() {
    return point;
  }

  /**
   * @param point The point to set
   */
  public void setPoint( Point point ) {
    this.point = point;
  }

  /**
   * Gets preventDefault
   *
   * @return value of preventDefault
   */
  public boolean isPreventingDefault() {
    return preventingDefault;
  }

  /**
   * @param preventingDefault The preventDefault to set
   */
  public void setPreventingDefault( boolean preventingDefault ) {
    this.preventingDefault = preventingDefault;
  }

  /**
   * Gets areaOwner
   *
   * @return value of areaOwner
   */
  public AreaOwner getAreaOwner() {
    return areaOwner;
  }

  /**
   * @param areaOwner The areaOwner to set
   */
  public void setAreaOwner( AreaOwner areaOwner ) {
    this.areaOwner = areaOwner;
  }
}
