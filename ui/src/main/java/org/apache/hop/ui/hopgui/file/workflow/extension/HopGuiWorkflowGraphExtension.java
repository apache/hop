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

package org.apache.hop.ui.hopgui.file.workflow.extension;

import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.eclipse.swt.events.MouseEvent;

public class HopGuiWorkflowGraphExtension {

  private HopGuiWorkflowGraph workflowGraph;
  private MouseEvent event;
  private Point point;
  private AreaOwner areaOwner;
  private boolean preventingDefault;

  public HopGuiWorkflowGraphExtension( HopGuiWorkflowGraph workflowGraph, MouseEvent event, Point point, AreaOwner areaOwner ) {
    this.workflowGraph = workflowGraph;
    this.event = event;
    this.point = point;
    this.areaOwner = areaOwner;
    this.preventingDefault = false;
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
   * Gets workflowGraph
   *
   * @return value of workflowGraph
   */
  public HopGuiWorkflowGraph getWorkflowGraph() {
    return workflowGraph;
  }

  /**
   * @param workflowGraph The workflowGraph to set
   */
  public void setWorkflowGraph( HopGuiWorkflowGraph workflowGraph ) {
    this.workflowGraph = workflowGraph;
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

  /**
   * Gets preventingDefault
   *
   * @return value of preventingDefault
   */
  public boolean isPreventingDefault() {
    return preventingDefault;
  }

  /**
   * @param preventingDefault The preventingDefault to set
   */
  public void setPreventingDefault( boolean preventingDefault ) {
    this.preventingDefault = preventingDefault;
  }
}
