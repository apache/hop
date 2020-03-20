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

package org.apache.hop.ui.hopgui.file.job.extension;

import org.apache.hop.core.gui.Point;
import org.apache.hop.ui.hopgui.file.job.HopGuiJobGraph;
import org.eclipse.swt.events.MouseEvent;

public class HopGuiJobGraphExtension {

  private HopGuiJobGraph jobGraph;
  private MouseEvent event;
  private Point point;

  public HopGuiJobGraphExtension( HopGuiJobGraph jobGraph, MouseEvent event, Point point ) {
    this.jobGraph = jobGraph;
    this.event = event;
    this.point = point;
  }

  /**
   * Gets jobGraph
   *
   * @return value of jobGraph
   */
  public HopGuiJobGraph getJobGraph() {
    return jobGraph;
  }

  /**
   * @param jobGraph The jobGraph to set
   */
  public void setJobGraph( HopGuiJobGraph jobGraph ) {
    this.jobGraph = jobGraph;
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
}
