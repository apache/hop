/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.workflow;

import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.IGC;
import org.apache.hop.core.gui.Point;

import java.util.List;

public class WorkflowPainterExtension {

  public IGC gc;
  public List<AreaOwner> areaOwners;
  public WorkflowMeta workflowMeta;
  public WorkflowHopMeta jobHop;
  public int x1, y1, x2, y2, mx, my;
  public Point offset;

  public WorkflowPainterExtension( IGC gc, List<AreaOwner> areaOwners, WorkflowMeta workflowMeta,
                                   WorkflowHopMeta jobHop, int x1, int y1, int x2, int y2, int mx, int my, Point offset ) {
    super();
    this.gc = gc;
    this.areaOwners = areaOwners;
    this.workflowMeta = workflowMeta;
    this.jobHop = jobHop;
    this.x1 = x1;
    this.y1 = y1;
    this.x2 = x2;
    this.y2 = y2;
    this.mx = mx;
    this.my = my;
    this.offset = offset;
  }
}
