/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowPainter;

import static org.apache.hop.git.PdiDiff.ADDED;
import static org.apache.hop.git.PdiDiff.ATTR_GIT;
import static org.apache.hop.git.PdiDiff.ATTR_STATUS;
import static org.apache.hop.git.PdiDiff.CHANGED;
import static org.apache.hop.git.PdiDiff.REMOVED;

@ExtensionPoint(
    id = "DrawDiffOnActionExtensionPoint",
    description = "Draws a marker on top of an action if it has some change",
    extensionPointId = "WorkflowPainterEnd")
public class DrawDiffOnActionExtensionPoint implements IExtensionPoint {

  @Override
  public void callExtensionPoint( ILogChannel log, IVariables variables, Object object ) throws HopException {
    if (!(object instanceof WorkflowPainter)) {
      return;
    }
    WorkflowPainter painter = (WorkflowPainter) object;
    Point offset = painter.getOffset();
    IGc gc = painter.getGc();
    WorkflowMeta workflowMeta = painter.getWorkflowMeta();
    try {
      workflowMeta.getActions().stream()
          .filter(je -> je.getAttribute(ATTR_GIT, ATTR_STATUS) != null)
          .forEach(
              je -> {
                if (workflowMeta.getWorkflowVersion() == null
                    ? false
                    : workflowMeta.getWorkflowVersion().startsWith("git")) {
                  String status = je.getAttribute(ATTR_GIT, ATTR_STATUS);
                  Point n = je.getLocation();
                  String location;
                  if (status.equals(REMOVED)) {
                    location = "removed.svg";
                  } else if (status.equals(CHANGED)) {
                    location = "changed.svg";
                  } else if (status.equals(ADDED)) {
                    location = "added.svg";
                  } else { // Unchanged
                    return;
                  }
                  int iconSize = ConstUi.ICON_SIZE;
                  try {
                    iconSize = PropsUi.getInstance().getIconSize();
                  } catch (Exception e) {
                    // Exception when accessed from Carte
                  }
                  int x = (n.x + iconSize + offset.x) - (iconSize / 4);
                  int y = n.y + offset.y - (iconSize / 4);
                  try {
                    gc.drawImage(
                        new SvgFile(location, getClass().getClassLoader()),
                        x,
                        y,
                        iconSize / 4,
                        iconSize / 4,
                        gc.getMagnification(),
                        0);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                } else {
                  je.getAttributesMap().remove(ATTR_GIT);
                }
              });
    } catch (Exception e) {
      throw new HopException("Error drawing status icon on action", e);
    }
  }
}
