/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.git;

import static org.apache.hop.git.HopDiff.ADDED;
import static org.apache.hop.git.HopDiff.ATTR_GIT;
import static org.apache.hop.git.HopDiff.ATTR_STATUS;
import static org.apache.hop.git.HopDiff.CHANGED;
import static org.apache.hop.git.HopDiff.REMOVED;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;

@ExtensionPoint(
    id = "DrawDiffOnTransExtensionPoint",
    description = "Draws a marker on top of a transform if it has some change",
    extensionPointId = "PipelinePainterEnd")
public class DrawDiffOnTransformExtensionPoint implements IExtensionPoint {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Object object)
      throws HopException {
    if (!(object instanceof PipelinePainter)) {
      return;
    }
    PipelinePainter painter = (PipelinePainter) object;
    DPoint offset = painter.getOffset();
    IGc gc = painter.getGc();
    PipelineMeta pipelineMeta = painter.getPipelineMeta();
    try {
      pipelineMeta.getTransforms().stream()
          .filter(transform -> transform.getAttribute(ATTR_GIT, ATTR_STATUS) != null)
          .forEach(
              transform -> {
                if (pipelineMeta.getPipelineVersion() != null
                    && pipelineMeta.getPipelineVersion().startsWith("git")) {
                  String status = transform.getAttribute(ATTR_GIT, ATTR_STATUS);
                  Point n = transform.getLocation();
                  String location;
                  switch (status) {
                    case REMOVED -> location = "removed.svg";
                    case CHANGED -> location = "changed.svg";
                    case ADDED -> location = "added.svg";
                    default -> {
                      return;
                    }
                  }
                  int iconSize = ConstUi.ICON_SIZE;
                  try {
                    iconSize = PropsUi.getInstance().getIconSize();
                  } catch (Exception e) {
                    // Exception when accessed from Hop Server
                  }
                  double x = (n.x + iconSize + offset.x) - (iconSize / 4);
                  double y = n.y + offset.y - (iconSize / 4);
                  try {
                    gc.drawImage(
                        new SvgFile(location, getClass().getClassLoader()),
                        (int) x,
                        (int) y,
                        iconSize / 2,
                        iconSize / 2,
                        gc.getMagnification(),
                        0);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                } else {
                  transform.getAttributesMap().remove(ATTR_GIT);
                }
              });
    } catch (Exception e) {
      throw new HopException("Error drawing status on transform", e);
    }
  }
}
