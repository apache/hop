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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelinePainterExtension;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;

import java.util.Map;

import static org.apache.hop.git.HopDiff.*;

@ExtensionPoint(
    id = "DrawDiffOnPipelineHopExtensionPoint",
    description = "Draws a marker on top of a pipeline Hop if it has added or removed",
    extensionPointId = "PipelinePainterArrow")
public class DrawDiffOnPipelineHopExtensionPoint
    implements IExtensionPoint<PipelinePainterExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, PipelinePainterExtension ext) throws HopException {

    IGc gc = ext.gc;
    ClassLoader classLoader = this.getClass().getClassLoader();

    try {
      Map<String, String> gitHops = ext.pipelineMeta.getAttributes(ATTR_GIT_HOPS);
      if (gitHops == null) {
        return;
      }

      for (String hopName : gitHops.keySet()) {
        String pipelineHopName = getPipelineHopName(ext.pipelineHop);
        if (ext.pipelineHop != null && pipelineHopName.equals(hopName)) {
          // Draw this status...
          //
          SvgFile svgFile = null;
          String status = gitHops.get(hopName);
          if (status != null) {
            switch (status) {
              case ADDED:
                svgFile = new SvgFile("added.svg", classLoader);
                break;
              case REMOVED:
                svgFile = new SvgFile("removed.svg", classLoader);
                break;
              case CHANGED:
                svgFile = new SvgFile("changed.svg", classLoader);
                break;
            }
            if (svgFile != null) {
              // Center of hop...
              //
              Point fr = ext.pipelineHop.getFromTransform().getLocation();
              Point to = ext.pipelineHop.getToTransform().getLocation();
              Point middle = new Point((fr.x + to.x) / 2, (fr.y + to.y) / 2);

              int iconSize = ConstUi.ICON_SIZE;
              try {
                iconSize = PropsUi.getInstance().getIconSize();
              } catch (Exception e) {
                // Exception when accessed from Hop Server
              }

              gc.drawImage(
                  svgFile,
                  middle.x,
                  middle.y,
                  iconSize / 2,
                  iconSize / 2,
                  gc.getMagnification(),
                  0);
            }
          }
        }
      }
    } catch (Exception e) {
      throw new HopException("Error drawing status on pipeline hop", e);
    }
  }
}
