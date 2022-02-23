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
 */

package org.apache.hop.www.async.xp;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.workflow.WorkflowPainterExtension;
import org.apache.hop.www.async.Defaults;

import java.util.Map;

@ExtensionPoint(
    id = "DrawAsyncLoggingIconExtensionPoint",
    description =
        "Draw the logging icon next to an action which has debug level information stored",
    extensionPointId = "WorkflowPainterAction")
public class DrawAsyncLoggingIconExtensionPoint
    implements IExtensionPoint<WorkflowPainterExtension> {

  public static final String STRING_AREA_OWNER_PREFIX = "Report pipeline async stats to service ";

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, WorkflowPainterExtension ext) {

    // Only the Pipeline action can have this...
    //
    if (!ext.actionMeta.isPipeline()) {
      return;
    }

    try {
      // This next map contains a simple "true" flag for every pipeline that contains a status
      //
      Map<String, String> pipelineMap =
          ext.actionMeta.getAttributesMap().get(Defaults.ASYNC_STATUS_GROUP);
      if (pipelineMap == null) {
        return;
      }

      String serviceName = pipelineMap.get(Defaults.ASYNC_ACTION_PIPELINE_SERVICE_NAME);
      if (serviceName == null) {
        return;
      }

      // Draw an icon
      //
      Rectangle r =
          drawLogIcon(ext.gc, ext.x1, ext.y1, ext.iconSize, this.getClass().getClassLoader());
      ext.areaOwners.add(
          new AreaOwner(
              AreaOwner.AreaType.CUSTOM,
              r.x,
              r.y,
              r.width,
              r.height,
              ext.offset,
              ext.actionMeta,
              STRING_AREA_OWNER_PREFIX + serviceName));

    } catch (Exception e) {
      // Just log the error, not that important
      log.logError("Error drawing async log icon", e);
    }
  }

  public Rectangle drawLogIcon(IGc gc, int x, int y, int iconSize, ClassLoader classLoader)
      throws Exception {
    int imageWidth = 16;
    int imageHeight = 16;
    int locationX = x + iconSize;
    int locationY =
        y
            + iconSize
            - imageHeight
            - 5; // -5 to prevent us from hitting the left bottom circle of the icon

    gc.drawImage(
        new SvgFile("ui/images/log.svg", classLoader),
        locationX,
        locationY,
        imageWidth,
        imageHeight,
        gc.getMagnification(),
        0);
    return new Rectangle(locationX, locationY, imageWidth, imageHeight);
  }
}
