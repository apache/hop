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

package org.apache.hop.workflow;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.SvgGc;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.variables.IVariables;

import java.util.ArrayList;

public class WorkflowSvgPainter {

  public static final String generateWorkflowSvg(
      WorkflowMeta workflowMeta, float magnification, IVariables variables) throws HopException {
    try {
      Point maximum = workflowMeta.getMaximum();
      maximum.multiply(magnification);

      HopSvgGraphics2D graphics2D = HopSvgGraphics2D.newDocument();

      SvgGc gc = new SvgGc(graphics2D, new Point(maximum.x, maximum.y), 32, 0, 0);
      WorkflowPainter workflowPainter =
          new WorkflowPainter(
              gc,
              variables,
              workflowMeta,
              maximum,
              null,
              null,
              null,
            null,
              new ArrayList<>(),
              32,
              1,
              0,
              "Arial",
              10,
              1.0d,
              false
          );
      workflowPainter.setMagnification(magnification);
      workflowPainter.drawWorkflow();

      // convert to SVG XML
      //
      return graphics2D.toXml();

    } catch (Exception e) {
      throw new HopException("Unable to generate SVG for workflow " + workflowMeta.getName(), e);
    }
  }
}
