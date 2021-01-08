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
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

public class HopGuiPipelinePainterFlyoutTooltipExtension {

  private AreaOwner areaOwner;
  private HopGuiPipelineGraph pipelineGraph;
  private Point point;

  public static final String DET_RUN = "DET_RUN";
  public static final String DET_INSPECT = "DET_INSPECT";
  public static final String DET_LABEL = "DET_LABEL";

  public HopGuiPipelinePainterFlyoutTooltipExtension( AreaOwner areaOwner, HopGuiPipelineGraph pipelineGraph, Point point ) {
    super();
    this.areaOwner = areaOwner;
    this.pipelineGraph = pipelineGraph;
    this.point = point;
  }

  public TransformMeta getTransformMeta() {
    return (TransformMeta) this.areaOwner.getParent();
  }

  public HopGuiPipelineGraph getHopGuiPipelineGraph() {
    return pipelineGraph;
  }

  public Point getPoint() {
    return point;
  }
}
