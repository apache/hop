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

package org.apache.hop.pipeline;

import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Point;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

public class PipelinePainterExtension {

  public IGc gc;
  public List<AreaOwner> areaOwners;
  public PipelineMeta pipelineMeta;
  public TransformMeta transformMeta;
  public PipelineHopMeta pipelineHop;
  public int x1, y1, x2, y2, mx, my;
  public Point offset;
  public int iconSize;

  public PipelinePainterExtension( IGc gc, List<AreaOwner> areaOwners, PipelineMeta pipelineMeta,
                                   TransformMeta transformMeta, PipelineHopMeta pipelineHop, int x1, int y1, int x2, int y2, int mx, int my, Point offset,
                                   int iconSize ) {
    super();
    this.gc = gc;
    this.areaOwners = areaOwners;
    this.pipelineMeta = pipelineMeta;
    this.transformMeta = transformMeta;
    this.pipelineHop = pipelineHop;
    this.x1 = x1;
    this.y1 = y1;
    this.x2 = x2;
    this.y2 = y2;
    this.mx = mx;
    this.my = my;
    this.offset = offset;
    this.iconSize = iconSize;
  }
}
