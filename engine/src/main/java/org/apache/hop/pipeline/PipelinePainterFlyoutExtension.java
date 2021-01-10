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

public class PipelinePainterFlyoutExtension {

  public IGc gc;
  public boolean shadow;
  public List<AreaOwner> areaOwners;
  public PipelineMeta pipelineMeta;
  public TransformMeta transformMeta;
  public Point offset;
  public Point area;
  public float translationX;
  public float translationY;
  public float magnification;

  public PipelinePainterFlyoutExtension( IGc gc, List<AreaOwner> areaOwners, PipelineMeta pipelineMeta,
                                         TransformMeta transformMeta, float translationX, float translationY, float magnification, Point area, Point offset ) {
    super();
    this.gc = gc;
    this.areaOwners = areaOwners;
    this.pipelineMeta = pipelineMeta;
    this.transformMeta = transformMeta;
    this.translationX = translationX;
    this.translationY = translationY;
    this.magnification = magnification;
    this.area = area;
    this.offset = offset;
  }
}
