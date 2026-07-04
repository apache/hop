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

package org.apache.hop.core.gui;

import java.util.ArrayList;
import java.util.List;

/** Result of rendering a pipeline or workflow canvas to SVG, including click regions. */
public class CanvasSvgRenderResult {

  private final String svg;
  private final List<AreaOwner> areaOwners;
  private final Rectangle viewPort;
  private final Rectangle graphPort;

  public CanvasSvgRenderResult(String svg, List<AreaOwner> areaOwners) {
    this(svg, areaOwners, null, null);
  }

  public CanvasSvgRenderResult(
      String svg, List<AreaOwner> areaOwners, Rectangle viewPort, Rectangle graphPort) {
    this.svg = svg;
    this.areaOwners = areaOwners == null ? new ArrayList<>() : new ArrayList<>(areaOwners);
    this.viewPort = viewPort;
    this.graphPort = graphPort;
  }

  public String getSvg() {
    return svg;
  }

  public List<AreaOwner> getAreaOwners() {
    return areaOwners;
  }

  public Rectangle getViewPort() {
    return viewPort;
  }

  public Rectangle getGraphPort() {
    return graphPort;
  }
}
