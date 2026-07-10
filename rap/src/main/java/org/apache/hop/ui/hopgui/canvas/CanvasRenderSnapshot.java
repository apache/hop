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

package org.apache.hop.ui.hopgui.canvas;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.gui.AreaOwner;
import org.eclipse.rap.json.JsonObject;

/** Cached SVG render payload served to the Hop Web canvas client. */
public class CanvasRenderSnapshot {

  private final long revision;
  private final String svg;
  private final List<AreaOwner> areaOwners;
  private final JsonObject props;

  public CanvasRenderSnapshot(
      long revision, String svg, List<AreaOwner> areaOwners, JsonObject props) {
    this.revision = revision;
    this.svg = svg;
    this.areaOwners = areaOwners == null ? new ArrayList<>() : new ArrayList<>(areaOwners);
    this.props = props == null ? new JsonObject() : props;
  }

  public long getRevision() {
    return revision;
  }

  public String getSvg() {
    return svg;
  }

  public List<AreaOwner> getAreaOwners() {
    return areaOwners;
  }

  public JsonObject getProps() {
    return props;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    json.add("revision", revision);
    json.add("svg", svg);
    json.add("areas", AreaOwnerJsonSerializer.toJsonArray(areaOwners));
    json.add("props", props);
    return json;
  }
}
