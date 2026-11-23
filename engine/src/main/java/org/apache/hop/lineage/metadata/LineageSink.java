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

package org.apache.hop.lineage.metadata;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.hub.LineageSinkTarget;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * A place lineage is sent to, such as an OpenLineage collector or DataHub. A pipeline or workflow
 * run configuration selects one; the runs it starts, and the runs those start, report their lineage
 * there.
 */
@HopMetadata(
    key = "lineage-sink",
    name = "i18n::LineageSink.name",
    description = "i18n::LineageSink.description",
    image = "ui/images/lineage-sink.svg",
    category = HopMetadataCategory.CONNECTIONS,
    documentationUrl = "/metadata-types/lineage-sink.html",
    hopMetadataPropertyType = HopMetadataPropertyType.LINEAGE_SINK,
    supportsGlobalReplace = true)
@Getter
@Setter
public class LineageSink extends HopMetadataBase implements IHopMetadata, Cloneable {

  /** The parent id of the widgets of a sink type in the editor. */
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "LineageSink-PluginSpecific-Options";

  @HopMetadataProperty private String description;

  /** The sink type and its configuration. */
  @HopMetadataProperty private ILineageSinkConfiguration sink;

  public LineageSink() {}

  public LineageSink(String name, String description, ILineageSinkConfiguration sink) {
    super(name);
    this.description = description;
    this.sink = sink;
  }

  public LineageSink(LineageSink other) {
    this(other.name, other.description, other.sink == null ? null : other.sink.clone());
  }

  @Override
  public LineageSink clone() {
    return new LineageSink(this);
  }

  /**
   * What the lineage hub needs to start this sink, with the settings resolved against the variables
   * of a run.
   *
   * @throws HopException when no sink type is set or the configuration can't be resolved
   */
  public LineageSinkTarget toTarget(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    if (sink == null) {
      throw new HopException("Lineage sink '" + name + "' has no sink type");
    }
    return new LineageSinkTarget(
        name, sink.getPluginId(), sink.toSettings(variables, metadataProvider));
  }
}
