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
 *
 */

package org.apache.hop.pipeline.transforms.streamoutput;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.datastream.metadata.DataStreamMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;

@Getter
@Setter
@Transform(
    id = "DataStreamOutput",
    name = "i18n::DataStreamOutputMeta.Name",
    description = "i18n::DataStreamOutputMeta.Description",
    image = "stream-output.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::DataStreamOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/data-stream-output.html")
@GuiPlugin
public class DataStreamOutputMeta extends BaseTransformMeta<DataStreamOutput, DataStreamOutputData>
    implements ITransformMeta {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "DATA_STREAM_OUTPUT_DIALOG_OPTIONS";
  public static final String WIDGET_COLUMN_NAME_FIELD = "COLUMN_NAME_FIELD";

  @GuiWidgetElement(
      id = WIDGET_COLUMN_NAME_FIELD,
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DataStreamMeta.class,
      toolTip = "i18n::DataStreamOutputMeta.DataStreamName.Tooltip",
      label = "i18n::DataStreamOutputMeta.DataStreamName.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty(key = "data-stream")
  private String dataStreamName;
}
