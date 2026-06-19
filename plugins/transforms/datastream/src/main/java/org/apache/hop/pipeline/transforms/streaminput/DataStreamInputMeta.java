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

package org.apache.hop.pipeline.transforms.streaminput;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datastream.metadata.DataStreamMeta;
import org.apache.hop.datastream.plugin.IDataStream;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.jspecify.annotations.NonNull;

@Getter
@Setter
@Transform(
    id = "DataStreamInput",
    name = "i18n::DataStreamInputMeta.Name",
    description = "i18n::DataStreamInputMeta.Description",
    image = "stream-input.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::DataStreamInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/data-stream-input.html")
@GuiPlugin
public class DataStreamInputMeta extends BaseTransformMeta<DataStreamInput, DataStreamInputData> {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "DATA_STREAM_INPUT_DIALOG_OPTIONS";
  public static final String WIDGET_COLUMN_NAME_FIELD = "COLUMN_NAME_FIELD";

  @GuiWidgetElement(
      id = WIDGET_COLUMN_NAME_FIELD,
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DataStreamMeta.class,
      toolTip = "i18n::DataStreamInputMeta.DataStreamName.Tooltip",
      label = "i18n::DataStreamInputMeta.DataStreamName.Label",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty(key = "data-stream")
  private String dataStreamName;

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Open the data stream, read the metadata and close it again.
    String realDataStreamName = variables.resolve(dataStreamName);
    try {
      DataStreamMeta dataStreamMeta =
          getAndValidateDataStream(metadataProvider, realDataStreamName);
      IDataStream dataStream = dataStreamMeta.getDataStream();

      // Initialize the data stream
      //
      try {
        dataStream.initialize(variables, metadataProvider, false, dataStreamMeta);
        IRowMeta streamRowMeta = dataStream.getRowMeta();
        inputRowMeta.addRowMeta(streamRowMeta);
      } finally {
        dataStream.close();
      }
    } catch (Exception e) {
      throw new HopTransformException(
          "Error getting row metadata from data stream " + realDataStreamName, e);
    }
  }

  public static @NonNull DataStreamMeta getAndValidateDataStream(
      IHopMetadataProvider metadataProvider, String realDataStreamName) throws HopException {
    IHopMetadataSerializer<DataStreamMeta> serializer =
        metadataProvider.getSerializer(DataStreamMeta.class);
    DataStreamMeta dataStreamMeta = serializer.load(realDataStreamName);
    if (dataStreamMeta == null) {
      throw new HopException("Unable to find data stream metadata element: " + realDataStreamName);
    }
    IDataStream dataStream = dataStreamMeta.getDataStream();
    if (dataStream == null) {
      throw new HopException(
          "Please specify and configure a data stream type in " + dataStreamMeta.getName());
    }
    return dataStreamMeta;
  }
}
