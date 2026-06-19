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

package org.apache.hop.datastream.metadata;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.datastream.plugin.IDataStream;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "data-stream",
    name = "i18n::DataStream.name",
    description = "i18n::DataStream.description",
    image = "ui/images/arrow-stream.svg",
    documentationUrl = "/metadata-types/data-stream.html",
    hopMetadataPropertyType = HopMetadataPropertyType.DATA_STREAM,
    supportsGlobalReplace = true)
@Getter
@Setter
public class DataStreamMeta extends HopMetadataBase implements IHopMetadata {
  public static final String GUI_WIDGETS_PARENT_ID = "DataStreamEditor-GuiWidgetsParent";

  private static final String WIDGET_ID_DATASTREAM_DESCRIPTION = "10000-datastream-description";

  @HopMetadataProperty private String description;

  @HopMetadataProperty private IDataStream dataStream;

  public DataStreamMeta() {
    super();
  }

  public DataStreamMeta(DataStreamMeta dataStreamMeta) {
    super(dataStreamMeta);
    this.description = dataStreamMeta.description;
    if (dataStreamMeta.dataStream != null) {
      this.dataStream = dataStreamMeta.dataStream.clone();
    }
  }
}
