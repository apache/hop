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
package org.apache.hop.vfs.minio.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@Getter
@Setter
@GuiPlugin
@HopMetadata(
    key = "MinioConnectionDefinition",
    name = "i18n::MinioMeta.Name",
    description = "i18n::MinioMeta.Description",
    image = "minio.svg",
    documentationUrl = "/metadata-types/minio-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.VFS_AZURE_CONNECTION)
public class MinioMeta extends HopMetadataBase implements Serializable, IHopMetadata {
  private static final String WIDGET_ID_MINIO_DESCRIPTION = "10000-minio-description";
  private static final String WIDGET_ID_MINIO_ACCESS_KEY = "10050-minio-access-key";
  private static final String WIDGET_ID_MINIO_SECRET_KEY = "10100-minio-secret-key";
  private static final String WIDGET_ID_MINIO_ENDPOINT_HOSTNAME = "10200-minio-endpoint-hostname";
  private static final String WIDGET_ID_MINIO_ENDPOINT_PORT = "10210-minio-endpoint-port";
  private static final String WIDGET_ID_MINIO_ENDPOINT_SECURE = "10220-minio-endpoint-secure";
  private static final String WIDGET_ID_MINIO_REGION = "10300-minio-region";
  private static final String WIDGET_ID_MINIO_PART_SIZE = "10400-minio-part-size";

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_DESCRIPTION,
      parentId = MinioMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.Description.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.Description.Description")
  @HopMetadataProperty
  private String description;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_ENDPOINT_HOSTNAME,
      parentId = MinioMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.Host.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.Host.Description")
  @HopMetadataProperty
  private String endPointHostname;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_ENDPOINT_PORT,
      parentId = MinioMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.Port.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.Port.Description")
  @HopMetadataProperty
  private String endPointPort;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_ENDPOINT_SECURE,
      parentId = MinioMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.Secure.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.Secure.Description")
  @HopMetadataProperty
  private boolean endPointSecure;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_ACCESS_KEY,
      parentId = MinioMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.AccessKey.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.AccessKey.Description")
  @HopMetadataProperty(password = true)
  private String accessKey;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_SECRET_KEY,
      parentId = MinioMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.SecretKey.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.SecretKey.Description")
  @HopMetadataProperty(password = true)
  private String secretKey;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_REGION,
      parentId = MinioMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.Region.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.Region.Description")
  @HopMetadataProperty
  private String region;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_PART_SIZE,
      parentId = MinioMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.PartSize.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio.metadata:MinioVFS.PartSize.Description")
  @HopMetadataProperty
  private String partSize;

  public MinioMeta() {
    // Do nothing
    this.partSize = Integer.toString(5 * 1024 * 1024);
  }
}
