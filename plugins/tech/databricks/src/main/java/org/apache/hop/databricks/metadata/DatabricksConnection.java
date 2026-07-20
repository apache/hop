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

package org.apache.hop.databricks.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * Workspace connection for the Databricks REST / Jobs API (not JDBC SQL warehouse). Used by
 * Databricks Job Run actions and related Jobs API clients. Also referenced by Databricks VFS
 * Connection metadata for Volumes / Workspace file access (host + PAT only — VFS scheme is the VFS
 * metadata name, not this connection name).
 */
@Getter
@Setter
@GuiPlugin
@HopMetadata(
    key = "DatabricksConnection",
    name = "i18n::DatabricksConnection.Name",
    description = "i18n::DatabricksConnection.Description",
    image = "databricks-connection.svg",
    category = HopMetadataCategory.CONNECTIONS,
    documentationUrl = "/metadata-types/databricks-connection.html")
public class DatabricksConnection extends HopMetadataBase implements Serializable, IHopMetadata {

  private static final String PARENT = DatabricksConnectionEditor.GUI_WIDGETS_PARENT_ID;

  @GuiWidgetElement(
      id = "10000-description",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::DatabricksConnection.Description.Label",
      toolTip = "i18n::DatabricksConnection.Description.Tooltip")
  @HopMetadataProperty
  private String description;

  @GuiWidgetElement(
      id = "10010-host",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::DatabricksConnection.Host.Label",
      toolTip = "i18n::DatabricksConnection.Host.Tooltip")
  @HopMetadataProperty
  private String host;

  @GuiWidgetElement(
      id = "10020-token",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::DatabricksConnection.Token.Label",
      toolTip = "i18n::DatabricksConnection.Token.Tooltip")
  @HopMetadataProperty(password = true)
  private String token;

  /** Optional API base path; default empty means {@code /api/2.1}. */
  @GuiWidgetElement(
      id = "10030-api-base",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::DatabricksConnection.ApiBase.Label",
      toolTip = "i18n::DatabricksConnection.ApiBase.Tooltip")
  @HopMetadataProperty
  private String apiBasePath;

  public DatabricksConnection() {
    this.apiBasePath = "/api/2.1";
  }
}
