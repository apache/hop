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

package org.apache.hop.vfs.databricks.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.databricks.metadata.DatabricksConnection;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * Named Databricks Volumes / Workspace VFS endpoint. The metadata <strong>name</strong> is the VFS
 * URI scheme (for example {@code dbx-jars:///Volumes/...}). Authentication comes from a referenced
 * {@link DatabricksConnection} (host + PAT), not from credentials stored on this object.
 */
@Getter
@Setter
@GuiPlugin
@HopMetadata(
    key = "DatabricksVfsConnectionDefinition",
    name = "i18n::DatabricksVfsConnection.Name",
    description = "i18n::DatabricksVfsConnection.Description",
    image = "databricks-connection.svg",
    category = HopMetadataCategory.FILE_STORAGE,
    documentationUrl = "/metadata-types/databricks-vfs-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.VFS_DATABRICKS_CONNECTION)
public class DatabricksVfsConnection extends HopMetadataBase implements Serializable, IHopMetadata {

  private static final String PARENT = DatabricksVfsConnectionEditor.GUI_WIDGETS_PARENT_ID;

  @GuiWidgetElement(
      id = "10000-description",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::DatabricksVfsConnection.Description.Label",
      toolTip = "i18n::DatabricksVfsConnection.Description.Tooltip")
  @HopMetadataProperty
  private String description;

  /**
   * Name of the {@link DatabricksConnection} that supplies workspace host and personal access
   * token.
   */
  @GuiWidgetElement(
      id = "10010-databricks-connection",
      parentId = PARENT,
      type = GuiElementType.METADATA,
      metadata = DatabricksConnection.class,
      label = "i18n::DatabricksVfsConnection.Connection.Label",
      toolTip = "i18n::DatabricksVfsConnection.Connection.Tooltip")
  @HopMetadataProperty
  private String databricksConnectionName;

  /**
   * Optional absolute workspace root for this scheme (for example {@code
   * /Volumes/catalog/schema/volume}). When set, short URIs are relative to it: {@code
   * name:///input} → {@code /Volumes/…/volume/input}. When blank, URIs must use full {@code
   * /Volumes/…} or {@code /Workspace/…} paths.
   */
  @GuiWidgetElement(
      id = "10020-default-base-path",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::DatabricksVfsConnection.DefaultBasePath.Label",
      toolTip = "i18n::DatabricksVfsConnection.DefaultBasePath.Tooltip")
  @HopMetadataProperty
  private String defaultBasePath;

  public DatabricksVfsConnection() {
    // defaults
  }
}
