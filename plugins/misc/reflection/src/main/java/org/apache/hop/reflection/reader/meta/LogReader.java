/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.reflection.reader.meta;

import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.ui.hopgui.file.pipeline.extension.TypePipelineFile;

@HopMetadata(
    key = "log-reader",
    name = "Log Reader",
    description = "Allows for a standardized way to read back logging information",
    image = "ui/images/log.svg")
@GuiPlugin
public class LogReader extends HopMetadataBase implements IHopMetadata {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "GuiPlugin-LogReader-Parent";

  @GuiWidgetElement(
      order = "10000-pipeline-filename",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "Pipeline filename",
      toolTip = "This is the filename of a pipeline to read pipeline logging information",
      typeFilename = TypePipelineFile.class)
  @HopMetadataProperty
  private String pipelineFilename;

  @GuiWidgetElement(
      order = "10100-transform-filename",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "Transform filename",
      toolTip = "This is the filename of a pipeline to read transform logging information",
      typeFilename = TypePipelineFile.class)
  @HopMetadataProperty
  private String transformFilename;

  @GuiWidgetElement(
      order = "10200-workflow-filename",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "Workflow filename",
      toolTip = "This is the filename of a pipeline to read workflow logging information",
      typeFilename = TypePipelineFile.class)
  @HopMetadataProperty
  private String workflowFilename;

  @GuiWidgetElement(
      order = "10300-action-filename",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "Action filename",
      toolTip = "This is the filename of a pipeline to read action logging information",
      typeFilename = TypePipelineFile.class)
  @HopMetadataProperty
  private String actionFilename;

  public LogReader() {
    super("log-reader");
  }

  public LogReader(LogReader l) {
    super(l.name);
    this.pipelineFilename = l.pipelineFilename;
    this.transformFilename = l.transformFilename;
    this.workflowFilename = l.workflowFilename;
    this.actionFilename = l.actionFilename;
  }

  @Override
  protected LogReader clone() {
    return new LogReader(this);
  }

  /**
   * Gets pipelineFilename
   *
   * @return value of pipelineFilename
   */
  public String getPipelineFilename() {
    return pipelineFilename;
  }

  /** @param pipelineFilename The pipelineFilename to set */
  public void setPipelineFilename(String pipelineFilename) {
    this.pipelineFilename = pipelineFilename;
  }

  /**
   * Gets transformFilename
   *
   * @return value of transformFilename
   */
  public String getTransformFilename() {
    return transformFilename;
  }

  /** @param transformFilename The transformFilename to set */
  public void setTransformFilename(String transformFilename) {
    this.transformFilename = transformFilename;
  }

  /**
   * Gets workflowFilename
   *
   * @return value of workflowFilename
   */
  public String getWorkflowFilename() {
    return workflowFilename;
  }

  /** @param workflowFilename The workflowFilename to set */
  public void setWorkflowFilename(String workflowFilename) {
    this.workflowFilename = workflowFilename;
  }

  /**
   * Gets actionFilename
   *
   * @return value of actionFilename
   */
  public String getActionFilename() {
    return actionFilename;
  }

  /** @param actionFilename The actionFilename to set */
  public void setActionFilename(String actionFilename) {
    this.actionFilename = actionFilename;
  }
}
