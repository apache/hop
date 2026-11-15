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

package org.apache.hop.lineage.datahub;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.metadata.LineageSink;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/**
 * Publishes the pipelines and workflows of a folder to DataHub without running them, for example
 * after a deployment. See {@link DataHubPublisher}.
 */
@Action(
    id = "PUBLISH_TO_DATAHUB",
    name = "i18n::ActionPublishToDataHub.Name",
    description = "i18n::ActionPublishToDataHub.Description",
    image = "publish-datahub.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionPublishToDataHub.Keywords",
    documentationUrl = "/workflow/actions/publishtodatahub.html")
@GuiPlugin
@Getter
@Setter
public class ActionPublishToDataHub extends ActionBase implements IAction {
  private static final Class<?> PKG = ActionPublishToDataHub.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "PublishToDataHub.Dialog.Options";
  public static final String GROUP_OPTIONS = "i18n::ActionPublishToDataHub.Group.Options";

  @GuiWidgetElement(
      id = "sinkName",
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = LineageSink.class,
      label = "i18n::ActionPublishToDataHub.Sink.Label",
      toolTip = "i18n::ActionPublishToDataHub.Sink.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "sink", hopMetadataPropertyType = HopMetadataPropertyType.LINEAGE_SINK)
  private String sinkName;

  @GuiWidgetElement(
      id = "folder",
      order = "0200",
      type = GuiElementType.FOLDER,
      label = "i18n::ActionPublishToDataHub.Folder.Label",
      toolTip = "i18n::ActionPublishToDataHub.Folder.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "folder")
  private String folder;

  @GuiWidgetElement(
      id = "includingDocumentation",
      order = "0300",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ActionPublishToDataHub.Documentation.Label",
      toolTip = "i18n::ActionPublishToDataHub.Documentation.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "include_documentation")
  private boolean includingDocumentation;

  public ActionPublishToDataHub() {
    this("");
  }

  public ActionPublishToDataHub(String name) {
    super(name, "");
    this.folder = "${PROJECT_HOME}";
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult == null ? new Result() : previousResult;
    result.setResult(false);
    try {
      DataHubPublisher publisher =
          new DataHubPublisher(this, getMetadataProvider(), getLogChannel());
      publisher.setIncludingDocumentation(includingDocumentation);
      DataHubPublisher.Result published =
          publisher.publish(sinkName, StringUtils.defaultIfBlank(folder, "${PROJECT_HOME}"));
      logBasic(DataHubPublishCommand.describe(published));
      if (published.isSuccess()) {
        result.setResult(true);
      } else {
        result.setNrErrors(1);
      }
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionPublishToDataHub.Error", e.getMessage()), e);
      result.setNrErrors(1);
    }
    return result;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  @Override
  public ActionPublishToDataHub clone() {
    return (ActionPublishToDataHub) super.clone();
  }
}
