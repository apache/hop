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
 *
 */

package org.apache.hop.documentation;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/** Generate documentation in MarkDown format. */
@Action(
    id = "DOCUMENTATION",
    name = "i18n::ActionDoc.Name",
    description = "i18n::ActionDoc.Description",
    image = "info.svg",
    categoryDescription = "i18n:org.apache.hop.documentation:ActionCategory.Category.Utility",
    keywords = "i18n::ActionDoc.keyword",
    documentationUrl = "/workflow/actions/documentation.html")
@Getter
@Setter
@GuiPlugin
public class ActionDoc extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionDoc.class;

  public static final String GUI_WIDGETS_PARENT_ID = "ActionDocDialog-GuiWidgetsParent";

  @GuiWidgetElement(
      id = "1000-action-doc-dialog-target-filename",
      parentId = GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      variables = true,
      label = "i18n::ActionDoc.Target.Label")
  @HopMetadataProperty
  private String targetParentFolder;

  @GuiWidgetElement(
      id = "1010-action-doc-dialog-include-parameters",
      parentId = GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::ActionDoc.Parameters.Label")
  @HopMetadataProperty
  private boolean includingParameters = true;

  @GuiWidgetElement(
      id = "1020-action-doc-dialog-include-notes",
      parentId = GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::ActionDoc.Notes.Label")
  @HopMetadataProperty
  private boolean includingNotes;

  @GuiWidgetElement(
      id = "1030-action-doc-dialog-include-metadata",
      parentId = GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::ActionDoc.Metadata.Label")
  @HopMetadataProperty
  private boolean includingMetadata = true;

  public ActionDoc() {}

  public ActionDoc(ActionDoc other) {
    super(other.getName(), other.getDescription(), other.getPluginId());
    this.targetParentFolder = other.targetParentFolder;
    this.includingNotes = other.includingNotes;
    this.includingParameters = other.includingParameters;
  }

  @Override
  public ActionDoc clone() {
    return new ActionDoc(this);
  }

  /**
   * Execute this action and return the result. In this case it means, just set the result boolean
   * in the Result class.
   *
   * @param prevResult The result of the previous execution
   * @return The Result of the execution.
   */
  @Override
  public Result execute(Result prevResult, int nr) {

    MultiMetadataProvider provider;
    if (getMetadataProvider() instanceof MultiMetadataProvider) {
      provider = (MultiMetadataProvider) getMetadataProvider();
    } else {
      provider = new MultiMetadataProvider(getVariables(), getMetadataProvider());
    }

    DocBuilder docBuilder =
        new DocBuilder(
            getLogChannel(),
            this,
            provider,
            getVariable(DocBuilder.VAR_PROJECT_NAME),
            getVariable(DocBuilder.VAR_PROJECT_HOME),
            targetParentFolder,
            includingParameters,
            includingNotes,
            includingMetadata);
    docBuilder.buildDocumentation(prevResult);
    return prevResult;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }
}
