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

package org.apache.hop.workflow.actions.deleteexecutioninfo;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/**
 * Deletes stored execution information from one location. Put it on a schedule to drop runs that
 * are no longer worth keeping. The location metadata itself is left in place.
 */
@Action(
    id = "DELETE_EXECUTION_INFO",
    name = "i18n::ActionDeleteExecutionInfo.Name",
    description = "i18n::ActionDeleteExecutionInfo.Description",
    image = "delete.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionDeleteExecutionInfo.keyword",
    documentationUrl = "/workflow/actions/deleteexecutioninfo.html")
@GuiPlugin
@Getter
@Setter
public class ActionDeleteExecutionInfo extends ActionBase implements IAction {
  private static final Class<?> PKG = ActionDeleteExecutionInfo.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "DeleteExecutionInfo.Dialog.Options";
  public static final String GROUP_OPTIONS = "i18n::ActionDeleteExecutionInfo.Group.Options";

  @GuiWidgetElement(
      id = "locationName",
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = ExecutionInfoLocation.class,
      label = "i18n::ActionDeleteExecutionInfo.Location.Label",
      toolTip = "i18n::ActionDeleteExecutionInfo.Location.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "location_name")
  private String locationName;

  @GuiWidgetElement(
      id = "age",
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::ActionDeleteExecutionInfo.Age.Label",
      toolTip = "i18n::ActionDeleteExecutionInfo.Age.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "age")
  private String age;

  @GuiWidgetElement(
      id = "unit",
      order = "0300",
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::ActionDeleteExecutionInfo.Unit.Label",
      toolTip = "i18n::ActionDeleteExecutionInfo.Unit.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "unit")
  private ExecutionInfoAgeUnit unit;

  @GuiWidgetElement(
      id = "deleteAll",
      order = "0400",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ActionDeleteExecutionInfo.DeleteAll.Label",
      toolTip = "i18n::ActionDeleteExecutionInfo.DeleteAll.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_OPTIONS,
      groupType = GuiWidgetGroupType.BOXES)
  @HopMetadataProperty(key = "delete_all")
  private boolean deleteAll;

  public ActionDeleteExecutionInfo() {
    this("");
  }

  public ActionDeleteExecutionInfo(String name) {
    super(name, "");
    this.age = "30";
    this.unit = ExecutionInfoAgeUnit.DAYS;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult == null ? new Result() : previousResult;
    result.setResult(false);
    IExecutionInfoLocation iLocation = null;
    try {
      String resolvedName = resolve(locationName);
      if (StringUtils.isEmpty(resolvedName)) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Error.NoLocation"));
      }
      if (getMetadataProvider() == null) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Error.NoMetadata"));
      }
      IHopMetadataSerializer<ExecutionInfoLocation> serializer =
          getMetadataProvider().getSerializer(ExecutionInfoLocation.class);
      ExecutionInfoLocation location = serializer.load(resolvedName);
      if (location == null || location.getExecutionInfoLocation() == null) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Error.Missing", resolvedName));
      }
      iLocation = location.getExecutionInfoLocation();
      iLocation.initialize(this, getMetadataProvider());
      Date cutoff = deleteAll ? null : cutoff();
      int deleted = iLocation.deleteExecutions(cutoff);
      logBasic(
          BaseMessages.getString(
              PKG, "ActionDeleteExecutionInfo.Deleted", Integer.toString(deleted), resolvedName));
      result.setNrLinesDeleted(deleted);
      result.setResult(true);
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Error.Failed"), e);
      result.setNrErrors(result.getNrErrors() + 1);
      result.setResult(false);
    } finally {
      if (iLocation != null) {
        try {
          iLocation.close();
        } catch (Exception e) {
          logError(BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Error.Close"), e);
        }
      }
    }
    return result;
  }

  /** Start dates strictly before this instant are deleted. */
  Date cutoff() throws HopException {
    String resolved = resolve(age);
    long amount;
    try {
      amount = Long.parseLong(resolved == null ? "" : resolved.trim());
    } catch (NumberFormatException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Error.Age", resolved));
    }
    if (amount <= 0) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Error.Age", resolved));
    }
    ExecutionInfoAgeUnit activeUnit = unit == null ? ExecutionInfoAgeUnit.DAYS : unit;
    LocalDateTime now = LocalDateTime.now();
    LocalDateTime cutoffTime =
        switch (activeUnit) {
          case MINUTES -> now.minusMinutes(amount);
          case HOURS -> now.minusHours(amount);
          case DAYS -> now.minusDays(amount);
          case WEEKS -> now.minusWeeks(amount);
          case MONTHS -> now.minusMonths(amount);
        };
    return Date.from(cutoffTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (StringUtils.isEmpty(variables.resolve(locationName))) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Error.NoLocation"),
              this));
    }
    if (deleteAll) {
      return;
    }
    String resolved = variables.resolve(age);
    // A variable that is not set yet can only be judged when the action runs.
    if (StringUtils.isEmpty(resolved)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Error.Age", resolved),
              this));
      return;
    }
    if (resolved.contains("${")) {
      return;
    }
    try {
      if (Long.parseLong(resolved.trim()) <= 0) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Error.Age", resolved),
                this));
      }
    } catch (NumberFormatException e) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ActionDeleteExecutionInfo.Error.Age", resolved),
              this));
    }
  }
}
