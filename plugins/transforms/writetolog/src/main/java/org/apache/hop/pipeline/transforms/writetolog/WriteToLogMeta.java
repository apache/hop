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

package org.apache.hop.pipeline.transforms.writetolog;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "WriteToLog",
    image = "writetolog.svg",
    name = "i18n::WriteToLog.Name",
    description = "i18n::WriteToLog.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::WriteToLog.Keyword",
    documentationUrl = "/pipeline/transforms/writetolog.html")
@GuiPlugin
@Getter
@Setter
public class WriteToLogMeta extends BaseTransformMeta<WriteToLog, WriteToLogData> {
  private static final Class<?> PKG = WriteToLogMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "WRITE_TO_LOG_DIALOG_OPTIONS";

  public static final String GROUP_OPTIONS = "i18n::WriteToLog.Tab.Options";
  public static final String GROUP_OPTIONS_ORDER = "0100";
  public static final String GROUP_MESSAGE = "i18n::WriteToLog.Tab.Message";
  public static final String GROUP_MESSAGE_ORDER = "0200";

  public static final String WIDGET_LOG_LEVEL = "logLevel";
  public static final String WIDGET_DISPLAY_HEADER = "displayHeader";
  public static final String WIDGET_LIMIT_ROWS = "limitRows";
  public static final String WIDGET_LIMIT_ROWS_NUMBER = "limitRowsNumber";

  @GuiWidgetElement(
      id = WIDGET_DISPLAY_HEADER,
      order = "0200",
      type = GuiElementType.CHECKBOX,
      label = "i18n::WriteToLogMeta.DisplayHeader.Label",
      toolTip = "i18n::WriteToLogMeta.DisplayHeader.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPTIONS,
      groupOrder = GROUP_OPTIONS_ORDER)
  @HopMetadataProperty(
      key = "displayHeader",
      injectionKey = "DISPLAY_HEADER",
      injectionKeyDescription = "WriteToLogMeta.Injection.DisplayHeader")
  private boolean displayHeader;

  @GuiWidgetElement(
      id = WIDGET_LIMIT_ROWS,
      order = "0300",
      type = GuiElementType.CHECKBOX,
      label = "i18n::WriteToLogMeta.LimitRows.Label",
      toolTip = "i18n::WriteToLogMeta.LimitRows.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPTIONS,
      groupOrder = GROUP_OPTIONS_ORDER)
  @HopMetadataProperty(
      key = "limitRows",
      injectionKey = "LIMIT_ROWS",
      injectionKeyDescription = "WriteToLogMeta.Injection.LimitRows")
  private boolean limitRows;

  @GuiWidgetElement(
      id = WIDGET_LIMIT_ROWS_NUMBER,
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::WriteToLogMeta.LimitRowsNumber.Label",
      toolTip = "i18n::WriteToLogMeta.LimitRowsNumber.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPTIONS,
      groupOrder = GROUP_OPTIONS_ORDER)
  @HopMetadataProperty(
      key = "limitRowsNumber",
      injectionKey = "LIMIT_ROWS_NUMBER",
      injectionKeyDescription = "WriteToLogMeta.Injection.LimitRowsNumber")
  private int limitRowsNumber;

  @HopMetadataProperty(
      key = "logmessage",
      injectionKey = "LOG_MESSAGE",
      injectionKeyDescription = "WriteToLogMeta.Injection.LogMessage")
  private String logMessage;

  /** The log level with which the message should be logged. */
  @HopMetadataProperty(
      key = "loglevel",
      storeWithCode = true,
      injectionKeyDescription = "WriteToLogMeta.Injection.LogLevel")
  private LogLevel logLevel;

  /** The fields which should be to logged. */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupDescription = "WriteToLogMeta.Injection.Fields",
      injectionKeyDescription = "WriteToLogMeta.Injection.Field")
  private List<LogField> logFields = new ArrayList<>();

  public WriteToLogMeta() {
    super();
  }

  /** Added for backwards compatibility with older prefixed log level values. */
  @Override
  public void convertLegacyXml(Node node) throws HopException {
    if (node == null) {
      return;
    }

    String loglevel = XmlHandler.getTagValue(node, "loglevel");
    if (loglevel != null && loglevel.startsWith("log_level_")) {
      switch (loglevel) {
        case "log_level_nothing":
          logLevel = LogLevel.NOTHING;
          break;
        case "log_level_error":
          logLevel = LogLevel.ERROR;
          break;
        case "log_level_minimal":
          logLevel = LogLevel.MINIMAL;
          break;
        case "log_level_basic":
          logLevel = LogLevel.BASIC;
          break;
        case "log_level_detailed":
          logLevel = LogLevel.DETAILED;
          break;
        case "log_level_debug":
          logLevel = LogLevel.DEBUG;
          break;
        case "log_level_rowlevel":
          logLevel = LogLevel.ROWLEVEL;
          break;
        default:
          break;
      }
    }
  }

  @Override
  public void setDefault() {
    displayHeader = true;
    logLevel = LogLevel.BASIC;
    logMessage = "";
    logFields = new ArrayList<>();
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    if (prev == null || prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "WriteToLogMeta.CheckResult.NotReceivingFields"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "WriteToLogMeta.CheckResult.TransformRecevingData", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      StringBuilder errorMessage = new StringBuilder();
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (LogField field : logFields) {
        int idx = prev.indexOfValue(field.getName());
        if (idx < 0) {
          errorMessage.append("\t\t" + field.getName() + Const.CR);
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage.append(
            BaseMessages.getString(PKG, "WriteToLogMeta.CheckResult.FieldsFound", errorMessage));

        cr =
            new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
        remarks.add(cr);
      } else {
        if (logFields.isEmpty()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_WARNING,
                  BaseMessages.getString(PKG, "WriteToLogMeta.CheckResult.NoFieldsEntered"),
                  transformMeta);

          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "WriteToLogMeta.CheckResult.AllFieldsFound"),
                  transformMeta);

          remarks.add(cr);
        }
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "WriteToLogMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "WriteToLogMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }
}
