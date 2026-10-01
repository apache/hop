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

package org.apache.hop.testing.transforms.datasetoutput;

import static org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_OK;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.DataSet;

@Getter
@Setter
@Transform(
    id = "DataSetOutput",
    description = "Write rows to a data set defined in the metadata",
    name = "Data set output",
    image = "write-to-dataset.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::DataSetOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/datasetoutput.html")
@GuiPlugin
public class DataSetOutputMeta extends BaseTransformMeta<DataSetOutput, DataSetOutputData> {
  private static final Class<?> PKG = DataSetOutputMeta.class;
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "DATA_SET_OUTPUT_DIALOG_OPTIONS";
  public static final String WIDGET_DATA_SET_NAME = "dataSetName";
  public static final String WIDGET_FOLDER_NAME = "folderName";
  public static final String WIDGET_CSV_FILENAME = "csvFilename";
  public static final String WIDGET_RECREATE = "recreateDataSet";
  public static final String WIDGET_VALIDATE = "validateDataSet";

  @GuiWidgetElement(
      id = WIDGET_DATA_SET_NAME,
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = DataSet.class,
      label = "i18n::DataSetOutputMeta.DataSetName.Label",
      toolTip = "i18n::DataSetOutputMeta.DataSetName.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Data Set")
  @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.PIPELINE_DATA_SET)
  private String dataSetName;

  @GuiWidgetElement(
      id = WIDGET_FOLDER_NAME,
      order = "0200",
      type = GuiElementType.FOLDER,
      label = "i18n::DataSetOutputMeta.FolderName.Label",
      toolTip = "i18n::DataSetOutputMeta.FolderName.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Data Set")
  @HopMetadataProperty
  private String folderName;

  @GuiWidgetElement(
      id = WIDGET_CSV_FILENAME,
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::DataSetOutputMeta.CsvFilename.Label",
      toolTip = "i18n::DataSetOutputMeta.CsvFilename.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Data Set")
  @HopMetadataProperty
  private String csvFilename;

  @GuiWidgetElement(
      id = WIDGET_RECREATE,
      order = "0400",
      type = GuiElementType.CHECKBOX,
      label = "i18n::DataSetOutputMeta.Recreate.Label",
      toolTip = "i18n::DataSetOutputMeta.Recreate.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Data Set")
  @HopMetadataProperty
  private boolean recreateDataSet;

  @GuiWidgetElement(
      id = WIDGET_VALIDATE,
      order = "0500",
      type = GuiElementType.CHECKBOX,
      label = "i18n::DataSetOutputMeta.Validate.Label",
      toolTip = "i18n::DataSetOutputMeta.Validate.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Data Set")
  @HopMetadataProperty
  private boolean validateDataSet;

  public DataSetOutputMeta() {
    super();
    this.recreateDataSet = true;
  }

  @Override
  public void setDefault() {
    recreateDataSet = true;
    validateDataSet = false;
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
    if (StringUtils.isEmpty(dataSetName)) {
      remarks.add(
          new CheckResult(
              TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DataSetOutputMeta.CheckResult.DataSetNameMissing"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DataSetOutputMeta.CheckResult.DataSetNameOK"),
              transformMeta));
    }

    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "DataSetOutputMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DataSetOutputMeta.CheckResult.NoInputReceived"),
              transformMeta));
    }
  }
}
