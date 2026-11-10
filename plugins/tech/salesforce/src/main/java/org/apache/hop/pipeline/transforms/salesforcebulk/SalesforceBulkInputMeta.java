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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.salesforce.SalesforceConnection;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@GuiPlugin
@Transform(
    id = "SalesforceBulkInput",
    name = "i18n::SalesforceBulkInput.Name",
    description = "i18n::SalesforceBulkInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::SalesforceBulkInput.keyword",
    image = "salesforcebulkinput.svg",
    documentationUrl = "/pipeline/transforms/salesforcebulkinput.html",
    excludedEngines = {"Beam*", "SparkPipelineEngine"})
public class SalesforceBulkInputMeta
    extends BaseTransformMeta<SalesforceBulkInput, SalesforceBulkInputData> {
  private static final Class<?> PKG = SalesforceBulkInputMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "SalesforceBulkInput.Dialog";
  private static final String GROUP_CONNECTION = "i18n::SalesforceBulkInput.Group.Connection";
  private static final String GROUP_QUERY = "i18n::SalesforceBulkInput.Group.Query";
  private static final String GROUP_FIELDS = "i18n::SalesforceBulkInput.Group.Fields";

  @GuiWidgetElement(
      id = "connection",
      order = "100",
      type = GuiElementType.METADATA,
      metadata = SalesforceConnection.class,
      label = "i18n::SalesforceBulkInput.Connection.Label",
      toolTip = "i18n::SalesforceBulkInput.Connection.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONNECTION,
      groupOrder = "100")
  @HopMetadataProperty(key = "connection")
  private String connection;

  @GuiWidgetElement(
      id = "operation",
      order = "100",
      type = GuiElementType.COMBO,
      label = "i18n::SalesforceBulkInput.Operation.Label",
      toolTip = "i18n::SalesforceBulkInput.Operation.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_QUERY,
      groupOrder = "200")
  @HopMetadataProperty(key = "operation", storeWithCode = true)
  private SalesforceBulkQueryOperation operation;

  @GuiWidgetElement(
      id = "soql",
      order = "200",
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 8,
      label = "i18n::SalesforceBulkInput.Soql.Label",
      toolTip = "i18n::SalesforceBulkInput.Soql.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_QUERY,
      groupOrder = "200")
  @HopMetadataProperty(key = "soql")
  private String soql;

  @GuiWidgetElement(
      id = "timeout",
      order = "300",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkInput.Timeout.Label",
      toolTip = "i18n::SalesforceBulkInput.Timeout.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_QUERY,
      groupOrder = "200")
  @HopMetadataProperty(key = "timeout")
  private String timeout;

  @GuiWidgetElement(
      id = "pollInterval",
      order = "400",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkInput.PollInterval.Label",
      toolTip = "i18n::SalesforceBulkInput.PollInterval.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_QUERY,
      groupOrder = "200")
  @HopMetadataProperty(key = "pollInterval")
  private String pollInterval;

  @GuiWidgetElement(
      id = "maxRows",
      order = "500",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkInput.MaxRows.Label",
      toolTip = "i18n::SalesforceBulkInput.MaxRows.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_QUERY,
      groupOrder = "200")
  @HopMetadataProperty(key = "maxRows")
  private String maxRows;

  @GuiWidgetElement(
      id = "jobIdField",
      order = "600",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkInput.JobIdField.Label",
      toolTip = "i18n::SalesforceBulkInput.JobIdField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_QUERY,
      groupOrder = "200")
  @HopMetadataProperty(key = "jobIdField")
  private String jobIdField;

  @GuiWidgetElement(
      id = "fields",
      order = "100",
      type = GuiElementType.TABLE,
      tableRows = 8,
      toolTip = "i18n::SalesforceBulkInput.Fields.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FIELDS,
      groupOrder = "300")
  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<SalesforceBulkInputField> fields;

  public SalesforceBulkInputMeta() {
    setDefault();
  }

  @Override
  public void setDefault() {
    connection = "";
    operation = SalesforceBulkQueryOperation.QUERY;
    soql = "SELECT Id, Name FROM Account";
    timeout = "3600";
    pollInterval = "10";
    maxRows = "0";
    jobIdField = "";
    fields = new ArrayList<>();
  }

  public List<String> hopTypeNames(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return List.of("String", "Integer", "Number", "BigNumber", "Date", "Timestamp", "Boolean");
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    rowMeta.clear();
    if (fields != null) {
      for (SalesforceBulkInputField field : fields) {
        if (field == null || Utils.isEmpty(field.getName())) {
          continue;
        }
        String fieldName = variables.resolve(field.getName());
        String typeName = Utils.isEmpty(field.getType()) ? "String" : field.getType();
        try {
          IValueMeta value =
              ValueMetaFactory.createValueMeta(
                  fieldName, ValueMetaFactory.getIdForValueMeta(typeName));
          value.setOrigin(name);
          rowMeta.addValueMeta(value);
        } catch (org.apache.hop.core.exception.HopPluginException e) {
          throw new HopTransformException(e);
        }
      }
    }
    if (variables != null && !Utils.isEmpty(variables.resolve(jobIdField))) {
      ValueMetaString job = new ValueMetaString(variables.resolve(jobIdField));
      job.setOrigin(name);
      rowMeta.addValueMeta(job);
    }
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
    if (Utils.isEmpty(variables.resolve(connection))) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceBulkInput.Check.NoConnection"),
              transformMeta));
    }
    if (Utils.isEmpty(variables.resolve(soql))) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceBulkInput.Check.NoSoql"),
              transformMeta));
    }
    if (fields == null || fields.isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceBulkInput.Check.NoFields"),
              transformMeta));
    }
  }
}
