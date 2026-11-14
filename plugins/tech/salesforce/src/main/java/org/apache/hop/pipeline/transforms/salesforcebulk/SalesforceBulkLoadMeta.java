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
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
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
    id = "SalesforceBulkLoad",
    name = "i18n::SalesforceBulkLoad.Name",
    description = "i18n::SalesforceBulkLoad.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::SalesforceBulkLoad.keyword",
    image = "salesforcebulkload.svg",
    documentationUrl = "/pipeline/transforms/salesforcebulkload.html",
    excludedEngines = {"Beam*", "SparkPipelineEngine"})
public class SalesforceBulkLoadMeta
    extends BaseTransformMeta<SalesforceBulkLoad, SalesforceBulkLoadData> {
  private static final Class<?> PKG = SalesforceBulkLoadMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "SalesforceBulkLoad.Dialog";
  private static final String GROUP_CONNECTION = "i18n::SalesforceBulkLoad.Group.Connection";
  private static final String GROUP_OPERATION = "i18n::SalesforceBulkLoad.Group.Operation";
  private static final String GROUP_FIELDS = "i18n::SalesforceBulkLoad.Group.Fields";

  @GuiWidgetElement(
      id = "connection",
      order = "100",
      type = GuiElementType.METADATA,
      metadata = SalesforceConnection.class,
      label = "i18n::SalesforceBulkLoad.Connection.Label",
      toolTip = "i18n::SalesforceBulkLoad.Connection.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONNECTION,
      groupOrder = "100")
  @HopMetadataProperty(key = "connection")
  private String connection;

  @GuiWidgetElement(
      id = "salesforceObject",
      order = "100",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkLoad.Object.Label",
      toolTip = "i18n::SalesforceBulkLoad.Object.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "object")
  private String salesforceObject;

  @GuiWidgetElement(
      id = "operation",
      order = "200",
      type = GuiElementType.COMBO,
      label = "i18n::SalesforceBulkLoad.Operation.Label",
      toolTip = "i18n::SalesforceBulkLoad.Operation.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "operation", storeWithCode = true)
  private SalesforceBulkOperation operation;

  @GuiWidgetElement(
      id = "externalIdField",
      order = "300",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkLoad.ExternalId.Label",
      toolTip = "i18n::SalesforceBulkLoad.ExternalId.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "externalIdField")
  private String externalIdField;

  @GuiWidgetElement(
      id = "concurrency",
      order = "400",
      type = GuiElementType.COMBO,
      label = "i18n::SalesforceBulkLoad.Concurrency.Label",
      toolTip = "i18n::SalesforceBulkLoad.Concurrency.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "concurrency", storeWithCode = true)
  private SalesforceBulkConcurrency concurrency;

  @GuiWidgetElement(
      id = "lineEnding",
      order = "500",
      type = GuiElementType.COMBO,
      label = "i18n::SalesforceBulkLoad.LineEnding.Label",
      toolTip = "i18n::SalesforceBulkLoad.LineEnding.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "lineEnding", storeWithCode = true)
  private SalesforceBulkLineEnding lineEnding;

  @GuiWidgetElement(
      id = "delimiter",
      order = "600",
      type = GuiElementType.COMBO,
      label = "i18n::SalesforceBulkLoad.Delimiter.Label",
      toolTip = "i18n::SalesforceBulkLoad.Delimiter.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "delimiter", storeWithCode = true)
  private SalesforceBulkDelimiter delimiter;

  @GuiWidgetElement(
      id = "timeout",
      order = "700",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkLoad.Timeout.Label",
      toolTip = "i18n::SalesforceBulkLoad.Timeout.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "timeout")
  private String timeout;

  @GuiWidgetElement(
      id = "pollInterval",
      order = "800",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkLoad.PollInterval.Label",
      toolTip = "i18n::SalesforceBulkLoad.PollInterval.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "pollInterval")
  private String pollInterval;

  @GuiWidgetElement(
      id = "maxBytes",
      order = "900",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkLoad.MaxBytes.Label",
      toolTip = "i18n::SalesforceBulkLoad.MaxBytes.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "maxBytes")
  private String maxBytes;

  @GuiWidgetElement(
      id = "salesforceIdField",
      order = "1000",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkLoad.IdField.Label",
      toolTip = "i18n::SalesforceBulkLoad.IdField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "salesforceIdField")
  private String salesforceIdField;

  @GuiWidgetElement(
      id = "jobIdField",
      order = "1100",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkLoad.JobIdField.Label",
      toolTip = "i18n::SalesforceBulkLoad.JobIdField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "jobIdField")
  private String jobIdField;

  @GuiWidgetElement(
      id = "successField",
      order = "1200",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkLoad.SuccessField.Label",
      toolTip = "i18n::SalesforceBulkLoad.SuccessField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "successField")
  private String successField;

  @GuiWidgetElement(
      id = "errorField",
      order = "1300",
      type = GuiElementType.TEXT,
      label = "i18n::SalesforceBulkLoad.ErrorField.Label",
      toolTip = "i18n::SalesforceBulkLoad.ErrorField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPERATION,
      groupOrder = "200")
  @HopMetadataProperty(key = "errorField")
  private String errorField;

  @GuiWidgetElement(
      id = "fields",
      order = "100",
      type = GuiElementType.TABLE,
      tableRows = 8,
      toolTip = "i18n::SalesforceBulkLoad.Fields.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FIELDS,
      groupOrder = "300")
  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<SalesforceBulkField> fields;

  public SalesforceBulkLoadMeta() {
    setDefault();
  }

  @Override
  public void setDefault() {
    connection = "";
    salesforceObject = "Account";
    operation = SalesforceBulkOperation.INSERT;
    externalIdField = "";
    concurrency = SalesforceBulkConcurrency.PARALLEL;
    lineEnding = SalesforceBulkLineEnding.LF;
    delimiter = SalesforceBulkDelimiter.COMMA;
    timeout = "3600";
    pollInterval = "10";
    maxBytes = "104857600";
    salesforceIdField = "SalesforceId";
    jobIdField = "SalesforceJobId";
    successField = "SalesforceSuccess";
    errorField = "SalesforceError";
    fields = new ArrayList<>();
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    add(rowMeta, variables, name, salesforceIdField, false);
    add(rowMeta, variables, name, jobIdField, false);
    add(rowMeta, variables, name, successField, true);
    add(rowMeta, variables, name, errorField, false);
  }

  private static void add(
      IRowMeta rowMeta, IVariables variables, String origin, String fieldName, boolean bool) {
    if (variables == null || Utils.isEmpty(variables.resolve(fieldName))) {
      return;
    }
    String resolved = variables.resolve(fieldName);
    if (bool) {
      ValueMetaBoolean value = new ValueMetaBoolean(resolved);
      value.setOrigin(origin);
      rowMeta.addValueMeta(value);
    } else {
      ValueMetaString value = new ValueMetaString(resolved);
      value.setOrigin(origin);
      rowMeta.addValueMeta(value);
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
    if (input == null || input.length == 0) {
      error(remarks, transformMeta, "SalesforceBulkLoad.Check.NoInput");
    }
    if (Utils.isEmpty(variables.resolve(connection))) {
      error(remarks, transformMeta, "SalesforceBulkLoad.Check.NoConnection");
    }
    if (Utils.isEmpty(variables.resolve(salesforceObject))) {
      error(remarks, transformMeta, "SalesforceBulkLoad.Check.NoObject");
    }
    if (fields == null || fields.isEmpty()) {
      error(remarks, transformMeta, "SalesforceBulkLoad.Check.NoFields");
    } else if (prev != null) {
      for (SalesforceBulkField field : fields) {
        if (field == null || Utils.isEmpty(field.getStreamField())) {
          error(remarks, transformMeta, "SalesforceBulkLoad.Check.BlankField");
          continue;
        }
        if (prev.indexOfValue(variables.resolve(field.getStreamField())) < 0) {
          error(
              remarks,
              transformMeta,
              "SalesforceBulkLoad.Check.MissingField",
              field.getStreamField());
        }
      }
    }
    if (operation == SalesforceBulkOperation.UPSERT
        && Utils.isEmpty(variables.resolve(externalIdField))) {
      error(remarks, transformMeta, "SalesforceBulkLoad.Check.NoExternalId");
    }
  }

  private void error(
      List<ICheckResult> remarks, TransformMeta transformMeta, String key, String... args) {
    remarks.add(
        new CheckResult(
            ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, key, args), transformMeta));
  }
}
