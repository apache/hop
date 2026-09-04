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

package org.apache.hop.pipeline.transforms.transformsmetrics;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "TransformsMetrics,StepsMetrics",
    image = "transformsmetrics.svg",
    name = "i18n::TransformsMetrics.Name",
    description = "i18n::TransformsMetrics.Description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Statistics",
    keywords = "i18n::TransformsMetricsMeta.keyword",
    documentationUrl = "/pipeline/transforms/outputtransformmetrics.html")
@GuiPlugin
public class TransformsMetricsMeta
    extends BaseTransformMeta<TransformsMetrics, TransformsMetricsData> {
  private static final Class<?> PKG = TransformsMetricsMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "TRANSFORMS_METRICS_DIALOG_OPTIONS";
  public static final String GROUP_FIELDS = "i18n::TransformsMetricsMeta.Group.Fields";
  public static final String WIDGET_TRANSFORM_NAME_FIELD = "TRANSFORM_NAME_FIELD";
  public static final String WIDGET_TRANSFORM_ID_FIELD = "TRANSFORM_ID_FIELD";
  public static final String WIDGET_LINES_INPUT_FIELD = "LINES_INPUT_FIELD";
  public static final String WIDGET_LINES_OUTPUT_FIELD = "LINES_OUTPUT_FIELD";
  public static final String WIDGET_LINES_READ_FIELD = "LINES_READ_FIELD";
  public static final String WIDGET_LINES_UPDATED_FIELD = "LINES_UPDATED_FIELD";
  public static final String WIDGET_LINES_WRITTEN_FIELD = "LINES_WRITTEN_FIELD";
  public static final String WIDGET_LINES_REJECTED_FIELD = "LINES_REJECTED_FIELD";
  public static final String WIDGET_DURATION_FIELD = "DURATION_FIELD";

  public static final String DEFAULT_TRANSFORM_NAME_FIELD = "Transform name";
  public static final String DEFAULT_TRANSFORM_ID_FIELD = "Transform id";
  public static final String DEFAULT_LINES_INPUT_FIELD = "Lines input";
  public static final String DEFAULT_LINES_OUTPUT_FIELD = "Lines output";
  public static final String DEFAULT_LINES_READ_FIELD = "Lines read";
  public static final String DEFAULT_LINES_UPDATED_FIELD = "Lines updated";
  public static final String DEFAULT_LINES_WRITTEN_FIELD = "Lines written";
  public static final String DEFAULT_LINES_REJECTED_FIELD = "Lines rejected";
  public static final String DEFAULT_DURATION_FIELD = "Duration";

  @HopMetadataProperty(
      groupKey = "transforms",
      key = "transform",
      injectionGroupKey = "TRANSFORMS",
      injectionGroupDescription = "TransformsMetricsMeta.Injection.TRANSFORMS")
  private List<MetricTransform> metricTransforms;

  @GuiWidgetElement(
      id = WIDGET_TRANSFORM_NAME_FIELD,
      order = "0100",
      type = GuiElementType.TEXT,
      label = "i18n::TransformsMetricsMeta.TransformNameField.Label",
      toolTip = "i18n::TransformsMetricsMeta.TransformNameField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_FIELDS,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "transformnamefield",
      injectionKey = "TRANSFORM_NAME_FIELD",
      injectionKeyDescription = "TransformsMetricsMeta.Injection.TRANSFORM_NAME_FIELD")
  private String transformNameField;

  @GuiWidgetElement(
      id = WIDGET_TRANSFORM_ID_FIELD,
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::TransformsMetricsMeta.TransformIdField.Label",
      toolTip = "i18n::TransformsMetricsMeta.TransformIdField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_FIELDS,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "transformidfield",
      injectionKey = "TRANSFORM_ID_FIELD",
      injectionKeyDescription = "TransformsMetricsMeta.Injection.TRANSFORM_ID_FIELD")
  private String transformIdField;

  @GuiWidgetElement(
      id = WIDGET_LINES_INPUT_FIELD,
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::TransformsMetricsMeta.LinesInputField.Label",
      toolTip = "i18n::TransformsMetricsMeta.LinesInputField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_FIELDS,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "transformlinesinputfield",
      injectionKey = "LINES_INPUT_FIELD",
      injectionKeyDescription = "TransformsMetricsMeta.Injection.LINES_INPUT_FIELD")
  private String linesInputField;

  @GuiWidgetElement(
      id = WIDGET_LINES_OUTPUT_FIELD,
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::TransformsMetricsMeta.LinesOutputField.Label",
      toolTip = "i18n::TransformsMetricsMeta.LinesOutputField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_FIELDS,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "transformlinesoutputfield",
      injectionKey = "LINES_OUTPUT_FIELD",
      injectionKeyDescription = "TransformsMetricsMeta.Injection.LINES_OUTPUT_FIELD")
  private String linesOutputField;

  @GuiWidgetElement(
      id = WIDGET_LINES_READ_FIELD,
      order = "0500",
      type = GuiElementType.TEXT,
      label = "i18n::TransformsMetricsMeta.LinesReadField.Label",
      toolTip = "i18n::TransformsMetricsMeta.LinesReadField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_FIELDS,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "transformlinesreadfield",
      injectionKey = "LINES_READ_FIELD",
      injectionKeyDescription = "TransformsMetricsMeta.Injection.LINES_READ_FIELD")
  private String linesReadField;

  @GuiWidgetElement(
      id = WIDGET_LINES_UPDATED_FIELD,
      order = "0600",
      type = GuiElementType.TEXT,
      label = "i18n::TransformsMetricsMeta.LinesUpdatedField.Label",
      toolTip = "i18n::TransformsMetricsMeta.LinesUpdatedField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_FIELDS,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "transformlinesupdatedfield",
      injectionKey = "LINES_UPDATED_FIELD",
      injectionKeyDescription = "TransformsMetricsMeta.Injection.LINES_UPDATED_FIELD")
  private String linesUpdatedField;

  @GuiWidgetElement(
      id = WIDGET_LINES_WRITTEN_FIELD,
      order = "0700",
      type = GuiElementType.TEXT,
      label = "i18n::TransformsMetricsMeta.LinesWrittenField.Label",
      toolTip = "i18n::TransformsMetricsMeta.LinesWrittenField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_FIELDS,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "transformlineswrittenfield",
      injectionKey = "LINES_WRITTEN_FIELD",
      injectionKeyDescription = "TransformsMetricsMeta.Injection.LINES_WRITTEN_FIELD")
  private String linesWrittenField;

  @GuiWidgetElement(
      id = WIDGET_LINES_REJECTED_FIELD,
      order = "0800",
      type = GuiElementType.TEXT,
      label = "i18n::TransformsMetricsMeta.LinesRejectedField.Label",
      toolTip = "i18n::TransformsMetricsMeta.LinesRejectedField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_FIELDS,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "transformlineserrorsfield",
      injectionKey = "LINES_REJECTED_FIELD",
      injectionKeyDescription = "TransformsMetricsMeta.Injection.LINES_REJECTED_FIELD")
  private String linesRejectedField;

  @GuiWidgetElement(
      id = WIDGET_DURATION_FIELD,
      order = "0900",
      type = GuiElementType.TEXT,
      label = "i18n::TransformsMetricsMeta.DurationField.Label",
      toolTip = "i18n::TransformsMetricsMeta.DurationField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      group = GROUP_FIELDS,
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS)
  @HopMetadataProperty(
      key = "transformsecondsfield",
      injectionKey = "DURATION_FIELD",
      injectionKeyDescription = "TransformsMetricsMeta.Injection.DURATION_FIELD")
  private String durationField;

  public TransformsMetricsMeta() {
    metricTransforms = new ArrayList<>();
  }

  @Override
  public void setDefault() {
    metricTransforms = new ArrayList<>();
    transformNameField = DEFAULT_TRANSFORM_NAME_FIELD;
    transformIdField = DEFAULT_TRANSFORM_ID_FIELD;
    linesInputField = DEFAULT_LINES_INPUT_FIELD;
    linesOutputField = DEFAULT_LINES_OUTPUT_FIELD;
    linesReadField = DEFAULT_LINES_READ_FIELD;
    linesUpdatedField = DEFAULT_LINES_UPDATED_FIELD;
    linesWrittenField = DEFAULT_LINES_WRITTEN_FIELD;
    linesRejectedField = DEFAULT_LINES_REJECTED_FIELD;
    durationField = DEFAULT_DURATION_FIELD;
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    inputRowMeta.clear();
    addStringField(inputRowMeta, variables.resolve(transformNameField), name);
    addStringField(inputRowMeta, variables.resolve(transformIdField), name);
    addIntegerField(inputRowMeta, variables.resolve(linesInputField), name);
    addIntegerField(inputRowMeta, variables.resolve(linesOutputField), name);
    addIntegerField(inputRowMeta, variables.resolve(linesReadField), name);
    addIntegerField(inputRowMeta, variables.resolve(linesUpdatedField), name);
    addIntegerField(inputRowMeta, variables.resolve(linesWrittenField), name);
    addIntegerField(inputRowMeta, variables.resolve(linesRejectedField), name);
    addIntegerField(inputRowMeta, variables.resolve(durationField), name);
  }

  private void addStringField(IRowMeta rowMeta, String fieldName, String origin) {
    if (StringUtils.isBlank(fieldName)) {
      return;
    }
    ValueMetaString valueMeta = new ValueMetaString(fieldName);
    valueMeta.setOrigin(origin);
    rowMeta.addValueMeta(valueMeta);
  }

  private void addIntegerField(IRowMeta rowMeta, String fieldName, String origin) {
    if (StringUtils.isBlank(fieldName)) {
      return;
    }
    ValueMetaInteger valueMeta =
        new ValueMetaInteger(fieldName, IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
    valueMeta.setOrigin(origin);
    rowMeta.addValueMeta(valueMeta);
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
    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "TransformsMetricsMeta.CheckResult.IncomingHopsNotSupported"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "TransformsMetricsMeta.CheckResult.NoIncomingHops"),
              transformMeta));
    }

    if (metricTransforms == null || metricTransforms.isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "TransformsMetricsMeta.CheckResult.NoTransformsEntered"),
              transformMeta));
      return;
    }

    boolean allFound = true;
    for (MetricTransform metricTransform : metricTransforms) {
      if (StringUtils.isEmpty(metricTransform.getName())) {
        continue;
      }
      if (pipelineMeta.findTransform(metricTransform.getName()) == null) {
        allFound = false;
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_WARNING,
                BaseMessages.getString(
                    PKG,
                    "TransformsMetricsMeta.CheckResult.TransformNotFound",
                    metricTransform.getName()),
                transformMeta));
      }
    }
    if (allFound) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "TransformsMetricsMeta.CheckResult.AllTransformsFound"),
              transformMeta));
    }
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {PipelineType.Normal};
  }
}
