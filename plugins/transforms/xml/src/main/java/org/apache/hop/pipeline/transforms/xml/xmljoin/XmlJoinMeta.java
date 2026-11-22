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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

/** This class knows how to handle the MetaData for the XML join transform */
@Getter
@Setter
@Transform(
    id = "XMLJoin",
    image = "XJN.svg",
    name = "i18n::XmlJoin.name",
    description = "i18n::XmlJoin.description",
    categoryDescription = "i18n::XmlJoin.category",
    keywords = "i18n::XmlJoinMeta.keyword",
    documentationUrl = "/pipeline/transforms/xmljoin.html",
    // Must match SparkConst.PLUGIN_ID — do not import engines-spark from transform modules.
    excludedEngines = {"SparkPipelineEngine"})
@GuiPlugin
public class XmlJoinMeta extends BaseTransformMeta<XmlJoin, XmlJoinData> {
  private static final Class<?> PKG = XmlJoinMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "XML_JOIN_DIALOG_OPTIONS";

  public static final String WIDGET_TARGET_XML_TRANSFORM = "XML_JOIN_TARGET_XML_TRANSFORM";
  public static final String WIDGET_TARGET_XML_FIELD = "XML_JOIN_TARGET_XML_FIELD";
  public static final String WIDGET_SOURCE_XML_TRANSFORM = "XML_JOIN_SOURCE_XML_TRANSFORM";
  public static final String WIDGET_SOURCE_XML_FIELD = "XML_JOIN_SOURCE_XML_FIELD";
  public static final String WIDGET_TARGET_XPATH = "XML_JOIN_TARGET_XPATH";
  public static final String WIDGET_COMPLEX_JOIN = "XML_JOIN_COMPLEX_JOIN";
  public static final String WIDGET_JOIN_COMPARE_FIELD = "XML_JOIN_JOIN_COMPARE_FIELD";
  public static final String WIDGET_VALUE_XML_FIELD = "XML_JOIN_VALUE_XML_FIELD";
  public static final String WIDGET_ENCODING = "XML_JOIN_ENCODING";
  public static final String WIDGET_OMIT_XML_HEADER = "XML_JOIN_OMIT_XML_HEADER";
  public static final String WIDGET_OMIT_NULL_VALUES = "XML_JOIN_OMIT_NULL_VALUES";

  private static final String GROUP_TARGET = "i18n::XmlJoin.TargetGroup.Label";
  private static final String GROUP_TARGET_ORDER = "100";
  private static final String GROUP_SOURCE = "i18n::XmlJoin.SourceGroup.Label";
  private static final String GROUP_SOURCE_ORDER = "200";
  private static final String GROUP_JOIN = "i18n::XmlJoin.JoinGroup.Label";
  private static final String GROUP_JOIN_ORDER = "300";
  private static final String GROUP_RESULT = "i18n::XmlJoin.ResultGroup.Label";
  private static final String GROUP_RESULT_ORDER = "400";

  /**
   * Flag: execute complex join. Each element goes under the node of its key: ? in the XPath
   * statement is replaced by the key field of each row.
   */
  @GuiWidgetElement(
      id = WIDGET_COMPLEX_JOIN,
      order = "0305",
      type = GuiElementType.CHECKBOX,
      label = "i18n::XmlJoin.ComplexJoin.Label",
      toolTip = "i18n::XmlJoin.ComplexJoin.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_JOIN,
      groupOrder = GROUP_JOIN_ORDER)
  @HopMetadataProperty(injectionKey = "COMPLEX_JOIN")
  private boolean complexJoin;

  /** What transform holds the XML string to join into */
  @GuiWidgetElement(
      id = WIDGET_TARGET_XML_TRANSFORM,
      order = "0110",
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::XmlJoin.TargetXMLTransform.Label",
      toolTip = "i18n::XmlJoin.TargetXMLTransform.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_TARGET,
      groupOrder = GROUP_TARGET_ORDER)
  @HopMetadataProperty(injectionKey = "TARGET_XML_TRANSFORM")
  private String targetXmlTransform;

  /** What field holds the XML string to join into */
  @GuiWidgetElement(
      id = WIDGET_TARGET_XML_FIELD,
      order = "0120",
      type = GuiElementType.COMBO,
      label = "i18n::XmlJoin.TargetXMLField.Label",
      toolTip = "i18n::XmlJoin.TargetXMLField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_TARGET,
      groupOrder = GROUP_TARGET_ORDER)
  @HopMetadataProperty(injectionKey = "TARGET_XML_FIELD")
  private String targetXmlField;

  /** What field holds the XML tags to join */
  @GuiWidgetElement(
      id = WIDGET_SOURCE_XML_FIELD,
      order = "0220",
      type = GuiElementType.COMBO,
      label = "i18n::XmlJoin.SourceXMLField.Label",
      toolTip = "i18n::XmlJoin.SourceXMLField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_SOURCE,
      groupOrder = GROUP_SOURCE_ORDER)
  @HopMetadataProperty(injectionKey = "SOURCE_XML_FIELD")
  private String sourceXmlField;

  /** The name value containing the resulting XML fragment */
  @GuiWidgetElement(
      id = WIDGET_VALUE_XML_FIELD,
      order = "0410",
      type = GuiElementType.TEXT,
      label = "i18n::XmlJoin.ValueXMLField.Label",
      toolTip = "i18n::XmlJoin.ValueXMLField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_RESULT,
      groupOrder = GROUP_RESULT_ORDER)
  @HopMetadataProperty(injectionKey = "VALUE_XML_FIELD")
  private String valueXmlField;

  /** The name of the repeating row XML element */
  @GuiWidgetElement(
      id = WIDGET_TARGET_XPATH,
      order = "0310",
      type = GuiElementType.TEXT,
      label = "i18n::XmlJoin.TargetXPath.Label",
      toolTip = "i18n::XmlJoin.TargetXPath.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_JOIN,
      groupOrder = GROUP_JOIN_ORDER)
  @HopMetadataProperty(injectionKey = "TARGET_XPATH")
  private String targetXPath;

  /** What transform holds the XML strings to join */
  @GuiWidgetElement(
      id = WIDGET_SOURCE_XML_TRANSFORM,
      order = "0210",
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::XmlJoin.SourceXMLTransform.Label",
      toolTip = "i18n::XmlJoin.SourceXMLTransform.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_SOURCE,
      groupOrder = GROUP_SOURCE_ORDER)
  @HopMetadataProperty(injectionKey = "SOURCE_XML_TRANSFORM")
  private String sourceXmlTransform;

  /** What field holds the join compare value */
  @GuiWidgetElement(
      id = WIDGET_JOIN_COMPARE_FIELD,
      order = "0330",
      type = GuiElementType.COMBO,
      label = "i18n::XmlJoin.JoinCompareFiled.Label",
      toolTip = "i18n::XmlJoin.JoinCompareField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_JOIN,
      groupOrder = GROUP_JOIN_ORDER)
  @HopMetadataProperty(injectionKey = "JOIN_COMPARE_FIELD")
  private String joinCompareField;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @GuiWidgetElement(
      id = WIDGET_ENCODING,
      order = "0420",
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::XmlJoin.Encoding.Label",
      toolTip = "i18n::XmlJoin.Encoding.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_RESULT,
      groupOrder = GROUP_RESULT_ORDER)
  @HopMetadataProperty(injectionKey = "ENCODING")
  private String encoding;

  /** Flag: execute complex join */
  @GuiWidgetElement(
      id = WIDGET_OMIT_XML_HEADER,
      order = "0430",
      type = GuiElementType.CHECKBOX,
      label = "i18n::XmlJoin.OmitXMLHeader.Label",
      toolTip = "i18n::XmlJoin.OmitXMLHeader.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_RESULT,
      groupOrder = GROUP_RESULT_ORDER)
  @HopMetadataProperty(injectionKey = "OMIT_XML_HEADER")
  private boolean omitXmlHeader;

  /** Flag: omit null values from result xml */
  @GuiWidgetElement(
      id = WIDGET_OMIT_NULL_VALUES,
      order = "0440",
      type = GuiElementType.CHECKBOX,
      label = "i18n::XmlJoin.OmitNullValues.Label",
      toolTip = "i18n::XmlJoin.OmitNullValues.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_RESULT,
      groupOrder = GROUP_RESULT_ORDER)
  @HopMetadataProperty(injectionKey = "OMIT_NULL_VALUES")
  private boolean omitNullValues;

  public XmlJoinMeta() {
    super();
    encoding = Const.UTF_8;
  }

  /**
   * At design time the info streams of this transform have no transform set, so Hop passes an empty
   * row for the target. Look the target transform up by name and use its fields instead.
   */
  @Override
  public void getFields(
      PipelineMeta pipelineMeta,
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (!isKnown(info, 0) && pipelineMeta != null) {
      TransformMeta targetTransform = pipelineMeta.findTransform(getTargetXmlTransform());
      if (targetTransform != null) {
        IRowMeta[] withTarget =
            new IRowMeta[] {
              pipelineMeta.getTransformFields(variables, targetTransform),
              info != null && info.length > 1 ? info[1] : null
            };
        getFields(row, name, withTarget, nextTransform, variables, metadataProvider);
        return;
      }
    }
    getFields(row, name, info, nextTransform, variables, metadataProvider);
  }

  /** An empty row is the row of a stream without a transform: its fields are not known. */
  private static boolean isKnown(IRowMeta[] info, int index) {
    return info != null && info.length > index && info[index] != null && !info[index].isEmpty();
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    // Left empty in the dialog, the field is null; older dialogs stored an empty name
    IValueMeta v = new ValueMetaString(variables.resolve(Const.NVL(this.getValueXmlField(), "")));
    v.setOrigin(name);

    if (isKnown(info, 0)) {
      IRowMeta knownTarget = info[0];
      // The target's row with the joined XML, as processRow() builds it, whatever rows the
      // previous transforms give here
      IRowMeta target = knownTarget.clone();
      row.clear();
      row.addRowMeta(target);
      row.addValueMeta(v);
      return;
    }
    try {
      /*
         Row should only include fields from the target and not the source. During the preview table
         generation the fields from all previous transforms (source and target) are included in the row so let's
         remove the source fields.
      */
      List<String> targetFieldNames = null;
      IRowMeta targetRowMeta = info != null && info.length > 0 ? info[0] : null;
      if (targetRowMeta != null) {
        targetFieldNames = Arrays.asList(targetRowMeta.getFieldNames());
      }
      IRowMeta sourceRowMeta = info != null && info.length > 1 ? info[1] : null;
      if (sourceRowMeta != null) {
        for (String fieldName : sourceRowMeta.getFieldNames()) {
          if (targetFieldNames == null || !targetFieldNames.contains(fieldName)) {
            row.removeValueMeta(fieldName);
          }
        }
      }
    } catch (HopValueException e) {
      // Pass
    }

    row.addValueMeta(v);
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
    // checks for empty field which are required
    if (Utils.isEmpty(this.targetXmlTransform)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.TargetXMLTransformNotSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.TargetXMLTransformSpecified"),
              transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(this.targetXmlField)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.TargetXMLFieldNotSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.TargetXMLFieldSpecified"),
              transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(this.sourceXmlTransform)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.SourceXMLTransformNotSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.SourceXMLTransformSpecified"),
              transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(this.sourceXmlField)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.SourceXMLFieldNotSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.SourceXMLFieldSpecified"),
              transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(this.valueXmlField)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.ResultFieldNotSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.ResultFieldSpecified"),
              transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(this.targetXPath)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.TargetXPathNotSpecified"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.TargetXPathSpecified"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have the right input streams leading to this transform!
    if (input.length > 0) {
      boolean targetTransformFound = false;
      boolean sourceTransformFound = false;
      for (String s : input) {
        if (this.targetXmlTransform != null && this.targetXmlTransform.equals(s)) {
          targetTransformFound = true;
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG, "XmlJoin.CheckResult.TargetXMLTransformFound", this.targetXmlTransform),
                  transformMeta);
          remarks.add(cr);
        }
        if (this.sourceXmlTransform != null && this.sourceXmlTransform.equals(s)) {
          sourceTransformFound = true;
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG, "XmlJoin.CheckResult.SourceXMLTransformFound", this.sourceXmlTransform),
                  transformMeta);
          remarks.add(cr);
        }
      }

      if (!targetTransformFound) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "XmlJoin.CheckResult.TargetXMLTransformNotFound", this.targetXmlTransform),
                transformMeta);
        remarks.add(cr);
      }
      if (!sourceTransformFound) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "XmlJoin.CheckResult.SourceXMLTransformNotFound", this.sourceXmlTransform),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XmlJoin.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  /**
   * Returns the Input/Output metadata for this transform. The generator transform only produces
   * output, does not accept input!
   */
  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {
      ioMeta = new TransformIOMeta(true, true, false, false, false, false);
      ioMeta.addStream(
          new Stream(
              IStream.StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "XmlJoinMeta.InfoStream.TargetInputStream.Description"),
              StreamIcon.INFO,
              null));
      ioMeta.addStream(
          new Stream(
              IStream.StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "XmlJoinMeta.InfoStream.SourceInputStream.Description"),
              StreamIcon.INFO,
              null));
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    for (IStream stream : infoStreams) {
      stream.setTransformMeta(TransformMeta.findTransform(transforms, stream.getSubject()));
    }
  }

  @Override
  public void resetTransformIoMeta() {
    // Don't reset!
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }
}
