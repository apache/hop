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

package org.apache.hop.pipeline.transforms.regexeval;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "RegexEval",
    image = "regexeval.svg",
    name = "i18n::RegexEval.Name",
    description = "i18n::RegexEval.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/regexeval.html")
public class RegexEvalMeta extends BaseTransformMeta
    implements ITransformMeta<RegexEval, RegexEvalData> {
  private static final Class<?> PKG = RegexEvalMeta.class; // For Translator

  private String script;
  private String matcher;
  private String resultfieldname;
  private boolean usevar;

  private boolean allowcapturegroups;
  private boolean replacefields;

  private boolean canoneq;
  private boolean caseinsensitive;
  private boolean comment;
  private boolean dotall;
  private boolean multiline;
  private boolean unicode;
  private boolean unix;

  private String[] fieldName;
  private int[] fieldType;
  private String[] fieldFormat;
  private String[] fieldGroup;
  private String[] fieldDecimal;
  private String[] fieldCurrency;
  private int[] fieldLength;
  private int[] fieldPrecision;
  private String[] fieldNullIf;
  private String[] fieldIfNull;
  private int[] fieldTrimType;

  public RegexEvalMeta() {
    super();
  }

  public Object clone() {
    RegexEvalMeta retval = (RegexEvalMeta) super.clone();

    int nrFields = fieldName.length;

    retval.allocate(nrFields);
    System.arraycopy(fieldName, 0, retval.fieldName, 0, nrFields);
    System.arraycopy(fieldType, 0, retval.fieldType, 0, nrFields);
    System.arraycopy(fieldLength, 0, retval.fieldLength, 0, nrFields);
    System.arraycopy(fieldPrecision, 0, retval.fieldPrecision, 0, nrFields);
    System.arraycopy(fieldFormat, 0, retval.fieldFormat, 0, nrFields);
    System.arraycopy(fieldGroup, 0, retval.fieldGroup, 0, nrFields);
    System.arraycopy(fieldDecimal, 0, retval.fieldDecimal, 0, nrFields);
    System.arraycopy(fieldCurrency, 0, retval.fieldCurrency, 0, nrFields);
    System.arraycopy(fieldNullIf, 0, retval.fieldNullIf, 0, nrFields);
    System.arraycopy(fieldIfNull, 0, retval.fieldIfNull, 0, nrFields);
    System.arraycopy(fieldTrimType, 0, retval.fieldTrimType, 0, nrFields);

    return retval;
  }

  public void allocate(int nrFields) {
    fieldName = new String[nrFields];
    fieldType = new int[nrFields];
    fieldFormat = new String[nrFields];
    fieldGroup = new String[nrFields];
    fieldDecimal = new String[nrFields];
    fieldCurrency = new String[nrFields];
    fieldLength = new int[nrFields];
    fieldPrecision = new int[nrFields];
    fieldNullIf = new String[nrFields];
    fieldIfNull = new String[nrFields];
    fieldTrimType = new int[nrFields];
  }

  public String getScript() {
    return script;
  }

  public String getRegexOptions() {
    StringBuilder options = new StringBuilder();

    if (isCaseInsensitiveFlagSet()) {
      options.append("(?i)");
    }
    if (isCommentFlagSet()) {
      options.append("(?x)");
    }
    if (isDotAllFlagSet()) {
      options.append("(?s)");
    }
    if (isMultilineFlagSet()) {
      options.append("(?m)");
    }
    if (isUnicodeFlagSet()) {
      options.append("(?u)");
    }
    if (isUnixLineEndingsFlagSet()) {
      options.append("(?d)");
    }
    return options.toString();
  }

  public void setScript(String script) {
    this.script = script;
  }

  public String getMatcher() {
    return matcher;
  }

  public void setMatcher(String matcher) {
    this.matcher = matcher;
  }

  public String getResultFieldName() {
    return resultfieldname;
  }

  public void setResultFieldName(String resultfieldname) {
    this.resultfieldname = resultfieldname;
  }

  public boolean isUseVariableInterpolationFlagSet() {
    return usevar;
  }

  public void setUseVariableInterpolationFlag(boolean usevar) {
    this.usevar = usevar;
  }

  public boolean isAllowCaptureGroupsFlagSet() {
    return allowcapturegroups;
  }

  public void setAllowCaptureGroupsFlag(boolean allowcapturegroups) {
    this.allowcapturegroups = allowcapturegroups;
  }

  public boolean isReplacefields() {
    return replacefields;
  }

  public void setReplacefields(boolean replacefields) {
    this.replacefields = replacefields;
  }

  public boolean isCanonicalEqualityFlagSet() {
    return canoneq;
  }

  public void setCanonicalEqualityFlag(boolean canoneq) {
    this.canoneq = canoneq;
  }

  public boolean isCaseInsensitiveFlagSet() {
    return caseinsensitive;
  }

  public void setCaseInsensitiveFlag(boolean caseinsensitive) {
    this.caseinsensitive = caseinsensitive;
  }

  public boolean isCommentFlagSet() {
    return comment;
  }

  public void setCommentFlag(boolean comment) {
    this.comment = comment;
  }

  public boolean isDotAllFlagSet() {
    return dotall;
  }

  public void setDotAllFlag(boolean dotall) {
    this.dotall = dotall;
  }

  public boolean isMultilineFlagSet() {
    return multiline;
  }

  public void setMultilineFlag(boolean multiline) {
    this.multiline = multiline;
  }

  public boolean isUnicodeFlagSet() {
    return unicode;
  }

  public void setUnicodeFlag(boolean unicode) {
    this.unicode = unicode;
  }

  public boolean isUnixLineEndingsFlagSet() {
    return unix;
  }

  public void setUnixLineEndingsFlag(boolean unix) {
    this.unix = unix;
  }

  public String[] getFieldName() {
    return fieldName;
  }

  public void setFieldName(String[] value) {
    this.fieldName = value;
  }

  public int[] getFieldType() {
    return fieldType;
  }

  public void setFieldType(int[] fieldType) {
    this.fieldType = fieldType;
  }

  public String[] getFieldFormat() {
    return fieldFormat;
  }

  public void setFieldFormat(String[] fieldFormat) {
    this.fieldFormat = fieldFormat;
  }

  public String[] getFieldGroup() {
    return fieldGroup;
  }

  public void setFieldGroup(String[] fieldGroup) {
    this.fieldGroup = fieldGroup;
  }

  public String[] getFieldDecimal() {
    return fieldDecimal;
  }

  public void setFieldDecimal(String[] fieldDecimal) {
    this.fieldDecimal = fieldDecimal;
  }

  public String[] getFieldCurrency() {
    return fieldCurrency;
  }

  public void setFieldCurrency(String[] fieldCurrency) {
    this.fieldCurrency = fieldCurrency;
  }

  public int[] getFieldLength() {
    return fieldLength;
  }

  public void setFieldLength(int[] fieldLength) {
    this.fieldLength = fieldLength;
  }

  public int[] getFieldPrecision() {
    return fieldPrecision;
  }

  public void setFieldPrecision(int[] fieldPrecision) {
    this.fieldPrecision = fieldPrecision;
  }

  public String[] getFieldNullIf() {
    return fieldNullIf;
  }

  public void setFieldNullIf(final String[] fieldNullIf) {
    this.fieldNullIf = fieldNullIf;
  }

  public String[] getFieldIfNull() {
    return fieldIfNull;
  }

  public void setFieldIfNull(final String[] fieldIfNull) {
    this.fieldIfNull = fieldIfNull;
  }

  public int[] getFieldTrimType() {
    return fieldTrimType;
  }

  public void setFieldTrimType(final int[] fieldTrimType) {
    this.fieldTrimType = fieldTrimType;
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      script = XmlHandler.getTagValue(transformNode, "script");
      matcher = XmlHandler.getTagValue(transformNode, "matcher");
      resultfieldname = XmlHandler.getTagValue(transformNode, "resultfieldname");
      usevar = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "usevar"));
      allowcapturegroups =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "allowcapturegroups"));
      replacefields = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "replacefields"));
      canoneq = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "canoneq"));
      caseinsensitive =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "caseinsensitive"));
      comment = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "comment"));
      dotall = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "dotall"));
      multiline = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "multiline"));
      unicode = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "unicode"));
      unix = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "unix"));

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        fieldName[i] = XmlHandler.getTagValue(fnode, "name");
        final String stype = XmlHandler.getTagValue(fnode, "type");
        fieldFormat[i] = XmlHandler.getTagValue(fnode, "format");
        fieldGroup[i] = XmlHandler.getTagValue(fnode, "group");
        fieldDecimal[i] = XmlHandler.getTagValue(fnode, "decimal");
        fieldCurrency[i] = XmlHandler.getTagValue(fnode, "currency");
        final String slen = XmlHandler.getTagValue(fnode, "length");
        final String sprc = XmlHandler.getTagValue(fnode, "precision");
        fieldNullIf[i] = XmlHandler.getTagValue(fnode, "nullif");
        fieldIfNull[i] = XmlHandler.getTagValue(fnode, "ifnull");
        final String trim = XmlHandler.getTagValue(fnode, "trimtype");
        fieldType[i] = ValueMetaFactory.getIdForValueMeta(stype);
        fieldLength[i] = Const.toInt(slen, -1);
        fieldPrecision[i] = Const.toInt(sprc, -1);
        fieldTrimType[i] = ValueMetaString.getTrimTypeByCode(trim);
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "RegexEvalMeta.Exception.UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  public void setDefault() {
    script = "";
    matcher = "";
    resultfieldname = "result";
    usevar = false;
    allowcapturegroups = false;
    replacefields = true;
    canoneq = false;
    caseinsensitive = false;
    comment = false;
    dotall = false;
    multiline = false;
    unicode = false;
    unix = false;

    allocate(0);
  }

  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] infos,
      TransformMeta nextTransforms,
      IVariables variables,
      IHopMetadataProvider metadataProviders)
      throws HopTransformException {
    try {
      if (!Utils.isEmpty(resultfieldname)) {
        if (replacefields) {
          int replaceIndex = inputRowMeta.indexOfValue(resultfieldname);
          if (replaceIndex < 0) {
            IValueMeta v = new ValueMetaBoolean(variables.resolve(resultfieldname));
            v.setOrigin(name);
            inputRowMeta.addValueMeta(v);
          } else {
            IValueMeta valueMeta = inputRowMeta.getValueMeta(replaceIndex);
            IValueMeta replaceMeta =
                ValueMetaFactory.cloneValueMeta(valueMeta, IValueMeta.TYPE_BOOLEAN);
            replaceMeta.setOrigin(name);
            inputRowMeta.setValueMeta(replaceIndex, replaceMeta);
          }
        } else {
          IValueMeta v = new ValueMetaBoolean(variables.resolve(resultfieldname));
          v.setOrigin(name);
          inputRowMeta.addValueMeta(v);
        }
      }

      if (allowcapturegroups == true) {
        for (int i = 0; i < fieldName.length; i++) {
          if (Utils.isEmpty(fieldName[i])) {
            continue;
          }

          if (replacefields) {
            int replaceIndex = inputRowMeta.indexOfValue(fieldName[i]);
            if (replaceIndex < 0) {
              inputRowMeta.addValueMeta(constructValueMeta(null, fieldName[i], i, name));
            } else {
              IValueMeta valueMeta = inputRowMeta.getValueMeta(replaceIndex);
              IValueMeta replaceMeta = constructValueMeta(valueMeta, fieldName[i], i, name);
              inputRowMeta.setValueMeta(replaceIndex, replaceMeta);
            }
          } else {
            inputRowMeta.addValueMeta(constructValueMeta(null, fieldName[i], i, name));
          }
        }
      }
    } catch (Exception e) {
      throw new HopTransformException(e);
    }
  }

  private IValueMeta constructValueMeta(
      IValueMeta sourceValueMeta, String fieldName, int i, String name) throws HopPluginException {
    int type = fieldType[i];
    if (type == IValueMeta.TYPE_NONE) {
      type = IValueMeta.TYPE_STRING;
    }
    IValueMeta v;
    if (sourceValueMeta == null) {
      v = ValueMetaFactory.createValueMeta(fieldName, type);
    } else {
      v = ValueMetaFactory.cloneValueMeta(sourceValueMeta, type);
    }
    v.setLength(fieldLength[i]);
    v.setPrecision(fieldPrecision[i]);
    v.setOrigin(name);
    v.setConversionMask(fieldFormat[i]);
    v.setDecimalSymbol(fieldDecimal[i]);
    v.setGroupingSymbol(fieldGroup[i]);
    v.setCurrencySymbol(fieldCurrency[i]);
    v.setTrimType(fieldTrimType[i]);

    return v;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    " + XmlHandler.addTagValue("script", script));
    retval.append("    " + XmlHandler.addTagValue("matcher", matcher));
    retval.append("    " + XmlHandler.addTagValue("resultfieldname", resultfieldname));
    retval.append("    " + XmlHandler.addTagValue("usevar", usevar));
    retval.append("    " + XmlHandler.addTagValue("allowcapturegroups", allowcapturegroups));
    retval.append("    " + XmlHandler.addTagValue("replacefields", replacefields));
    retval.append("    " + XmlHandler.addTagValue("canoneq", canoneq));
    retval.append("    " + XmlHandler.addTagValue("caseinsensitive", caseinsensitive));
    retval.append("    " + XmlHandler.addTagValue("comment", comment));
    retval.append("    " + XmlHandler.addTagValue("dotall", dotall));
    retval.append("    " + XmlHandler.addTagValue("multiline", multiline));
    retval.append("    " + XmlHandler.addTagValue("unicode", unicode));
    retval.append("    " + XmlHandler.addTagValue("unix", unix));

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < fieldName.length; i++) {
      if (fieldName[i] != null && fieldName[i].length() != 0) {
        retval.append("      <field>").append(Const.CR);
        retval.append("        ").append(XmlHandler.addTagValue("name", fieldName[i]));
        retval
            .append("        ")
            .append(
                XmlHandler.addTagValue("type", ValueMetaFactory.getValueMetaName(fieldType[i])));
        retval.append("        ").append(XmlHandler.addTagValue("format", fieldFormat[i]));
        retval.append("        ").append(XmlHandler.addTagValue("group", fieldGroup[i]));
        retval.append("        ").append(XmlHandler.addTagValue("decimal", fieldDecimal[i]));
        retval.append("        ").append(XmlHandler.addTagValue("length", fieldLength[i]));
        retval.append("        ").append(XmlHandler.addTagValue("precision", fieldPrecision[i]));
        retval.append("        ").append(XmlHandler.addTagValue("nullif", fieldNullIf[i]));
        retval.append("        ").append(XmlHandler.addTagValue("ifnull", fieldIfNull[i]));
        retval
            .append("        ")
            .append(
                XmlHandler.addTagValue(
                    "trimtype", ValueMetaString.getTrimTypeCode(fieldTrimType[i])));
        retval.append("        ").append(XmlHandler.addTagValue("currency", fieldCurrency[i]));
        retval.append("      </field>").append(Const.CR);
      }
    }
    retval.append("    </fields>").append(Const.CR);

    return retval.toString();
  }

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

    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "RegexEvalMeta.CheckResult.ConnectedTransformOK",
                  String.valueOf(prev.size())),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "RegexEvalMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }

    // Check Field to evaluate
    if (!Utils.isEmpty(matcher)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "RegexEvalMeta.CheckResult.MatcherOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "RegexEvalMeta.CheckResult.NoMatcher"),
              transformMeta);
      remarks.add(cr);
    }

    // Check Result Field name
    if (!Utils.isEmpty(resultfieldname)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "RegexEvalMeta.CheckResult.ResultFieldnameOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "RegexEvalMeta.CheckResult.NoResultFieldname"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      RegexEvalData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new RegexEval(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  public RegexEvalData getTransformData() {
    return new RegexEvalData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
