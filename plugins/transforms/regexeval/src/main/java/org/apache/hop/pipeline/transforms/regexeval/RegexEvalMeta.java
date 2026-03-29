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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "RegexEval",
    image = "regexeval.svg",
    name = "i18n::RegexEval.Name",
    description = "i18n::RegexEval.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    keywords = "i18n::RegexEvalMeta.keyword",
    documentationUrl = "/pipeline/transforms/regexeval.html")
@Getter
@Setter
public class RegexEvalMeta extends BaseTransformMeta<RegexEval, RegexEvalData> {
  private static final Class<?> PKG = RegexEvalMeta.class;
  public static final String CONST_SPACES = "        ";

  @Getter
  @Setter
  public static class RegexField {
    @HopMetadataProperty(key = "name")
    private String fieldName;

    @HopMetadataProperty(
        key = "type",
        intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class)
    private int fieldType;

    @HopMetadataProperty(key = "format")
    private String fieldFormat;

    @HopMetadataProperty(key = "group")
    private String fieldGroup;

    @HopMetadataProperty(key = "decimal")
    private String fieldDecimal;

    @HopMetadataProperty(key = "currency")
    private String fieldCurrency;

    @HopMetadataProperty(key = "length")
    private int fieldLength;

    @HopMetadataProperty(key = "precision")
    private int fieldPrecision;

    @HopMetadataProperty(key = "nullif")
    private String fieldNullIf;

    @HopMetadataProperty(key = "ifnull")
    private String fieldIfNull;

    @HopMetadataProperty(
        key = "trimtype",
        intCodeConverter = ValueMetaBase.TrimTypeCodeConverter.class)
    private int fieldTrimType;

    public RegexField() {}

    public RegexField(RegexField f) {
      this();
      this.fieldName = f.fieldName;
      this.fieldType = f.fieldType;
      this.fieldFormat = f.fieldFormat;
      this.fieldGroup = f.fieldGroup;
      this.fieldDecimal = f.fieldDecimal;
      this.fieldCurrency = f.fieldCurrency;
      this.fieldLength = f.fieldLength;
      this.fieldPrecision = f.fieldPrecision;
      this.fieldNullIf = f.fieldNullIf;
      this.fieldIfNull = f.fieldIfNull;
      this.fieldTrimType = f.fieldTrimType;
    }

    private IValueMeta constructValueMeta(IValueMeta sourceValueMeta, String name)
        throws HopPluginException {
      int type = fieldType;
      if (type == IValueMeta.TYPE_NONE) {
        type = IValueMeta.TYPE_STRING;
      }
      IValueMeta v;
      if (sourceValueMeta == null) {
        v = ValueMetaFactory.createValueMeta(fieldName, type);
      } else {
        v = ValueMetaFactory.cloneValueMeta(sourceValueMeta, type);
      }
      v.setLength(fieldLength);
      v.setPrecision(fieldPrecision);
      v.setOrigin(name);
      v.setConversionMask(fieldFormat);
      v.setDecimalSymbol(fieldDecimal);
      v.setGroupingSymbol(fieldGroup);
      v.setCurrencySymbol(fieldCurrency);
      v.setTrimType(fieldTrimType);

      return v;
    }
  }

  @HopMetadataProperty(key = "script")
  private String script;

  @HopMetadataProperty(key = "matcher")
  private String matcher;

  @HopMetadataProperty(key = "resultfieldname")
  private String resultFieldName;

  @HopMetadataProperty(key = "usevar")
  private boolean usingVariables;

  @HopMetadataProperty(key = "allowcapturegroups")
  private boolean allowingCaptureGroups;

  @HopMetadataProperty(key = "replacefields")
  private boolean replacingFields;

  @HopMetadataProperty(key = "canoneq")
  private boolean canonicalEqualityEnabled;

  @HopMetadataProperty(key = "caseinsensitive")
  private boolean caseInsensitive;

  @HopMetadataProperty(key = "comment")
  private boolean commentingEnabled;

  @HopMetadataProperty(key = "dotall")
  private boolean dotAllEnabled;

  @HopMetadataProperty(key = "multiline")
  private boolean multiLine;

  @HopMetadataProperty(key = "unicode")
  private boolean unicode;

  @HopMetadataProperty(key = "unix")
  private boolean unixLineEndings;

  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<RegexField> regexFields;

  public RegexEvalMeta() {
    super();
    script = "";
    matcher = "";
    replacingFields = true;
    regexFields = new ArrayList<>();
  }

  public RegexEvalMeta(RegexEvalMeta m) {
    this();
    this.script = m.script;
    this.matcher = m.matcher;
    this.resultFieldName = m.resultFieldName;
    this.usingVariables = m.usingVariables;
    this.allowingCaptureGroups = m.allowingCaptureGroups;
    this.replacingFields = m.replacingFields;
    this.canonicalEqualityEnabled = m.canonicalEqualityEnabled;
    this.caseInsensitive = m.caseInsensitive;
    this.commentingEnabled = m.commentingEnabled;
    this.dotAllEnabled = m.dotAllEnabled;
    this.multiLine = m.multiLine;
    this.unicode = m.unicode;
    this.unixLineEndings = m.unixLineEndings;
    m.regexFields.forEach(field -> this.regexFields.add(new RegexField(field)));
  }

  @Override
  public RegexEvalMeta clone() {
    return new RegexEvalMeta(this);
  }

  public String getRegexOptions() {
    StringBuilder options = new StringBuilder();

    if (isCaseInsensitive()) {
      options.append("(?i)");
    }
    if (isCommentingEnabled()) {
      options.append("(?x)");
    }
    if (isDotAllEnabled()) {
      options.append("(?s)");
    }
    if (isMultiLine()) {
      options.append("(?m)");
    }
    if (isUnicode()) {
      options.append("(?u)");
    }
    if (isUnixLineEndings()) {
      options.append("(?d)");
    }
    return options.toString();
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String transformName,
      IRowMeta[] infos,
      TransformMeta nextTransforms,
      IVariables variables,
      IHopMetadataProvider metadataProviders)
      throws HopTransformException {
    try {
      if (!Utils.isEmpty(resultFieldName)) {
        if (replacingFields) {
          int replaceIndex = inputRowMeta.indexOfValue(resultFieldName);
          if (replaceIndex < 0) {
            IValueMeta v = new ValueMetaBoolean(variables.resolve(resultFieldName));
            v.setOrigin(transformName);
            inputRowMeta.addValueMeta(v);
          } else {
            IValueMeta valueMeta = inputRowMeta.getValueMeta(replaceIndex);
            IValueMeta replaceMeta =
                ValueMetaFactory.cloneValueMeta(valueMeta, IValueMeta.TYPE_BOOLEAN);
            replaceMeta.setOrigin(transformName);
            inputRowMeta.setValueMeta(replaceIndex, replaceMeta);
          }
        } else {
          IValueMeta v = new ValueMetaBoolean(variables.resolve(resultFieldName));
          v.setOrigin(transformName);
          inputRowMeta.addValueMeta(v);
        }
      }

      if (allowingCaptureGroups) {
        for (RegexField field : regexFields) {
          if (Utils.isEmpty(field.fieldName)) {
            continue;
          }

          if (replacingFields) {
            int replaceIndex = inputRowMeta.indexOfValue(field.fieldName);
            if (replaceIndex < 0) {
              inputRowMeta.addValueMeta(field.constructValueMeta(null, transformName));
            } else {
              IValueMeta valueMeta = inputRowMeta.getValueMeta(replaceIndex);
              IValueMeta replaceMeta = field.constructValueMeta(valueMeta, transformName);
              inputRowMeta.setValueMeta(replaceIndex, replaceMeta);
            }
          } else {
            inputRowMeta.addValueMeta(field.constructValueMeta(null, transformName));
          }
        }
      }
    } catch (Exception e) {
      throw new HopTransformException(e);
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

    CheckResult cr;

    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "RegexEvalMeta.CheckResult.ConnectedTransformOK",
                  String.valueOf(prev.size())),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "RegexEvalMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }

    // Check Field to evaluate
    if (!Utils.isEmpty(matcher)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "RegexEvalMeta.CheckResult.MatcherOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "RegexEvalMeta.CheckResult.NoMatcher"),
              transformMeta);
      remarks.add(cr);
    }

    // Check Result Field name
    if (!Utils.isEmpty(resultFieldName)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "RegexEvalMeta.CheckResult.ResultFieldnameOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "RegexEvalMeta.CheckResult.NoResultFieldname"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
