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

package org.apache.hop.pipeline.transforms.replacestring;

import static org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_OK;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
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
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "ReplaceString",
    image = "replaceinstring.svg",
    name = "i18n::ReplaceString.Name",
    description = "i18n::ReplaceString.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::ReplaceStringMeta.keyword",
    documentationUrl = "/pipeline/transforms/replacestring.html")
public class ReplaceStringMeta extends BaseTransformMeta<ReplaceString, ReplaceStringData> {
  private static final Class<?> PKG = ReplaceStringMeta.class;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "ReplaceString.Injection.FIELDS")
  private List<RSField> fields;

  public ReplaceStringMeta() {
    super();
    this.fields = new ArrayList<>();
  }

  public ReplaceStringMeta(ReplaceStringMeta m) {
    this();
    m.fields.forEach(f -> this.fields.add(new RSField(f)));
  }

  @Override
  public ReplaceStringMeta clone() {
    return new ReplaceStringMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    for (RSField field : fields) {
      String fieldName = variables.resolve(field.getFieldOutStream());
      IValueMeta valueMeta;
      if (!Utils.isEmpty(field.getFieldOutStream())) {
        // We have a new field
        valueMeta = new ValueMetaString(fieldName);
        valueMeta.setOrigin(name);
        // set encoding to new field from source field
        IValueMeta sourceField = inputRowMeta.searchValueMeta(field.getFieldInStream());
        if (sourceField != null) {
          valueMeta.setStringEncoding(sourceField.getStringEncoding());
        }
        inputRowMeta.addValueMeta(valueMeta);
      } else {
        valueMeta = inputRowMeta.searchValueMeta(field.getFieldInStream());
        if (valueMeta == null) {
          continue;
        }
        valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
      }
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transforminfo,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    CheckResult cr;
    StringBuilder errorMessage = new StringBuilder();
    boolean first = true;
    boolean errorFound = false;

    if (prev == null) {
      errorMessage
          .append(BaseMessages.getString(PKG, "ReplaceStringMeta.CheckResult.NoInputReceived"))
          .append(Const.CR);
      cr = new CheckResult(TYPE_RESULT_ERROR, errorMessage.toString(), transforminfo);
      remarks.add(cr);
    } else {

      for (RSField field : fields) {
        String fieldIn = field.getFieldInStream();

        IValueMeta v = prev.searchValueMeta(fieldIn);
        if (v == null) {
          if (first) {
            first = false;
            errorMessage
                .append(
                    BaseMessages.getString(
                        PKG, "ReplaceStringMeta.CheckResult.MissingInStreamFields"))
                .append(Const.CR);
          }
          errorFound = true;
          errorMessage.append("\t\t").append(fieldIn).append(Const.CR);
        }
      }
      if (errorFound) {
        cr = new CheckResult(TYPE_RESULT_ERROR, errorMessage.toString(), transforminfo);
      } else {
        cr =
            new CheckResult(
                TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ReplaceStringMeta.CheckResult.FoundInStreamFields"),
                transforminfo);
      }
      remarks.add(cr);

      // Check whether all are strings
      first = true;
      errorFound = false;
      for (RSField field : fields) {
        String fieldIn = field.getFieldInStream();

        IValueMeta v = prev.searchValueMeta(fieldIn);
        if (v != null && v.getType() != IValueMeta.TYPE_STRING) {
          if (first) {
            first = false;
            errorMessage
                .append(
                    BaseMessages.getString(
                        PKG, "ReplaceStringMeta.CheckResult.OperationOnNonStringFields"))
                .append(Const.CR);
          }
          errorFound = true;
          errorMessage.append("\t\t").append(fieldIn).append(Const.CR);
        }
      }
      if (errorFound) {
        cr = new CheckResult(TYPE_RESULT_ERROR, errorMessage.toString(), transforminfo);
      } else {
        cr =
            new CheckResult(
                TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "ReplaceStringMeta.CheckResult.AllOperationsOnStringFields"),
                transforminfo);
      }
      remarks.add(cr);

      int x = 0;
      for (RSField field : fields) {
        x++;
        if (Utils.isEmpty(field.getFieldInStream())) {
          cr =
              new CheckResult(
                  TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG,
                      "ReplaceStringMeta.CheckResult.InStreamFieldMissing",
                      Integer.toString(x)),
                  transforminfo);
          remarks.add(cr);
        }
      }

      // Check if all input fields are distinct.
      for (int idx = 0; idx < fields.size(); idx++) {
        for (int jdx = 0; jdx < fields.size(); jdx++) {
          RSField field1 = fields.get(idx);
          RSField field2 = fields.get(jdx);
          if (field1.getFieldInStream().equals(field2.getFieldInStream()) && idx < jdx) {
            String errMessage =
                BaseMessages.getString(
                    PKG,
                    "ReplaceStringMeta.CheckResult.FieldInputError",
                    field1.getFieldInStream());
            cr = new CheckResult(TYPE_RESULT_ERROR, errMessage, transforminfo);
            remarks.add(cr);
          }
        }
      }
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public static final class RSField {
    @HopMetadataProperty(
        key = "in_stream_name",
        injectionKey = "FIELD_IN_STREAM",
        injectionKeyDescription = "ReplaceString.Injection.FIELD_IN_STREAM")
    private String fieldInStream;

    @HopMetadataProperty(
        key = "out_stream_name",
        injectionKey = "FIELD_OUT_STREAM",
        injectionKeyDescription = "ReplaceString.Injection.FIELD_OUT_STREAM")
    private String fieldOutStream;

    @HopMetadataProperty(
        key = "use_regex",
        injectionKey = "USE_REGEX",
        injectionKeyDescription = "ReplaceString.Injection.USE_REGEX")
    private boolean usingRegEx;

    @HopMetadataProperty(
        key = "replace_string",
        injectionKey = "REPLACE_STRING",
        injectionKeyDescription = "ReplaceString.Injection.REPLACE_STRING")
    private String replaceString;

    @HopMetadataProperty(
        key = "replace_by_string",
        injectionKey = "REPLACE_BY",
        injectionKeyDescription = "ReplaceString.Injection.REPLACE_BY")
    private String replaceByString;

    @HopMetadataProperty(
        key = "set_empty_string",
        injectionKey = "EMPTY_STRING",
        injectionKeyDescription = "ReplaceString.Injection.EMPTY_STRING")
    private boolean settingEmptyString;

    @HopMetadataProperty(
        key = "replace_field_by_string",
        injectionKey = "REPLACE_WITH_FIELD",
        injectionKeyDescription = "ReplaceString.Injection.REPLACE_WITH_FIELD")
    private String replaceFieldByString;

    @HopMetadataProperty(
        key = "whole_word",
        injectionKey = "REPLACE_WHOLE_WORD",
        injectionKeyDescription = "ReplaceString.Injection.REPLACE_WHOLE_WORD")
    private boolean replacingWholeWord;

    @HopMetadataProperty(
        key = "case_sensitive",
        injectionKey = "CASE_SENSITIVE",
        injectionKeyDescription = "ReplaceString.Injection.CASE_SENSITIVE")
    private boolean caseSensitive;

    @HopMetadataProperty(
        key = "is_unicode",
        injectionKey = "IS_UNICODE",
        injectionKeyDescription = "ReplaceString.Injection.IS_UNICODE")
    private boolean unicode;

    public RSField() {}

    public RSField(RSField f) {
      this();
      this.fieldInStream = f.fieldInStream;
      this.fieldOutStream = f.fieldOutStream;
      this.usingRegEx = f.usingRegEx;
      this.replaceString = f.replaceString;
      this.replaceByString = f.replaceByString;
      this.settingEmptyString = f.settingEmptyString;
      this.replaceFieldByString = f.replaceFieldByString;
      this.replacingWholeWord = f.replacingWholeWord;
      this.caseSensitive = f.caseSensitive;
      this.unicode = f.unicode;
    }

    /**
     * Gets fieldInStream
     *
     * @return value of fieldInStream
     */
    public String getFieldInStream() {
      return fieldInStream;
    }

    /**
     * Sets fieldInStream
     *
     * @param fieldInStream value of fieldInStream
     */
    public void setFieldInStream(String fieldInStream) {
      this.fieldInStream = fieldInStream;
    }

    /**
     * Gets fieldOutStream
     *
     * @return value of fieldOutStream
     */
    public String getFieldOutStream() {
      return fieldOutStream;
    }

    /**
     * Sets fieldOutStream
     *
     * @param fieldOutStream value of fieldOutStream
     */
    public void setFieldOutStream(String fieldOutStream) {
      this.fieldOutStream = fieldOutStream;
    }

    /**
     * Gets usingRegEx
     *
     * @return value of usingRegEx
     */
    public boolean isUsingRegEx() {
      return usingRegEx;
    }

    /**
     * Sets usingRegEx
     *
     * @param usingRegEx value of usingRegEx
     */
    public void setUsingRegEx(boolean usingRegEx) {
      this.usingRegEx = usingRegEx;
    }

    /**
     * Gets replaceString
     *
     * @return value of replaceString
     */
    public String getReplaceString() {
      return replaceString;
    }

    /**
     * Sets replaceString
     *
     * @param replaceString value of replaceString
     */
    public void setReplaceString(String replaceString) {
      this.replaceString = replaceString;
    }

    /**
     * Gets replaceByString
     *
     * @return value of replaceByString
     */
    public String getReplaceByString() {
      return replaceByString;
    }

    /**
     * Sets replaceByString
     *
     * @param replaceByString value of replaceByString
     */
    public void setReplaceByString(String replaceByString) {
      this.replaceByString = replaceByString;
    }

    /**
     * Gets settingEmptyString
     *
     * @return value of settingEmptyString
     */
    public boolean isSettingEmptyString() {
      return settingEmptyString;
    }

    /**
     * Sets settingEmptyString
     *
     * @param settingEmptyString value of settingEmptyString
     */
    public void setSettingEmptyString(boolean settingEmptyString) {
      this.settingEmptyString = settingEmptyString;
    }

    /**
     * Gets replaceFieldByString
     *
     * @return value of replaceFieldByString
     */
    public String getReplaceFieldByString() {
      return replaceFieldByString;
    }

    /**
     * Sets replaceFieldByString
     *
     * @param replaceFieldByString value of replaceFieldByString
     */
    public void setReplaceFieldByString(String replaceFieldByString) {
      this.replaceFieldByString = replaceFieldByString;
    }

    /**
     * Gets replacingWholeWord
     *
     * @return value of replacingWholeWord
     */
    public boolean isReplacingWholeWord() {
      return replacingWholeWord;
    }

    /**
     * Sets replacingWholeWord
     *
     * @param replacingWholeWord value of replacingWholeWord
     */
    public void setReplacingWholeWord(boolean replacingWholeWord) {
      this.replacingWholeWord = replacingWholeWord;
    }

    /**
     * Gets caseSensitive
     *
     * @return value of caseSensitive
     */
    public boolean isCaseSensitive() {
      return caseSensitive;
    }

    /**
     * Sets caseSensitive
     *
     * @param caseSensitive value of caseSensitive
     */
    public void setCaseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
    }

    /**
     * Gets unicode
     *
     * @return value of unicode
     */
    public boolean isUnicode() {
      return unicode;
    }

    /**
     * Sets unicode
     *
     * @param unicode value of unicode
     */
    public void setUnicode(boolean unicode) {
      this.unicode = unicode;
    }
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<RSField> getFields() {
    return fields;
  }

  /**
   * Sets fields
   *
   * @param fields value of fields
   */
  public void setFields(List<RSField> fields) {
    this.fields = fields;
  }
}
