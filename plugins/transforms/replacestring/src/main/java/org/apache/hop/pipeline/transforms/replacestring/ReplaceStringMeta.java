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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
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

import static org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_OK;

@InjectionSupported(
    localizationPrefix = "ReplaceString.Injection.",
    groups = {"FIELDS"})
@Transform(
    id = "ReplaceString",
    image = "replaceinstring.svg",
    name = "i18n::BaseTransform.TypeLongDesc.ReplaceString",
    description = "i18n::BaseTransform.TypeTooltipDesc.ReplaceString",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/replacestring.html")
public class ReplaceStringMeta extends BaseTransformMeta
    implements ITransformMeta<ReplaceString, ReplaceStringData> {
  private static final Class<?> PKG = ReplaceStringMeta.class; // For Translator

  @Injection(name = "FIELD_IN_STREAM", group = "FIELDS")
  private String[] fieldInStream;

  @Injection(name = "FIELD_OUT_STREAM", group = "FIELDS")
  private String[] fieldOutStream;

  @Injection(name = "USE_REGEX", group = "FIELDS")
  private int[] useRegEx;

  @Injection(name = "REPLACE_STRING", group = "FIELDS")
  private String[] replaceString;

  @Injection(name = "REPLACE_BY", group = "FIELDS")
  private String[] replaceByString;

  /** Flag : set empty string */
  @Injection(name = "EMPTY_STRING", group = "FIELDS")
  private boolean[] setEmptyString;

  @Injection(name = "REPLACE_WITH_FIELD", group = "FIELDS")
  private String[] replaceFieldByString;

  @Injection(name = "REPLACE_WHOLE_WORD", group = "FIELDS")
  private int[] wholeWord;

  @Injection(name = "CASE_SENSITIVE", group = "FIELDS")
  private int[] caseSensitive;

  @Injection(name = "IS_UNICODE", group = "FIELDS")
  private int[] isUnicode;

  protected static final String[] caseSensitiveCode = {"no", "yes"};

  protected static final String[] isUnicodeCode = {"no", "yes"};

  protected static final String[] caseSensitiveDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  protected static final String[] isUnicodeDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  public static final int CASE_SENSITIVE_NO = 0;

  public static final int CASE_SENSITIVE_YES = 1;

  public static final int IS_UNICODE_NO = 0;

  public static final int IS_UNICODE_YES = 1;

  protected static final String[] wholeWordDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  protected static final String[] wholeWordCode = {"no", "yes"};

  public static final int WHOLE_WORD_NO = 0;

  public static final int WHOLE_WORD_YES = 1;

  protected static final String[] useRegExDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  protected static final String[] useRegExCode = {"no", "yes"};

  public static final int USE_REGEX_NO = 0;

  public static final int USE_REGEX_YES = 1;

  public ReplaceStringMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the fieldInStream. */
  public String[] getFieldInStream() {
    return fieldInStream;
  }

  /** @param keyStream The fieldInStream to set. */
  public void setFieldInStream(String[] keyStream) {
    this.fieldInStream = keyStream;
  }

  public int[] getCaseSensitive() {
    return caseSensitive;
  }

  public int[] isUnicode() {
    return isUnicode;
  }

  public int[] getWholeWord() {
    return wholeWord;
  }

  public void setWholeWord(int[] wholeWord) {
    this.wholeWord = wholeWord;
  }

  public int[] getUseRegEx() {
    return useRegEx;
  }

  public void setUseRegEx(int[] useRegEx) {
    this.useRegEx = useRegEx;
  }

  /** @return the setEmptyString */
  public boolean[] isSetEmptyString() {
    return setEmptyString;
  }

  /** @param setEmptyString the setEmptyString to set */
  public void setEmptyString(boolean[] setEmptyString) {
    this.setEmptyString = setEmptyString;
  }

  /** @return Returns the fieldOutStream. */
  public String[] getFieldOutStream() {
    return fieldOutStream;
  }

  /** @param keyStream The fieldOutStream to set. */
  public void setFieldOutStream(String[] keyStream) {
    this.fieldOutStream = keyStream;
  }

  public String[] getReplaceString() {
    return replaceString;
  }

  public void setReplaceString(String[] replaceString) {
    this.replaceString = replaceString;
  }

  public String[] getReplaceByString() {
    return replaceByString;
  }

  public void setReplaceByString(String[] replaceByString) {
    this.replaceByString = replaceByString;
  }

  public String[] getFieldReplaceByString() {
    return replaceFieldByString;
  }

  public void setFieldReplaceByString(String[] replaceFieldByString) {
    this.replaceFieldByString = replaceFieldByString;
  }

  public void setCaseSensitive(int[] caseSensitive) {
    this.caseSensitive = caseSensitive;
  }

  public void setIsUnicode(int[] isUnicode) {
    this.isUnicode = isUnicode;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public void allocate(int nrkeys) {
    fieldInStream = new String[nrkeys];
    fieldOutStream = new String[nrkeys];
    useRegEx = new int[nrkeys];
    replaceString = new String[nrkeys];
    replaceByString = new String[nrkeys];
    setEmptyString = new boolean[nrkeys];
    replaceFieldByString = new String[nrkeys];
    wholeWord = new int[nrkeys];
    caseSensitive = new int[nrkeys];
    isUnicode = new int[nrkeys];
  }

  public Object clone() {
    ReplaceStringMeta retval = (ReplaceStringMeta) super.clone();
    int nrkeys = fieldInStream.length;

    retval.allocate(nrkeys);

    System.arraycopy(fieldInStream, 0, retval.fieldInStream, 0, nrkeys);
    System.arraycopy(fieldOutStream, 0, retval.fieldOutStream, 0, nrkeys);
    System.arraycopy(useRegEx, 0, retval.useRegEx, 0, nrkeys);
    System.arraycopy(replaceString, 0, retval.replaceString, 0, nrkeys);
    System.arraycopy(replaceByString, 0, retval.replaceByString, 0, nrkeys);
    System.arraycopy(setEmptyString, 0, retval.setEmptyString, 0, nrkeys);
    System.arraycopy(replaceFieldByString, 0, retval.replaceFieldByString, 0, nrkeys);
    System.arraycopy(wholeWord, 0, retval.wholeWord, 0, nrkeys);
    System.arraycopy(caseSensitive, 0, retval.caseSensitive, 0, nrkeys);
    System.arraycopy(isUnicode, 0, retval.isUnicode, 0, nrkeys);
    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      ReplaceStringData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ReplaceString(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      int nrkeys;

      Node lookup = XmlHandler.getSubNode(transformNode, "fields");
      nrkeys = XmlHandler.countNodes(lookup, "field");

      allocate(nrkeys);

      for (int i = 0; i < nrkeys; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(lookup, "field", i);

        fieldInStream[i] = Const.NVL(XmlHandler.getTagValue(fnode, "in_stream_name"), "");
        fieldOutStream[i] = Const.NVL(XmlHandler.getTagValue(fnode, "out_stream_name"), "");
        useRegEx[i] =
            getCaseSensitiveByCode(Const.NVL(XmlHandler.getTagValue(fnode, "use_regex"), ""));
        replaceString[i] = Const.NVL(XmlHandler.getTagValue(fnode, "replace_string"), "");
        replaceByString[i] = Const.NVL(XmlHandler.getTagValue(fnode, "replace_by_string"), "");
        String emptyString = XmlHandler.getTagValue(fnode, "set_empty_string");

        setEmptyString[i] = !Utils.isEmpty(emptyString) && "Y".equalsIgnoreCase(emptyString);
        replaceFieldByString[i] =
            Const.NVL(XmlHandler.getTagValue(fnode, "replace_field_by_string"), "");
        wholeWord[i] =
            getWholeWordByCode(Const.NVL(XmlHandler.getTagValue(fnode, "whole_word"), ""));
        caseSensitive[i] =
            getCaseSensitiveByCode(Const.NVL(XmlHandler.getTagValue(fnode, "case_sensitive"), ""));
        isUnicode[i] =
            getIsUniCodeByCode(Const.NVL(XmlHandler.getTagValue(fnode, "is_unicode"), ""));
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "ReplaceStringMeta.Exception.UnableToReadTransformMetaFromXML"),
          e);
    }
  }

  private static int getIsUniCodeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < isUnicodeCode.length; i++) {
      if (isUnicodeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public void setDefault() {
    fieldInStream = null;
    fieldOutStream = null;
    int nrkeys = 0;

    allocate(nrkeys);
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(500);

    retval.append("    <fields>").append(Const.CR);

    for (int i = 0; i < fieldInStream.length; i++) {
      retval.append("      <field>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("in_stream_name", fieldInStream[i]));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("out_stream_name", fieldOutStream[i]));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("use_regex", getUseRegExCode(useRegEx[i])));
      retval.append("        ").append(XmlHandler.addTagValue("replace_string", replaceString[i]));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("replace_by_string", replaceByString[i]));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("set_empty_string", setEmptyString[i]));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("replace_field_by_string", replaceFieldByString[i]));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("whole_word", getWholeWordCode(wholeWord[i])));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("case_sensitive", getCaseSensitiveCode(caseSensitive[i])));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("is_unicode", getIsUniCodeCode(isUnicode[i])));
      retval.append("      </field>").append(Const.CR);
    }

    retval.append("    </fields>").append(Const.CR);

    return retval.toString();
  }

  private static String getIsUniCodeCode(int i) {
    if (i < 0 || i >= isUnicodeCode.length) {
      return isUnicodeCode[0];
    }
    return isUnicodeCode[i];
  }

  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    int nrFields = fieldInStream == null ? 0 : fieldInStream.length;
    for (int i = 0; i < nrFields; i++) {
      String fieldName = variables.resolve(fieldOutStream[i]);
      IValueMeta valueMeta;
      if (!Utils.isEmpty(fieldOutStream[i])) {
        // We have a new field
        valueMeta = new ValueMetaString(fieldName);
        valueMeta.setOrigin(name);
        // set encoding to new field from source field http://jira.pentaho.com/browse/PDI-11839
        IValueMeta sourceField = inputRowMeta.searchValueMeta(fieldInStream[i]);
        if (sourceField != null) {
          valueMeta.setStringEncoding(sourceField.getStringEncoding());
        }
        inputRowMeta.addValueMeta(valueMeta);
      } else {
        valueMeta = inputRowMeta.searchValueMeta(fieldInStream[i]);
        if (valueMeta == null) {
          continue;
        }
        valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
      }
    }
  }

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

      for (int i = 0; i < fieldInStream.length; i++) {
        String field = fieldInStream[i];

        IValueMeta v = prev.searchValueMeta(field);
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
          errorMessage.append("\t\t").append(field).append(Const.CR);
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
      for (int i = 0; i < fieldInStream.length; i++) {
        String field = fieldInStream[i];

        IValueMeta v = prev.searchValueMeta(field);
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
          errorMessage.append("\t\t").append(field).append(Const.CR);
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

      if (fieldInStream.length > 0) {
        for (int idx = 0; idx < fieldInStream.length; idx++) {
          if (Utils.isEmpty(fieldInStream[idx])) {
            cr =
                new CheckResult(
                    TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                        PKG,
                        "ReplaceStringMeta.CheckResult.InStreamFieldMissing",
                        Integer.toString(idx + 1)),
                    transforminfo);
            remarks.add(cr);
          }
        }
      }

      // Check if all input fields are distinct.
      for (int idx = 0; idx < fieldInStream.length; idx++) {
        for (int jdx = 0; jdx < fieldInStream.length; jdx++) {
          if (fieldInStream[idx].equals(fieldInStream[jdx]) && idx != jdx && idx < jdx) {
            String errMessage =
                BaseMessages.getString(
                    PKG, "ReplaceStringMeta.CheckResult.FieldInputError", fieldInStream[idx]);
            cr = new CheckResult(TYPE_RESULT_ERROR, errMessage, transforminfo);
            remarks.add(cr);
          }
        }
      }
    }
  }

  public ReplaceStringData getTransformData() {
    return new ReplaceStringData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  private static String getCaseSensitiveCode(int i) {
    if (i < 0 || i >= caseSensitiveCode.length) {
      return caseSensitiveCode[0];
    }
    return caseSensitiveCode[i];
  }

  private static String getWholeWordCode(int i) {
    if (i < 0 || i >= wholeWordCode.length) {
      return wholeWordCode[0];
    }
    return wholeWordCode[i];
  }

  private static String getUseRegExCode(int i) {
    if (i < 0 || i >= useRegExCode.length) {
      return useRegExCode[0];
    }
    return useRegExCode[i];
  }

  public static String getCaseSensitiveDesc(int i) {
    if (i < 0 || i >= caseSensitiveDesc.length) {
      return caseSensitiveDesc[0];
    }
    return caseSensitiveDesc[i];
  }

  public static String getIsUnicodeDesc(int i) {
    if (i < 0 || i >= isUnicodeDesc.length) {
      return isUnicodeDesc[0];
    }
    return isUnicodeDesc[i];
  }

  public static String getWholeWordDesc(int i) {
    if (i < 0 || i >= wholeWordDesc.length) {
      return wholeWordDesc[0];
    }
    return wholeWordDesc[i];
  }

  public static String getUseRegExDesc(int i) {
    if (i < 0 || i >= useRegExDesc.length) {
      return useRegExDesc[0];
    }
    return useRegExDesc[i];
  }

  private static int getCaseSensitiveByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < caseSensitiveCode.length; i++) {
      if (caseSensitiveCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  private static int getWholeWordByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < wholeWordCode.length; i++) {
      if (wholeWordCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  private static int getRegExByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < useRegExCode.length; i++) {
      if (useRegExCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public static int getCaseSensitiveByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < caseSensitiveDesc.length; i++) {
      if (caseSensitiveDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getCaseSensitiveByCode(tt);
  }

  public static int getIsUnicodeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < isUnicodeDesc.length; i++) {
      if (isUnicodeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getIsUniCodeByCode(tt);
  }

  public static int getWholeWordByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < wholeWordDesc.length; i++) {
      if (wholeWordDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getWholeWordByCode(tt);
  }

  public static int getUseRegExByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < useRegExDesc.length; i++) {
      if (useRegExDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getRegExByCode(tt);
  }

  private void nullToEmpty(String[] strings) {
    for (int i = 0; i < strings.length; i++) {
      if (strings[i] == null) {
        strings[i] = StringUtils.EMPTY;
      }
    }
  }

  /**
   * If we use injection we can have different arrays lengths. We need synchronize them for
   * consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    int nrFields = (fieldInStream == null) ? -1 : fieldInStream.length;
    if (nrFields <= 0) {
      return;
    }
    String[][] rtnStringArrays =
        Utils.normalizeArrays(
            nrFields, fieldOutStream, replaceString, replaceByString, replaceFieldByString);
    fieldOutStream = rtnStringArrays[0];
    replaceString = rtnStringArrays[1];
    replaceByString = rtnStringArrays[2];
    replaceFieldByString = rtnStringArrays[3];

    nullToEmpty(fieldOutStream);
    nullToEmpty(replaceString);
    nullToEmpty(replaceByString);
    nullToEmpty(replaceFieldByString);

    int[][] rtnIntArrays =
        Utils.normalizeArrays(nrFields, useRegEx, wholeWord, caseSensitive, isUnicode);
    useRegEx = rtnIntArrays[0];
    wholeWord = rtnIntArrays[1];
    caseSensitive = rtnIntArrays[2];
    isUnicode = rtnIntArrays[3];

    boolean[][] rtnBooleanArrays = Utils.normalizeArrays(nrFields, setEmptyString);
    setEmptyString = rtnBooleanArrays[0];
  }
}
