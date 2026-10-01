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

package org.apache.hop.pipeline.transforms.stringoperations;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.jetbrains.annotations.NotNull;

@Transform(
    id = "StringOperations",
    image = "stringoperations.svg",
    name = "i18n::StringOperations.Name",
    description = "i18n::StringOperations.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::StringOperationsMeta.keyword",
    documentationUrl = "/pipeline/transforms/stringoperations.html")
@InjectionSupported(localizationPrefix = "StringOperationsDialog.Injection.")
@Getter
@Setter
public class StringOperationsMeta
    extends BaseTransformMeta<StringOperations, StringOperationsData> {

  private static final Class<?> PKG = StringOperationsMeta.class;
  public static final String CONST_SPACES = "        ";

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionKey = "FIELD")
  private List<StringOperation> operations;

  public StringOperationsMeta() {
    super();
    this.operations = new ArrayList<>();
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
    // Add new field?
    for (StringOperation operation : operations) {
      IValueMeta v;
      String outputField = variables.resolve(operation.fieldOutStream);
      if (!Utils.isEmpty(outputField)) {
        // Add a new field
        v = new ValueMetaString(outputField);
        v.setLength(100, -1);
        v.setOrigin(name);
        inputRowMeta.addValueMeta(v);
      } else {
        v = inputRowMeta.searchValueMeta(operation.fieldInStream);
        if (v == null) {
          continue;
        }
        v.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
        if (operation.paddingType == Padding.LEFT || operation.paddingType == Padding.RIGHT) {
          int padLen = Const.toInt(variables.resolve(operation.padLen), 0);
          if (padLen > v.getLength()) {
            // alter meta data
            v.setLength(padLen);
          }
        }
      }
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

    if (checkNoInputReceived(remarks, transformMeta, prev)) {
      return;
    }
    StringBuilder errorMessage = checkMissingFields(remarks, transformMeta, prev);
    checkAllFieldsAreStrings(remarks, transformMeta, prev, errorMessage);
    checkMissingInputFields(remarks, transformMeta);
    checkDistinctInputFields(remarks, transformMeta);
  }

  private static boolean checkNoInputReceived(
      List<ICheckResult> remarks, TransformMeta transformMeta, IRowMeta prev) {
    CheckResult cr;
    if (prev == null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "StringOperationsMeta.CheckResult.NoInputReceived")
                  + Const.CR,
              transformMeta);
      remarks.add(cr);
      // Nothing more to do.
      //
      return true;
    }
    return false;
  }

  private void checkDistinctInputFields(List<ICheckResult> remarks, TransformMeta transformMeta) {
    CheckResult cr;
    // Check if all input fields are distinct.
    Set<String> inFields = new HashSet<>();
    for (StringOperation operation : operations) {
      if (inFields.contains(operation.fieldInStream)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "StringOperationsMeta.CheckResult.FieldInputError",
                    operation.fieldInStream),
                transformMeta);
        remarks.add(cr);
      }
      inFields.add(operation.fieldInStream);
    }
  }

  private void checkMissingInputFields(List<ICheckResult> remarks, TransformMeta transformMeta) {
    CheckResult cr;
    int idx = 1;
    for (StringOperation operation : operations) {
      if (Utils.isEmpty(operation.fieldInStream)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "StringOperationsMeta.CheckResult.InStreamFieldMissing",
                    Integer.toString(idx)),
                transformMeta);
        remarks.add(cr);
      }
      idx++;
    }
  }

  private void checkAllFieldsAreStrings(
      List<ICheckResult> remarks,
      TransformMeta transformMeta,
      IRowMeta prev,
      StringBuilder errorMessage) {
    boolean first;
    boolean errorFound;
    CheckResult cr;
    // Check whether all are strings
    first = true;
    errorFound = false;
    for (StringOperation operation : operations) {
      IValueMeta v = prev.searchValueMeta(operation.fieldInStream);
      if (v != null && v.getType() != IValueMeta.TYPE_STRING) {
        if (first) {
          first = false;
          errorMessage
              .append(
                  BaseMessages.getString(
                      PKG, "StringOperationsMeta.CheckResult.OperationOnNonStringFields"))
              .append(Const.CR);
        }
        errorFound = true;
        errorMessage.append("\t\t").append(operation.fieldInStream).append(Const.CR);
      }
    }
    if (errorFound) {
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "StringOperationsMeta.CheckResult.AllOperationsOnStringFields"),
              transformMeta);
    }
    remarks.add(cr);
  }

  private @NotNull StringBuilder checkMissingFields(
      List<ICheckResult> remarks, TransformMeta transformMeta, IRowMeta prev) {
    CheckResult cr;
    boolean errorFound = false;
    StringBuilder errorMessage = new StringBuilder();
    for (StringOperation operation : operations) {
      IValueMeta v = prev.searchValueMeta(operation.fieldInStream);
      if (v == null) {
        errorMessage
            .append(
                BaseMessages.getString(
                    PKG, "StringOperationsMeta.CheckResult.MissingInStreamFields"))
            .append(Const.CR);
      }
      errorFound = true;
      errorMessage.append("\t\t").append(operation.fieldInStream).append(Const.CR);
    }
    if (errorFound) {
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "StringOperationsMeta.CheckResult.FoundInStreamFields"),
              transformMeta);
    }
    remarks.add(cr);
    return errorMessage;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Getter
  @Setter
  public static class StringOperation {
    /** which field in input stream to compare with? */
    @HopMetadataProperty(
        key = "in_stream_name",
        injectionKey = "SOURCEFIELDS",
        injectionKeyDescription = "StringOperationsDialog.Injection.SOURCEFIELDS")
    private String fieldInStream;

    /** output field */
    @HopMetadataProperty(
        key = "out_stream_name",
        injectionKey = "TARGETFIELDS",
        injectionKeyDescription = "StringOperationsDialog.Injection.TARGETFIELDS")
    private String fieldOutStream;

    /** Trim type */
    @HopMetadataProperty(
        key = "trim_type",
        storeWithCode = true,
        injectionKey = "TRIMTYPE",
        injectionKeyDescription = "StringOperationsDialog.Injection.TRIMTYPE")
    private TrimType trimType;

    /** Lower/Upper type */
    @HopMetadataProperty(
        key = "lower_upper",
        storeWithCode = true,
        injectionKey = "LOWERUPPER",
        injectionKeyDescription = "StringOperationsDialog.Injection.LOWERUPPER")
    private LowerUpper lowerUpper;

    /** InitCap */
    @HopMetadataProperty(
        key = "init_cap",
        storeWithCode = true,
        injectionKey = "INITCAP",
        injectionKeyDescription = "StringOperationsDialog.Injection.INITCAP")
    private InitCap initCap;

    @HopMetadataProperty(
        key = "mask_xml",
        storeWithCode = true,
        injectionKey = "MASKXML",
        injectionKeyDescription = "StringOperationsDialog.Injection.MASKXML")
    private MaskXml maskXml;

    @HopMetadataProperty(
        key = "digits",
        storeWithCode = true,
        injectionKey = "DIGITS",
        injectionKeyDescription = "StringOperationsDialog.Injection.DIGITS")
    private Digits digits;

    @HopMetadataProperty(
        key = "remove_special_characters",
        storeWithCode = true,
        injectionKey = "SPECIALCHARS",
        injectionKeyDescription = "StringOperationsDialog.Injection.SPECIALCHARS")
    private RemoveSpecialChars removeSpecialChars;

    /** padding type */
    @HopMetadataProperty(
        key = "padding_type",
        storeWithCode = true,
        injectionKey = "PADDING",
        injectionKeyDescription = "StringOperationsDialog.Injection.PADDING")
    private Padding paddingType;

    /** Pad length */
    @HopMetadataProperty(
        key = "pad_len",
        injectionKey = "PADLEN",
        injectionKeyDescription = "StringOperationsDialog.Injection.PADLEN")
    private String padLen;

    @HopMetadataProperty(
        key = "pad_char",
        injectionKey = "PADCHAR",
        injectionKeyDescription = "StringOperationsDialog.Injection.PADCHAR")
    private String padChar;

    public StringOperation() {
      this.trimType = TrimType.NONE;
      this.lowerUpper = LowerUpper.NONE;
      this.initCap = InitCap.NO;
      this.maskXml = MaskXml.NONE;
      this.paddingType = Padding.NONE;
      this.removeSpecialChars = RemoveSpecialChars.NONE;
      this.digits = Digits.NONE;
    }

    public StringOperation(StringOperation op) {
      this();
      this.digits = op.digits;
      this.fieldInStream = op.fieldInStream;
      this.fieldOutStream = op.fieldOutStream;
      this.initCap = op.initCap;
      this.lowerUpper = op.lowerUpper;
      this.maskXml = op.maskXml;
      this.padChar = op.padChar;
      this.paddingType = op.paddingType;
      this.padLen = op.padLen;
      this.removeSpecialChars = op.removeSpecialChars;
      this.trimType = op.trimType;
    }
  }

  @Getter
  public enum TrimType implements IEnumHasCodeAndDescription {
    NONE(ValueMetaBase.trimTypeCode[0], ValueMetaBase.trimTypeDesc[0]),
    LEFT(ValueMetaBase.trimTypeCode[1], ValueMetaBase.trimTypeDesc[1]),
    RIGHT(ValueMetaBase.trimTypeCode[2], ValueMetaBase.trimTypeDesc[2]),
    BOTH(ValueMetaBase.trimTypeCode[3], ValueMetaBase.trimTypeDesc[3]),
    ;
    private final String code;
    private final String description;

    TrimType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(TrimType.class);
    }

    public static TrimType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(TrimType.class, description, NONE);
    }
  }

  @Getter
  public enum LowerUpper implements IEnumHasCodeAndDescription {
    NONE("none", BaseMessages.getString(PKG, "StringOperationsMeta.LowerUpper.None")),
    LOWER("lower", BaseMessages.getString(PKG, "StringOperationsMeta.LowerUpper.Lower")),
    UPPER("upper", BaseMessages.getString(PKG, "StringOperationsMeta.LowerUpper.Upper")),
    ;
    private final String code;
    private final String description;

    LowerUpper(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(LowerUpper.class);
    }

    public static LowerUpper lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(LowerUpper.class, description, NONE);
    }
  }

  @Getter
  public enum InitCap implements IEnumHasCodeAndDescription {
    NO("no", BaseMessages.getString("System.Combo.No")),
    YES("yes", BaseMessages.getString("System.Combo.Yes")),
    ;
    private final String code;
    private final String description;

    InitCap(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(InitCap.class);
    }

    public static InitCap lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(InitCap.class, description, NO);
    }
  }

  @Getter
  public enum Digits implements IEnumHasCodeAndDescription {
    NONE("none", BaseMessages.getString(PKG, "StringOperationsMeta.Digits.None")),
    DIGITS_ONLY("digits_only", BaseMessages.getString(PKG, "StringOperationsMeta.Digits.Only")),
    DIGITS_REMOVE(
        "remove_digits", BaseMessages.getString(PKG, "StringOperationsMeta.Digits.Remove")),
    ;
    private final String code;
    private final String description;

    Digits(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(Digits.class);
    }

    public static Digits lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(Digits.class, description, NONE);
    }
  }

  @Getter
  public enum MaskXml implements IEnumHasCodeAndDescription {
    NONE("none", BaseMessages.getString(PKG, "StringOperationsMeta.MaskXML.None")),
    ESCAPE_XML("escapexml", BaseMessages.getString(PKG, "StringOperationsMeta.MaskXML.EscapeXML")),
    CDATA("cdata", BaseMessages.getString(PKG, "StringOperationsMeta.MaskXML.CDATA")),
    UNESCAPE_XML(
        "unescapexml", BaseMessages.getString(PKG, "StringOperationsMeta.MaskXML.UnEscapeXML")),
    ESCAPE_SQL("escapesql", BaseMessages.getString(PKG, "StringOperationsMeta.MaskXML.EscapeSQL")),
    ESCAPE_HTML(
        "escapehtml", BaseMessages.getString(PKG, "StringOperationsMeta.MaskXML.EscapeHTML")),
    UNESCAPE_HTML(
        "unescapehtml", BaseMessages.getString(PKG, "StringOperationsMeta.MaskXML.UnEscapeHTML")),
    ;

    private final String code;
    private final String description;

    MaskXml(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(MaskXml.class);
    }

    public static MaskXml lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(MaskXml.class, description, NONE);
    }
  }

  @Getter
  public enum RemoveSpecialChars implements IEnumHasCodeAndDescription {
    NONE("none", BaseMessages.getString(PKG, "StringOperationsMeta.RemoveSpecialCharacters.None")),
    CR("cr", BaseMessages.getString(PKG, "StringOperationsMeta.RemoveSpecialCharacters.CR")),
    LF("lf", BaseMessages.getString(PKG, "StringOperationsMeta.RemoveSpecialCharacters.LF")),
    CRLF("crlf", BaseMessages.getString(PKG, "StringOperationsMeta.RemoveSpecialCharacters.CRLF")),
    TAB("tab", BaseMessages.getString(PKG, "StringOperationsMeta.RemoveSpecialCharacters.TAB")),
    SPACE(
        "espace",
        BaseMessages.getString(PKG, "StringOperationsMeta.RemoveSpecialCharacters.Space")),
    ;
    private final String code;
    private final String description;

    RemoveSpecialChars(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(RemoveSpecialChars.class);
    }

    public static RemoveSpecialChars lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          RemoveSpecialChars.class, description, NONE);
    }
  }

  @Getter
  public enum Padding implements IEnumHasCodeAndDescription {
    NONE("none", BaseMessages.getString(PKG, "StringOperationsMeta.Padding.None")),
    LEFT("left", BaseMessages.getString(PKG, "StringOperationsMeta.Padding.Left")),
    RIGHT("right", BaseMessages.getString(PKG, "StringOperationsMeta.Padding.Right")),
    ;
    private final String code;
    private final String description;

    Padding(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(Padding.class);
    }

    public static Padding lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(Padding.class, description, NONE);
    }
  }
}
