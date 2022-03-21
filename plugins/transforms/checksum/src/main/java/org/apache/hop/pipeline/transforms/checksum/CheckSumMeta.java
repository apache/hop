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

package org.apache.hop.pipeline.transforms.checksum;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "CheckSum",
    image = "checksum.svg",
    name = "i18n::CheckSum.Name",
    description = "i18n::CheckSum.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::CheckSumMeta.keyword",
    documentationUrl = "/pipeline/transforms/addchecksum.html")
public class CheckSumMeta extends BaseTransformMeta<CheckSum, CheckSumData> {

  private static final Class<?> PKG = CheckSumMeta.class; // For Translator

  public enum CheckSumType implements IEnumHasCode {
    NONE("NONE", ""),
    CRC32("CRC32", BaseMessages.getString(PKG, "CheckSumMeta.Type.CRC32")),
    ADLER32("ADLER32", BaseMessages.getString(PKG, "CheckSumMeta.Type.ADLER32")),
    MD5("MD5", BaseMessages.getString(PKG, "CheckSumMeta.Type.MD5")),
    SHA1("SHA-1", BaseMessages.getString(PKG, "CheckSumMeta.Type.SHA1")),
    SHA256("SHA-256", BaseMessages.getString(PKG, "CheckSumMeta.Type.SHA256")),
    SHA384("SHA-384", BaseMessages.getString(PKG, "CheckSumMeta.Type.SHA384")),
    SHA512("SHA-512", BaseMessages.getString(PKG, "CheckSumMeta.Type.SHA512")),
    ;

    private String code;
    private String description;

    CheckSumType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static final CheckSumType getTypeFromDescription(String description) {
      for (CheckSumType type : values()) {
        if (type.description.equals(description)) {
          return type;
        }
      }
      return NONE;
    }

    public static final String[] getDescriptions() {
      String[] descriptions = new String[values().length - 1];
      for (int i = 1; i < values().length; i++) {
        descriptions[i - 1] = values()[i].description;
      }
      return descriptions;
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    @Override
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
      return description;
    }
  }

  public enum ResultType implements IEnumHasCode {
    STRING("string", BaseMessages.getString(PKG, "CheckSumMeta.ResultType.String")),
    HEXADECIMAL("hexadecimal", BaseMessages.getString(PKG, "CheckSumMeta.ResultType.Hexadecimal")),
    BINARY("binary", BaseMessages.getString(PKG, "CheckSumMeta.ResultType.Binary")),
    ;

    private String code;
    private String description;

    ResultType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static final ResultType getTypeFromDescription(String description) {
      for (ResultType type : values()) {
        if (type.description.equals(description)) {
          return type;
        }
      }
      return STRING;
    }

    public static final String[] getDescriptions() {
      String[] descriptions = new String[values().length];
      for (int i = 0; i < values().length; i++) {
        descriptions[i] = values()[i].description;
      }
      return descriptions;
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    @Override
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
      return description;
    }
  }

  /** by which fields to display? */
  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionKey = "FIELD",
      injectionKeyDescription = "CheckSum.Injection.FIELD")
  private List<Field> fields;

  @HopMetadataProperty(
      key = "resultfieldName",
      injectionKey = "RESULT_FIELD",
      injectionKeyDescription = "CheckSum.Injection.RESULT_FIELD")
  private String resultFieldName;

  @HopMetadataProperty(
      key = "checksumtype",
      storeWithCode = true,
      injectionKey = "TYPE",
      injectionKeyDescription = "CheckSum.Injection.TYPE")
  private CheckSumType checkSumType;

  /** result type */
  @HopMetadataProperty(
      key = "resultType",
      storeWithCode = true,
      injectionKey = "RESULT_TYPE",
      injectionKeyDescription = "CheckSum.Injection.RESULT_TYPE")
  private ResultType resultType;

  public CheckSumMeta() {
    fields = new ArrayList<>();
    checkSumType = CheckSumType.CRC32;
    resultType = ResultType.STRING;
  }

  @Override
  public CheckSumMeta clone() {
    CheckSumMeta meta = new CheckSumMeta();
    meta.checkSumType = checkSumType;
    meta.resultFieldName = resultFieldName;
    meta.resultType = resultType;
    for (Field field : fields) {
      meta.fields.add(new Field(field));
    }
    return meta;
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
    // Output field (String)
    if (!Utils.isEmpty(resultFieldName)) {
      IValueMeta v = null;
      if (checkSumType == CheckSumType.CRC32 || checkSumType == CheckSumType.ADLER32) {
        v = new ValueMetaInteger(variables.resolve(resultFieldName));
      } else {
        switch (resultType) {
          case BINARY:
            v = new ValueMetaBinary(variables.resolve(resultFieldName));
            break;
          default:
            v = new ValueMetaString(variables.resolve(resultFieldName));
            break;
        }
      }
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
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
    String errorMessage = "";

    if (Utils.isEmpty(resultFieldName)) {
      errorMessage = BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
    } else {
      errorMessage = BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    remarks.add(cr);

    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.NotReceivingFields"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "CheckSumMeta.CheckResult.TransformRecevingData", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      boolean errorFound = false;
      errorMessage = "";

      // Starting from selected fields in ...
      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        int idx = prev.indexOfValue(field.getName());
        if (idx < 0) {
          errorMessage += "\t\t" + field.getName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.FieldsFound", errorMessage);

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        if (!fields.isEmpty()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.AllFieldsFound"),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_WARNING,
                  BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.NoFieldsEntered"),
                  transformMeta);
          remarks.add(cr);
        }
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "CheckSumMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "CheckSumMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<Field> getFields() {
    return fields;
  }

  /** @param fields The fields to set */
  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

  /**
   * Gets resultFieldName
   *
   * @return value of resultFieldName
   */
  public String getResultFieldName() {
    return resultFieldName;
  }

  /** @param resultFieldName The resultFieldName to set */
  public void setResultFieldName(String resultFieldName) {
    this.resultFieldName = resultFieldName;
  }

  /**
   * Gets checkSumType
   *
   * @return value of checkSumType
   */
  public CheckSumType getCheckSumType() {
    return checkSumType;
  }

  /** @param checkSumType The checkSumType to set */
  public void setCheckSumType(CheckSumType checkSumType) {
    this.checkSumType = checkSumType;
  }

  /**
   * Gets resultType
   *
   * @return value of resultType
   */
  public ResultType getResultType() {
    return resultType;
  }

  /** @param resultType The resultType to set */
  public void setResultType(ResultType resultType) {
    this.resultType = resultType;
  }
}
