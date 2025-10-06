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

package org.apache.hop.pipeline.transforms.creditcardvalidator;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
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
    id = "CreditCardValidator",
    image = "creditcardvalidator.svg",
    name = "i18n::CreditCardValidator.Name",
    description = "i18n::CreditCardValidator.Description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Validation",
    keywords = "i18n::CreditCardValidatorMeta.keyword",
    documentationUrl = "/pipeline/transforms/creditcardvalidator.html")
public class CreditCardValidatorMeta
    extends BaseTransformMeta<CreditCardValidator, CreditCardValidatorData> {
  private static final Class<?> PKG = CreditCardValidatorMeta.class;

  /** dynamic field */
  @HopMetadataProperty(key = "fieldname")
  private String fieldName;

  @HopMetadataProperty(key = "cardtype")
  private String cardType;

  @HopMetadataProperty(key = "notvalidmsg")
  private String notValidMessage;

  /** function result: new value name */
  @HopMetadataProperty(key = "resultfieldname")
  private String resultFieldName;

  @HopMetadataProperty(key = "onlydigits")
  private boolean onlyDigits;

  public CreditCardValidatorMeta() {
    super(); // allocate BaseTransformMeta
  }

  public CreditCardValidatorMeta(CreditCardValidatorMeta m) {
    this.fieldName = m.fieldName;
    this.cardType = m.cardType;
    this.notValidMessage = m.notValidMessage;
    this.resultFieldName = m.resultFieldName;
    this.onlyDigits = m.onlyDigits;
  }

  @Override
  public CreditCardValidatorMeta clone() {
    return new CreditCardValidatorMeta(this);
  }

  @Override
  public void setDefault() {
    resultFieldName = "result";
    onlyDigits = false;
    cardType = "card type";
    notValidMessage = "not valid message";
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
    String realResultFieldName = variables.resolve(resultFieldName);
    if (!Utils.isEmpty(realResultFieldName)) {
      IValueMeta v = new ValueMetaBoolean(realResultFieldName);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    String realCardType = variables.resolve(cardType);
    if (!Utils.isEmpty(realCardType)) {
      IValueMeta v = new ValueMetaString(realCardType);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    String realNotValidMessage = variables.resolve(notValidMessage);
    if (!Utils.isEmpty(notValidMessage)) {
      IValueMeta v = new ValueMetaString(realNotValidMessage);
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

    String realresultfieldname = variables.resolve(resultFieldName);
    if (Utils.isEmpty(realresultfieldname)) {
      errorMessage =
          BaseMessages.getString(PKG, "CreditCardValidatorMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "CreditCardValidatorMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(fieldName)) {
      errorMessage =
          BaseMessages.getString(PKG, "CreditCardValidatorMeta.CheckResult.CardFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "CreditCardValidatorMeta.CheckResult.CardFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "CreditCardValidatorMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "CreditCardValidatorMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Gets fieldName
   *
   * @return value of fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * Sets fieldName
   *
   * @param fieldName value of fieldName
   */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * Gets cardType
   *
   * @return value of cardType
   */
  public String getCardType() {
    return cardType;
  }

  /**
   * Sets cardType
   *
   * @param cardType value of cardType
   */
  public void setCardType(String cardType) {
    this.cardType = cardType;
  }

  /**
   * Gets notValidMessage
   *
   * @return value of notValidMessage
   */
  public String getNotValidMessage() {
    return notValidMessage;
  }

  /**
   * Sets notValidMessage
   *
   * @param notValidMessage value of notValidMessage
   */
  public void setNotValidMessage(String notValidMessage) {
    this.notValidMessage = notValidMessage;
  }

  /**
   * Gets resultFieldName
   *
   * @return value of resultFieldName
   */
  public String getResultFieldName() {
    return resultFieldName;
  }

  /**
   * Sets resultFieldName
   *
   * @param resultFieldName value of resultFieldName
   */
  public void setResultFieldName(String resultFieldName) {
    this.resultFieldName = resultFieldName;
  }

  /**
   * Gets onlyDigits
   *
   * @return value of onlyDigits
   */
  public boolean isOnlyDigits() {
    return onlyDigits;
  }

  /**
   * Sets onlyDigits
   *
   * @param onlyDigits value of onlyDigits
   */
  public void setOnlyDigits(boolean onlyDigits) {
    this.onlyDigits = onlyDigits;
  }
}
