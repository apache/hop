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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 03-Juin-2008
 *
 */

@Transform(
    id = "CreditCardValidator",
    image = "creditcardvalidator.svg",
    name = "i18n::BaseTransform.TypeLongDesc.CreditCardValidator",
    description = "i18n::BaseTransform.TypeTooltipDesc.CreditCardValidator",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/creditcardvalidator.html")
public class CreditCardValidatorMeta extends BaseTransformMeta
    implements ITransformMeta<CreditCardValidator, CreditCardValidatorData> {

  private static final Class<?> PKG = CreditCardValidatorMeta.class; // For Translator

  /** dynamic field */
  private String fieldname;

  private String cardtype;

  private String notvalidmsg;

  /** function result: new value name */
  private String resultfieldname;

  private boolean onlydigits;

  public CreditCardValidatorMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the fieldname. */
  public String getDynamicField() {
    return this.fieldname;
  }

  /** @param fieldname The fieldname to set. */
  public void setDynamicField(String fieldname) {
    this.fieldname = fieldname;
  }

  /** @return Returns the resultName. */
  public String getResultFieldName() {
    return resultfieldname;
  }

  public void setOnlyDigits(boolean onlydigits) {
    this.onlydigits = onlydigits;
  }

  public boolean isOnlyDigits() {
    return this.onlydigits;
  }

  /** @param resultfieldname The resultfieldname to set. */
  public void setResultFieldName(String resultfieldname) {
    this.resultfieldname = resultfieldname;
  }

  /** @param cardtype The cardtype to set. */
  public void setCardType(String cardtype) {
    this.cardtype = cardtype;
  }

  /** @return Returns the cardtype. */
  public String getCardType() {
    return cardtype;
  }

  /** @param notvalidmsg The notvalidmsg to set. */
  public void setNotValidMsg(String notvalidmsg) {
    this.notvalidmsg = notvalidmsg;
  }

  /** @return Returns the notvalidmsg. */
  public String getNotValidMsg() {
    return notvalidmsg;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    CreditCardValidatorMeta retval = (CreditCardValidatorMeta) super.clone();

    return retval;
  }

  public void setDefault() {
    resultfieldname = "result";
    onlydigits = false;
    cardtype = "card type";
    notvalidmsg = "not valid message";
  }

  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    String realresultfieldname = variables.resolve(resultfieldname);
    if (!Utils.isEmpty(realresultfieldname)) {
      IValueMeta v = new ValueMetaBoolean(realresultfieldname);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    String realcardtype = variables.resolve(cardtype);
    if (!Utils.isEmpty(realcardtype)) {
      IValueMeta v = new ValueMetaString(realcardtype);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    String realnotvalidmsg = variables.resolve(notvalidmsg);
    if (!Utils.isEmpty(notvalidmsg)) {
      IValueMeta v = new ValueMetaString(realnotvalidmsg);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    ").append(XmlHandler.addTagValue("fieldname", fieldname));
    retval.append("    ").append(XmlHandler.addTagValue("resultfieldname", resultfieldname));
    retval.append("    ").append(XmlHandler.addTagValue("cardtype", cardtype));
    retval.append("    ").append(XmlHandler.addTagValue("onlydigits", onlydigits));
    retval.append("    ").append(XmlHandler.addTagValue("notvalidmsg", notvalidmsg));

    return retval.toString();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      fieldname = XmlHandler.getTagValue(transformNode, "fieldname");
      resultfieldname = XmlHandler.getTagValue(transformNode, "resultfieldname");
      cardtype = XmlHandler.getTagValue(transformNode, "cardtype");
      notvalidmsg = XmlHandler.getTagValue(transformNode, "notvalidmsg");
      onlydigits = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "onlydigits"));

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "CreditCardValidatorMeta.Exception.UnableToReadTransformMeta"),
          e);
    }
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
    String errorMessage = "";

    String realresultfieldname = variables.resolve(resultfieldname);
    if (Utils.isEmpty(realresultfieldname)) {
      errorMessage =
          BaseMessages.getString(PKG, "CreditCardValidatorMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "CreditCardValidatorMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(fieldname)) {
      errorMessage =
          BaseMessages.getString(PKG, "CreditCardValidatorMeta.CheckResult.CardFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "CreditCardValidatorMeta.CheckResult.CardFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "CreditCardValidatorMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "CreditCardValidatorMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public CreditCardValidator createTransform(
      TransformMeta transformMeta,
      CreditCardValidatorData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new CreditCardValidator(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public CreditCardValidatorData getTransformData() {
    return new CreditCardValidatorData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
