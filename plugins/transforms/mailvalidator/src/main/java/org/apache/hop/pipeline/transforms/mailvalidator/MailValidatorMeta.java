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

package org.apache.hop.pipeline.transforms.mailvalidator;

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
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "MailValidator",
    image = "mailvalidator.svg",
    name = "i18n::BaseTransform.TypeLongDesc.MailValidator",
    description = "i18n::BaseTransform.TypeTooltipDesc.MailValidator",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Validation",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/mailvalidator.html")
public class MailValidatorMeta extends BaseTransformMeta
    implements ITransformMeta<MailValidator, MailValidatorData> {
  private static final Class<?> PKG = MailValidatorMeta.class; // For Translator

  /** dynamic email address */
  private String emailfield;

  private boolean ResultAsString;

  private boolean smtpCheck;

  private String emailValideMsg;

  private String emailNotValideMsg;

  private String errorsFieldName;

  private String timeout;

  private String defaultSMTP;

  private String emailSender;

  private String defaultSMTPField;

  private boolean isdynamicDefaultSMTP;

  private String resultfieldname;

  public MailValidatorMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the emailfield. */
  public String getEmailField() {
    return emailfield;
  }

  /**
   * @param emailfield The emailfield to set.
   * @deprecated use {@link #setEmailField(String)} instead
   */
  @Deprecated
  public void setEmailfield(String emailfield) {
    setEmailField(emailfield);
  }

  public void setEmailField(String emailfield) {
    this.emailfield = emailfield;
  }

  /** @return Returns the resultName. */
  public String getResultFieldName() {
    return resultfieldname;
  }

  /** @param resultfieldname The resultfieldname to set. */
  public void setResultFieldName(String resultfieldname) {
    this.resultfieldname = resultfieldname;
  }

  /** @param emailValideMsg The emailValideMsg to set. */
  public void setEmailValideMsg(String emailValideMsg) {
    this.emailValideMsg = emailValideMsg;
  }

  /**
   * @return Returns the emailValideMsg.
   * @deprecated use {@link #getEmailValideMsg()} instead
   */
  @Deprecated
  public String getEMailValideMsg() {
    return getEmailValideMsg();
  }

  public String getEmailValideMsg() {
    return emailValideMsg;
  }

  /**
   * @return Returns the emailNotValideMsg.
   * @deprecated use {@link #getEmailNotValideMsg()} instead
   */
  @Deprecated
  public String getEMailNotValideMsg() {
    return getEmailNotValideMsg();
  }

  public String getEmailNotValideMsg() {
    return emailNotValideMsg;
  }

  /** @return Returns the errorsFieldName. */
  public String getErrorsField() {
    return errorsFieldName;
  }

  /** @param errorsFieldName The errorsFieldName to set. */
  public void setErrorsField(String errorsFieldName) {
    this.errorsFieldName = errorsFieldName;
  }

  /** @return Returns the timeout. */
  public String getTimeOut() {
    return timeout;
  }

  /** @param timeout The timeout to set. */
  public void setTimeOut(String timeout) {
    this.timeout = timeout;
  }

  /** @return Returns the defaultSMTP. */
  public String getDefaultSMTP() {
    return defaultSMTP;
  }

  /** @param defaultSMTP The defaultSMTP to set. */
  public void setDefaultSMTP(String defaultSMTP) {
    this.defaultSMTP = defaultSMTP;
  }

  /**
   * @return Returns the emailSender.
   * @deprecated use {@link #getEmailSender()} instead
   */
  @Deprecated
  public String geteMailSender() {
    return getEmailSender();
  }

  public String getEmailSender() {
    return emailSender;
  }

  /**
   * @param emailSender The emailSender to set.
   * @deprecated use {@link #setEmailSender(String)} instead
   */
  @Deprecated
  public void seteMailSender(String emailSender) {
    setEmailSender(emailSender);
  }

  public void setEmailSender(String emailSender) {
    this.emailSender = emailSender;
  }

  /** @return Returns the defaultSMTPField. */
  public String getDefaultSMTPField() {
    return defaultSMTPField;
  }

  /** @param defaultSMTPField The defaultSMTPField to set. */
  public void setDefaultSMTPField(String defaultSMTPField) {
    this.defaultSMTPField = defaultSMTPField;
  }

  /**
   * @return Returns the isdynamicDefaultSMTP.
   * @deprecated use {@link #isDynamicDefaultSMTP()} instead
   */
  @Deprecated
  public boolean isdynamicDefaultSMTP() {
    return isDynamicDefaultSMTP();
  }

  public boolean isDynamicDefaultSMTP() {
    return isdynamicDefaultSMTP;
  }

  /**
   * @param isdynamicDefaultSMTP The isdynamicDefaultSMTP to set.
   * @deprecated use {@link #setDynamicDefaultSMTP(boolean)} instead
   */
  @Deprecated
  public void setdynamicDefaultSMTP(boolean isdynamicDefaultSMTP) {
    setDynamicDefaultSMTP(isdynamicDefaultSMTP);
  }

  public void setDynamicDefaultSMTP(boolean isdynamicDefaultSMTP) {
    this.isdynamicDefaultSMTP = isdynamicDefaultSMTP;
  }

  /** @param emailNotValideMsg The emailNotValideMsg to set. */
  public void setEmailNotValideMsg(String emailNotValideMsg) {
    this.emailNotValideMsg = emailNotValideMsg;
  }

  public boolean isResultAsString() {
    return ResultAsString;
  }

  public void setResultAsString(boolean ResultAsString) {
    this.ResultAsString = ResultAsString;
  }

  public void setSMTPCheck(boolean smtpcheck) {
    this.smtpCheck = smtpcheck;
  }

  public boolean isSMTPCheck() {
    return smtpCheck;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    MailValidatorMeta retval = (MailValidatorMeta) super.clone();

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      MailValidatorData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new MailValidator(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  public void setDefault() {
    resultfieldname = "result";
    emailValideMsg = "email address is valid";
    emailNotValideMsg = "email address is not valid";
    ResultAsString = false;
    errorsFieldName = "Error message";
    timeout = "0";
    defaultSMTP = null;
    emailSender = "noreply@domain.com";
    smtpCheck = false;
    isdynamicDefaultSMTP = false;
    defaultSMTPField = null;
  }

  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    String realResultFieldName = variables.resolve(resultfieldname);
    if (ResultAsString) {
      IValueMeta v = new ValueMetaString(realResultFieldName);
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);

    } else {
      IValueMeta v = new ValueMetaBoolean(realResultFieldName);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    String realErrorsFieldName = variables.resolve(errorsFieldName);
    if (!Utils.isEmpty(realErrorsFieldName)) {
      IValueMeta v = new ValueMetaString(realErrorsFieldName);
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    " + XmlHandler.addTagValue("emailfield", emailfield));
    retval.append("    " + XmlHandler.addTagValue("resultfieldname", resultfieldname));
    retval.append("    ").append(XmlHandler.addTagValue("ResultAsString", ResultAsString));
    retval.append("    ").append(XmlHandler.addTagValue("smtpCheck", smtpCheck));

    retval.append("    " + XmlHandler.addTagValue("emailValideMsg", emailValideMsg));
    retval.append("    " + XmlHandler.addTagValue("emailNotValideMsg", emailNotValideMsg));
    retval.append("    " + XmlHandler.addTagValue("errorsFieldName", errorsFieldName));
    retval.append("    " + XmlHandler.addTagValue("timeout", timeout));
    retval.append("    " + XmlHandler.addTagValue("defaultSMTP", defaultSMTP));
    retval.append("    " + XmlHandler.addTagValue("emailSender", emailSender));
    retval.append("    " + XmlHandler.addTagValue("defaultSMTPField", defaultSMTPField));

    retval.append("    " + XmlHandler.addTagValue("isdynamicDefaultSMTP", isdynamicDefaultSMTP));

    return retval.toString();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      emailfield = XmlHandler.getTagValue(transformNode, "emailfield");
      resultfieldname = XmlHandler.getTagValue(transformNode, "resultfieldname");
      ResultAsString =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "ResultAsString"));
      smtpCheck = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "smtpCheck"));

      emailValideMsg = XmlHandler.getTagValue(transformNode, "emailValideMsg");
      emailNotValideMsg = XmlHandler.getTagValue(transformNode, "emailNotValideMsg");
      errorsFieldName = XmlHandler.getTagValue(transformNode, "errorsFieldName");
      timeout = XmlHandler.getTagValue(transformNode, "timeout");
      defaultSMTP = XmlHandler.getTagValue(transformNode, "defaultSMTP");
      emailSender = XmlHandler.getTagValue(transformNode, "emailSender");
      defaultSMTPField = XmlHandler.getTagValue(transformNode, "defaultSMTPField");

      isdynamicDefaultSMTP =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "isdynamicDefaultSMTP"));

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "MailValidatorMeta.Exception.UnableToReadTransformMeta"), e);
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

    if (Utils.isEmpty(resultfieldname)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.ResultFieldMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.ResultFieldOk"),
              transformMeta);
    }
    remarks.add(cr);

    if (this.ResultAsString) {
      if (Utils.isEmpty(emailValideMsg)) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.EmailValidMsgMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.EmailValidMsgOk"),
                transformMeta);
      }
      remarks.add(cr);

      if (Utils.isEmpty(emailNotValideMsg)) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "MailValidatorMeta.CheckResult.EmailNotValidMsgMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.EmailNotValidMsgOk"),
                transformMeta);
      }
      remarks.add(cr);
    }

    if (Utils.isEmpty(emailfield)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.eMailFieldMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.eMailFieldOK"),
              transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "MailValidatorMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.NoInpuReceived"),
              transformMeta);
    }
    remarks.add(cr);
    if (ResultAsString) {
      if (Utils.isEmpty(emailValideMsg)) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.eMailValidMsgMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.eMailValidMsgOk"),
                transformMeta);
      }
      remarks.add(cr);

      if (Utils.isEmpty(emailNotValideMsg)) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "MailValidatorMeta.CheckResult.eMailNotValidMsgMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.eMailNotValidMsgOk"),
                transformMeta);
      }
      remarks.add(cr);
    }
    // SMTP check
    if (smtpCheck) {
      // sender
      if (Utils.isEmpty(emailSender)) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.eMailSenderMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "MailValidatorMeta.CheckResult.eMailSenderOk"),
                transformMeta);
      }
      remarks.add(cr);

      // dynamic default SMTP
      if (isdynamicDefaultSMTP) {
        if (Utils.isEmpty(defaultSMTPField)) {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG, "MailValidatorMeta.CheckResult.dynamicDefaultSMTPFieldMissing"),
                  transformMeta);
        } else {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG, "MailValidatorMeta.CheckResult.dynamicDefaultSMTPFieldOk"),
                  transformMeta);
        }
        remarks.add(cr);
      }
    }
  }

  public MailValidatorData getTransformData() {
    return new MailValidatorData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
