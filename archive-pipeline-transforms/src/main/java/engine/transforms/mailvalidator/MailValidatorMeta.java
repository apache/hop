/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.mailvalidator;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 03-Juin-2008
 *
 */

public class MailValidatorMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = MailValidatorMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * dynamic email address
   */
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

  /**
   * @return Returns the emailfield.
   */
  public String getEmailField() {
    return emailfield;
  }

  /**
   * @param emailfield The emailfield to set.
   * @deprecated use {@link #setEmailField(String)} instead
   */
  @Deprecated
  public void setEmailfield( String emailfield ) {
    setEmailField( emailfield );
  }

  public void setEmailField( String emailfield ) {
    this.emailfield = emailfield;
  }

  /**
   * @return Returns the resultName.
   */
  public String getResultFieldName() {
    return resultfieldname;
  }

  /**
   * @param resultfieldname The resultfieldname to set.
   */
  public void setResultFieldName( String resultfieldname ) {
    this.resultfieldname = resultfieldname;
  }

  /**
   * @param emailValideMsg The emailValideMsg to set.
   */
  public void setEmailValideMsg( String emailValideMsg ) {
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

  /**
   * @return Returns the errorsFieldName.
   */
  public String getErrorsField() {
    return errorsFieldName;
  }

  /**
   * @param errorsFieldName The errorsFieldName to set.
   */
  public void setErrorsField( String errorsFieldName ) {
    this.errorsFieldName = errorsFieldName;
  }

  /**
   * @return Returns the timeout.
   */
  public String getTimeOut() {
    return timeout;
  }

  /**
   * @param timeout The timeout to set.
   */
  public void setTimeOut( String timeout ) {
    this.timeout = timeout;
  }

  /**
   * @return Returns the defaultSMTP.
   */
  public String getDefaultSMTP() {
    return defaultSMTP;
  }

  /**
   * @param defaultSMTP The defaultSMTP to set.
   */
  public void setDefaultSMTP( String defaultSMTP ) {
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
  public void seteMailSender( String emailSender ) {
    setEmailSender( emailSender );
  }

  public void setEmailSender( String emailSender ) {
    this.emailSender = emailSender;
  }

  /**
   * @return Returns the defaultSMTPField.
   */
  public String getDefaultSMTPField() {
    return defaultSMTPField;
  }

  /**
   * @param defaultSMTPField The defaultSMTPField to set.
   */
  public void setDefaultSMTPField( String defaultSMTPField ) {
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
  public void setdynamicDefaultSMTP( boolean isdynamicDefaultSMTP ) {
    setDynamicDefaultSMTP( isdynamicDefaultSMTP );
  }

  public void setDynamicDefaultSMTP( boolean isdynamicDefaultSMTP ) {
    this.isdynamicDefaultSMTP = isdynamicDefaultSMTP;
  }

  /**
   * @param emailNotValideMsg The emailNotValideMsg to set.
   */
  public void setEmailNotValideMsg( String emailNotValideMsg ) {
    this.emailNotValideMsg = emailNotValideMsg;
  }

  public boolean isResultAsString() {
    return ResultAsString;
  }

  public void setResultAsString( boolean ResultAsString ) {
    this.ResultAsString = ResultAsString;
  }

  public void setSMTPCheck( boolean smtpcheck ) {
    this.smtpCheck = smtpcheck;
  }

  public boolean isSMTPCheck() {
    return smtpCheck;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public Object clone() {
    MailValidatorMeta retval = (MailValidatorMeta) super.clone();

    return retval;
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

  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {

    String realResultFieldName = variables.environmentSubstitute( resultfieldname );
    if ( ResultAsString ) {
      IValueMeta v = new ValueMetaString( realResultFieldName );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      r.addValueMeta( v );

    } else {
      IValueMeta v = new ValueMetaBoolean( realResultFieldName );
      v.setOrigin( name );
      r.addValueMeta( v );
    }

    String realErrorsFieldName = variables.environmentSubstitute( errorsFieldName );
    if ( !Utils.isEmpty( realErrorsFieldName ) ) {
      IValueMeta v = new ValueMetaString( realErrorsFieldName );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XMLHandler.addTagValue( "emailfield", emailfield ) );
    retval.append( "    " + XMLHandler.addTagValue( "resultfieldname", resultfieldname ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "ResultAsString", ResultAsString ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "smtpCheck", smtpCheck ) );

    retval.append( "    " + XMLHandler.addTagValue( "emailValideMsg", emailValideMsg ) );
    retval.append( "    " + XMLHandler.addTagValue( "emailNotValideMsg", emailNotValideMsg ) );
    retval.append( "    " + XMLHandler.addTagValue( "errorsFieldName", errorsFieldName ) );
    retval.append( "    " + XMLHandler.addTagValue( "timeout", timeout ) );
    retval.append( "    " + XMLHandler.addTagValue( "defaultSMTP", defaultSMTP ) );
    retval.append( "    " + XMLHandler.addTagValue( "emailSender", emailSender ) );
    retval.append( "    " + XMLHandler.addTagValue( "defaultSMTPField", defaultSMTPField ) );

    retval.append( "    " + XMLHandler.addTagValue( "isdynamicDefaultSMTP", isdynamicDefaultSMTP ) );

    return retval.toString();
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      emailfield = XMLHandler.getTagValue( transformNode, "emailfield" );
      resultfieldname = XMLHandler.getTagValue( transformNode, "resultfieldname" );
      ResultAsString = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "ResultAsString" ) );
      smtpCheck = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "smtpCheck" ) );

      emailValideMsg = XMLHandler.getTagValue( transformNode, "emailValideMsg" );
      emailNotValideMsg = XMLHandler.getTagValue( transformNode, "emailNotValideMsg" );
      errorsFieldName = XMLHandler.getTagValue( transformNode, "errorsFieldName" );
      timeout = XMLHandler.getTagValue( transformNode, "timeout" );
      defaultSMTP = XMLHandler.getTagValue( transformNode, "defaultSMTP" );
      emailSender = XMLHandler.getTagValue( transformNode, "emailSender" );
      defaultSMTPField = XMLHandler.getTagValue( transformNode, "defaultSMTPField" );

      isdynamicDefaultSMTP = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "isdynamicDefaultSMTP" ) );

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "MailValidatorMeta.Exception.UnableToReadTransformMeta" ), e );
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( Utils.isEmpty( resultfieldname ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MailValidatorMeta.CheckResult.ResultFieldMissing" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MailValidatorMeta.CheckResult.ResultFieldOk" ), transformMeta );
    }
    remarks.add( cr );

    if ( this.ResultAsString ) {
      if ( Utils.isEmpty( emailValideMsg ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "MailValidatorMeta.CheckResult.EmailValidMsgMissing" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "MailValidatorMeta.CheckResult.EmailValidMsgOk" ), transformMeta );
      }
      remarks.add( cr );

      if ( Utils.isEmpty( emailNotValideMsg ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "MailValidatorMeta.CheckResult.EmailNotValidMsgMissing" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "MailValidatorMeta.CheckResult.EmailNotValidMsgOk" ), transformMeta );
      }
      remarks.add( cr );
    }

    if ( Utils.isEmpty( emailfield ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MailValidatorMeta.CheckResult.eMailFieldMissing" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MailValidatorMeta.CheckResult.eMailFieldOK" ), transformMeta );
    }
    remarks.add( cr );

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MailValidatorMeta.CheckResult.ReceivingInfoFromOtherTransforms" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MailValidatorMeta.CheckResult.NoInpuReceived" ), transformMeta );
    }
    remarks.add( cr );
    if ( ResultAsString ) {
      if ( Utils.isEmpty( emailValideMsg ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "MailValidatorMeta.CheckResult.eMailValidMsgMissing" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "MailValidatorMeta.CheckResult.eMailValidMsgOk" ), transformMeta );
      }
      remarks.add( cr );

      if ( Utils.isEmpty( emailNotValideMsg ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "MailValidatorMeta.CheckResult.eMailNotValidMsgMissing" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "MailValidatorMeta.CheckResult.eMailNotValidMsgOk" ), transformMeta );
      }
      remarks.add( cr );
    }
    // SMTP check
    if ( smtpCheck ) {
      // sender
      if ( Utils.isEmpty( emailSender ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "MailValidatorMeta.CheckResult.eMailSenderMissing" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "MailValidatorMeta.CheckResult.eMailSenderOk" ), transformMeta );
      }
      remarks.add( cr );

      // dynamic default SMTP
      if ( isdynamicDefaultSMTP ) {
        if ( Utils.isEmpty( defaultSMTPField ) ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
              PKG, "MailValidatorMeta.CheckResult.dynamicDefaultSMTPFieldMissing" ), transformMeta );
        } else {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "MailValidatorMeta.CheckResult.dynamicDefaultSMTPFieldOk" ), transformMeta );
        }
        remarks.add( cr );
      }
    }

  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new MailValidator( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new MailValidatorData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

}
