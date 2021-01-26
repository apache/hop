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

package org.apache.hop.workflow.actions.mailvalidator;

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.w3c.dom.Node;

import java.util.List;

@Action(
        id = "MAIL_VALIDATOR",
        name = "i18n::ActionMailValidator.Name",
        description = "i18n::ActionMailValidator.Description",
        image = "MailValidator.svg",
        categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Mail",
        documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/mailvalidator.html"
)
public class ActionMailValidator extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionMailValidator.class; // For Translator

  private boolean smtpCheck;
  private String timeout;
  private String defaultSMTP;
  private String emailSender;
  private String emailAddress;

  public ActionMailValidator( String n, String scr ) {
    super( n, "" );
    emailAddress = null;
    smtpCheck = false;
    timeout = "0";
    defaultSMTP = null;
    emailSender = "noreply@domain.com";
  }

  public ActionMailValidator() {
    this( "", "" );
  }

  public void setSMTPCheck( boolean smtpcheck ) {
    this.smtpCheck = smtpcheck;
  }

  public boolean isSMTPCheck() {
    return smtpCheck;
  }

  public String getEmailAddress() {
    return this.emailAddress;
  }

  public void setEmailAddress( String emailAddress ) {
    this.emailAddress = emailAddress;
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
   */
  public String geteMailSender() {
    return emailSender;
  }

  /**
   * @param emailSender The emailSender to set.
   */
  public void seteMailSender( String emailSender ) {
    this.emailSender = emailSender;
  }

  public Object clone() {
    ActionMailValidator je = (ActionMailValidator) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 100 );
    retval.append( "      " ).append( XmlHandler.addTagValue( "smtpCheck", smtpCheck ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "timeout", timeout ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "defaultSMTP", defaultSMTP ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "emailSender", emailSender ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "emailAddress", emailAddress ) );

    retval.append( super.getXml() );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      smtpCheck = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "smtpCheck" ) );
      timeout = XmlHandler.getTagValue( entrynode, "timeout" );
      defaultSMTP = XmlHandler.getTagValue( entrynode, "defaultSMTP" );
      emailSender = XmlHandler.getTagValue( entrynode, "emailSender" );
      emailAddress = XmlHandler.getTagValue( entrynode, "emailAddress" );

    } catch ( Exception e ) {
      throw new HopXmlException(
        BaseMessages.getString( PKG, "ActionMailValidator.Meta.UnableToLoadFromXML" ), e );
    }
  }

  /**
   * Execute this action and return the result. In this case it means, just set the result boolean in the Result
   * class.
   *
   * @param previousResult The result of the previous execution
   * @return The Result of the execution.
   */
  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setNrErrors( 1 );
    result.setResult( false );

    String realEmailAddress = resolve( emailAddress );
    if ( Utils.isEmpty( realEmailAddress ) ) {
      logError( BaseMessages.getString( PKG, "ActionMailValidator.Error.EmailEmpty" ) );
      return result;
    }
    String realSender = resolve( emailSender );
    if ( smtpCheck ) {
      // check sender
      if ( Utils.isEmpty( realSender ) ) {
        logError( BaseMessages.getString( PKG, "ActionMailValidator.Error.EmailSenderEmpty" ) );
        return result;
      }
    }

    String realDefaultSMTP = resolve( defaultSMTP );
    int timeOut = Const.toInt( resolve( timeout ), 0 );

    // Split the mail-address: separated by variables
    String[] mailsCheck = realEmailAddress.split( " " );
    boolean exitloop = false;
    boolean mailIsValid = false;
    String MailError = null;
    for ( int i = 0; i < mailsCheck.length && !exitloop; i++ ) {
      String email = mailsCheck[ i ];
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ActionMailValidator.CheckingMail", email ) );
      }

      // Check if address is valid
      MailValidationResult resultValidator = MailValidation.isAddressValid( log, email, realSender, realDefaultSMTP, timeOut, smtpCheck );

      mailIsValid = resultValidator.isValide();
      MailError = resultValidator.getErrorMessage();

      if ( log.isDetailed() ) {
        if ( mailIsValid ) {
          logDetailed( BaseMessages.getString( PKG, "ActionMailValidator.MailValid", email ) );
        } else {
          logDetailed( BaseMessages.getString( PKG, "ActionMailValidator.MailNotValid", email ) );
          logDetailed( MailError );
        }

      }
      // invalid mail? exit loop
      if ( !resultValidator.isValide() ) {
        exitloop = true;
      }
    }

    result.setResult( mailIsValid );
    if ( mailIsValid ) {
      result.setNrErrors( 0 );
    }

    // return result

    return result;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables, IHopMetadataProvider metadataProvider ) {

    ActionValidatorUtils.andValidator().validate( this, "emailAddress", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    ActionValidatorUtils.andValidator().validate( this, "emailSender", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator(), ActionValidatorUtils.emailValidator() ) );

    if ( isSMTPCheck() ) {
      ActionValidatorUtils.andValidator().validate( this, "defaultSMTP", remarks, AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    }
  }
}
