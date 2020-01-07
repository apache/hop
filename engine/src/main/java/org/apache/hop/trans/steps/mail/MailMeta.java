/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans.steps.mail;

import java.util.List;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;

import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * Send mail step. based on Mail job entry
 *
 * @author Samatar
 * @since 28-07-2008
 */

public class MailMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = MailMeta.class; // for i18n purposes, needed by Translator2!!

  private String server;

  private String destination;

  private String destinationCc;

  private String destinationBCc;

  /** Caution : this is not the reply to addresses but the mail sender name */
  private String replyAddress;

  /** Caution : this is not the reply to addresses but the mail sender */
  private String replyName;

  private String subject;

  private boolean includeDate;

  private boolean includeSubFolders;

  private boolean zipFilenameDynamic;

  private boolean isFilenameDynamic;

  private String dynamicFieldname;

  private String dynamicWildcard;

  private String dynamicZipFilename;

  private String sourcefilefoldername;

  private String sourcewildcard;

  private String contactPerson;

  private String contactPhone;

  private String comment;

  private boolean includingFiles;

  private boolean zipFiles;

  private String zipFilename;

  private String ziplimitsize;

  private boolean usingAuthentication;

  private String authenticationUser;

  private String authenticationPassword;

  private boolean onlySendComment;

  private boolean useHTML;

  private boolean usingSecureAuthentication;

  private boolean usePriority;

  private String port;

  private String priority;

  private String importance;

  private String sensitivity;

  private String secureConnectionType;

  /** The encoding to use for reading: null or empty string means system default encoding */
  private String encoding;

  /** The reply to addresses */
  private String replyToAddresses;

  private String[] embeddedimages;

  private String[] contentids;

  /** Flag : attach file from content defined in a field **/
  private boolean attachContentFromField;

  /** file content field name **/
  private String attachContentField;

  /** filename content field **/
  private String attachContentFileNameField;

  public MailMeta() {
    super(); // allocate BaseStepMeta
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public void allocate( int value ) {
    this.embeddedimages = new String[value];
    this.contentids = new String[value];
  }

  private void readData( Node stepnode ) {
    setServer( XMLHandler.getTagValue( stepnode, "server" ) );
    setPort( XMLHandler.getTagValue( stepnode, "port" ) );
    setDestination( XMLHandler.getTagValue( stepnode, "destination" ) );
    setDestinationCc( XMLHandler.getTagValue( stepnode, "destinationCc" ) );
    setDestinationBCc( XMLHandler.getTagValue( stepnode, "destinationBCc" ) );
    setReplyToAddresses( XMLHandler.getTagValue( stepnode, "replyToAddresses" ) );
    setReplyAddress( XMLHandler.getTagValue( stepnode, "replyto" ) );
    setReplyName( XMLHandler.getTagValue( stepnode, "replytoname" ) );
    setSubject( XMLHandler.getTagValue( stepnode, "subject" ) );
    setIncludeDate( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "include_date" ) ) );
    setIncludeSubFolders( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "include_subfolders" ) ) );
    setZipFilenameDynamic( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "zipFilenameDynamic" ) ) );
    setisDynamicFilename( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "isFilenameDynamic" ) ) );
    setDynamicFieldname( XMLHandler.getTagValue( stepnode, "dynamicFieldname" ) );
    setDynamicWildcard( XMLHandler.getTagValue( stepnode, "dynamicWildcard" ) );
    setAttachContentFromField( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "attachContentFromField" ) ) );
    setAttachContentField( XMLHandler.getTagValue( stepnode, "attachContentField" ) );
    setAttachContentFileNameField( XMLHandler.getTagValue( stepnode, "attachContentFileNameField" ) );

    setDynamicZipFilenameField( XMLHandler.getTagValue( stepnode, "dynamicZipFilename" ) );
    setSourceFileFoldername( XMLHandler.getTagValue( stepnode, "sourcefilefoldername" ) );
    setSourceWildcard( XMLHandler.getTagValue( stepnode, "sourcewildcard" ) );
    setContactPerson( XMLHandler.getTagValue( stepnode, "contact_person" ) );
    setContactPhone( XMLHandler.getTagValue( stepnode, "contact_phone" ) );
    setComment( XMLHandler.getTagValue( stepnode, "comment" ) );
    setIncludingFiles( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "include_files" ) ) );
    setUsingAuthentication( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "use_auth" ) ) );
    setUsingSecureAuthentication( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "use_secure_auth" ) ) );
    setAuthenticationUser( XMLHandler.getTagValue( stepnode, "auth_user" ) );
    setAuthenticationPassword( Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue(
      stepnode, "auth_password" ) ) );
    setOnlySendComment( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "only_comment" ) ) );
    setUseHTML( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "use_HTML" ) ) );
    setUsePriority( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "use_Priority" ) ) );
    setEncoding( XMLHandler.getTagValue( stepnode, "encoding" ) );
    setPriority( XMLHandler.getTagValue( stepnode, "priority" ) );
    setImportance( XMLHandler.getTagValue( stepnode, "importance" ) );
    setSensitivity( XMLHandler.getTagValue( stepnode, "sensitivity" ) );
    setSecureConnectionType( XMLHandler.getTagValue( stepnode, "secureconnectiontype" ) );
    setZipFiles( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "zip_files" ) ) );
    setZipFilename( XMLHandler.getTagValue( stepnode, "zip_name" ) );
    setZipLimitSize( XMLHandler.getTagValue( stepnode, "zip_limit_size" ) );

    Node images = XMLHandler.getSubNode( stepnode, "embeddedimages" );
    // How many field embedded images ?
    int nrImages = XMLHandler.countNodes( images, "embeddedimage" );

    allocate( nrImages );

    // Read them all...
    for ( int i = 0; i < nrImages; i++ ) {
      Node fnode = XMLHandler.getSubNodeByNr( images, "embeddedimage", i );

      embeddedimages[i] = XMLHandler.getTagValue( fnode, "image_name" );
      contentids[i] = XMLHandler.getTagValue( fnode, "content_id" );
    }
  }

  public void setEmbeddedImage( int i, String value ) {
    embeddedimages[i] = value;
  }

  public void setEmbeddedImages( String[] value ) {
    this.embeddedimages = value;
  }

  public void setContentIds( int i, String value ) {
    contentids[i] = value;
  }

  public void setContentIds( String[] value ) {
    this.contentids = value;
  }

  @Override
  public void setDefault() {
  }

  @Override
  public String getXML() throws HopException {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( super.getXML() );

    retval.append( "      " ).append( XMLHandler.addTagValue( "server", this.server ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "port", this.port ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "destination", this.destination ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "destinationCc", this.destinationCc ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "destinationBCc", this.destinationBCc ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "replyToAddresses", this.replyToAddresses ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "replyto", this.replyAddress ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "replytoname", this.replyName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "subject", this.subject ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "include_date", this.includeDate ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "include_subfolders", this.includeSubFolders ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "zipFilenameDynamic", this.zipFilenameDynamic ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "isFilenameDynamic", this.isFilenameDynamic ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "attachContentFromField", this.attachContentFromField ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "attachContentField", this.attachContentField ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "attachContentFileNameField", this.attachContentFileNameField ) );

    retval.append( "      " ).append( XMLHandler.addTagValue( "dynamicFieldname", this.dynamicFieldname ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "dynamicWildcard", this.dynamicWildcard ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "dynamicZipFilename", this.dynamicZipFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "sourcefilefoldername", this.sourcefilefoldername ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "sourcewildcard", this.sourcewildcard ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "contact_person", this.contactPerson ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "contact_phone", this.contactPhone ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "comment", this.comment ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "include_files", this.includingFiles ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "zip_files", this.zipFiles ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "zip_name", this.zipFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "zip_limit_size", this.ziplimitsize ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "use_auth", this.usingAuthentication ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "use_secure_auth", this.usingSecureAuthentication ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "auth_user", this.authenticationUser ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "auth_password", Encr
        .encryptPasswordIfNotUsingVariables( this.authenticationPassword ) ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "only_comment", this.onlySendComment ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "use_HTML", this.useHTML ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "use_Priority", this.usePriority ) );
    retval.append( "    " + XMLHandler.addTagValue( "encoding", this.encoding ) );
    retval.append( "    " + XMLHandler.addTagValue( "priority", this.priority ) );
    retval.append( "    " + XMLHandler.addTagValue( "importance", this.importance ) );
    retval.append( "    " + XMLHandler.addTagValue( "sensitivity", this.sensitivity ) );
    retval.append( "    " + XMLHandler.addTagValue( "secureconnectiontype", this.secureConnectionType ) );

    retval.append( "      <embeddedimages>" ).append( Const.CR );
    if ( embeddedimages != null ) {
      for ( int i = 0; i < embeddedimages.length; i++ ) {
        retval.append( "        <embeddedimage>" ).append( Const.CR );
        retval.append( "          " ).append( XMLHandler.addTagValue( "image_name", embeddedimages[i] ) );
        retval.append( "          " ).append( XMLHandler.addTagValue( "content_id", contentids[i] ) );
        retval.append( "        </embeddedimage>" ).append( Const.CR );
      }
    }
    retval.append( "      </embeddedimages>" ).append( Const.CR );

    return retval.toString();
  }

  public void setServer( String s ) {
    this.server = s;
  }

  public String getServer() {
    return this.server;
  }

  public void setDestination( String dest ) {
    this.destination = dest;
  }

  public void setDestinationCc( String destCc ) {
    this.destinationCc = destCc;
  }

  public void setDestinationBCc( String destBCc ) {
    this.destinationBCc = destBCc;
  }

  public String getDestination() {
    return this.destination;
  }

  public String getDestinationCc() {
    return this.destinationCc;
  }

  public String getDestinationBCc() {

    return this.destinationBCc;
  }

  public void setReplyAddress( String reply ) {
    this.replyAddress = reply;
  }

  public String getReplyAddress() {
    return this.replyAddress;
  }

  public void setReplyName( String replyname ) {
    this.replyName = replyname;
  }

  public String getReplyName() {
    return this.replyName;
  }

  public void setSubject( String subj ) {
    this.subject = subj;
  }

  public String getSubject() {
    return this.subject;
  }

  public void setIncludeDate( boolean incl ) {
    this.includeDate = incl;
  }

  public void setIncludeSubFolders( boolean incl ) {
    this.includeSubFolders = incl;
  }

  public boolean isIncludeSubFolders() {
    return this.includeSubFolders;
  }

  public String[] getEmbeddedImages() {
    return embeddedimages;
  }

  public String[] getContentIds() {
    return contentids;
  }

  public boolean isZipFilenameDynamic() {
    return this.zipFilenameDynamic;
  }

  public void setZipFilenameDynamic( boolean isdynamic ) {
    this.zipFilenameDynamic = isdynamic;
  }

  public void setisDynamicFilename( boolean isdynamic ) {
    this.isFilenameDynamic = isdynamic;
  }

  public void setAttachContentFromField( boolean attachContentFromField ) {
    this.attachContentFromField = attachContentFromField;
  }

  public void setAttachContentField( String attachContentField ) {
    this.attachContentField = attachContentField;
  }

  public void setAttachContentFileNameField( String attachContentFileNameField ) {
    this.attachContentFileNameField = attachContentFileNameField;
  }

  public void setDynamicWildcard( String dynamicwildcard ) {
    this.dynamicWildcard = dynamicwildcard;
  }

  public void setDynamicZipFilenameField( String dynamiczipfilename ) {
    this.dynamicZipFilename = dynamiczipfilename;
  }

  public String getDynamicZipFilenameField() {
    return this.dynamicZipFilename;
  }

  public String getDynamicWildcard() {
    return this.dynamicWildcard;
  }

  public void setSourceFileFoldername( String sourcefile ) {
    this.sourcefilefoldername = sourcefile;
  }

  public String getSourceFileFoldername() {
    return this.sourcefilefoldername;
  }

  public void setSourceWildcard( String wildcard ) {
    this.sourcewildcard = wildcard;
  }

  public String getSourceWildcard() {
    return this.sourcewildcard;
  }

  public void setDynamicFieldname( String dynamicfield ) {
    this.dynamicFieldname = dynamicfield;
  }

  public String getDynamicFieldname() {
    return this.dynamicFieldname;
  }

  public boolean getIncludeDate() {
    return this.includeDate;
  }

  public boolean isDynamicFilename() {
    return this.isFilenameDynamic;
  }

  public boolean isAttachContentFromField() {
    return this.attachContentFromField;
  }

  public String getAttachContentField() {
    return this.attachContentField;
  }

  public String getAttachContentFileNameField() {
    return this.attachContentFileNameField;
  }

  public void setContactPerson( String person ) {
    this.contactPerson = person;
  }

  public String getContactPerson() {
    return this.contactPerson;
  }

  public void setContactPhone( String phone ) {
    this.contactPhone = phone;
  }

  public String getContactPhone() {
    return this.contactPhone;
  }

  public void setComment( String comm ) {
    this.comment = comm;
  }

  public String getComment() {
    return this.comment;
  }

  public boolean isIncludingFiles() {
    return this.includingFiles;
  }

  public void setIncludingFiles( boolean includeFiles ) {
    this.includingFiles = includeFiles;
  }

  /**
   * @return Returns the zipFilename.
   */
  public String getZipFilename() {
    return this.zipFilename;
  }

  /**
   * @return Returns the ziplimitsize.
   */
  public String getZipLimitSize() {
    return this.ziplimitsize;
  }

  /**
   * @param ziplimitsize
   *          The ziplimitsize to set.
   */
  public void setZipLimitSize( String ziplimitsize ) {
    this.ziplimitsize = ziplimitsize;
  }

  /**
   * @param zipFilename
   *          The zipFilename to set.
   */
  public void setZipFilename( String zipFilename ) {
    this.zipFilename = zipFilename;
  }

  /**
   * @return Returns the zipFiles.
   */
  public boolean isZipFiles() {
    return zipFiles;
  }

  /**
   * @param zipFiles
   *          The zipFiles to set.
   */
  public void setZipFiles( boolean zipFiles ) {
    this.zipFiles = zipFiles;
  }

  /**
   * @return Returns the authenticationPassword.
   */
  public String getAuthenticationPassword() {
    return this.authenticationPassword;
  }

  /**
   * @param authenticationPassword
   *          The authenticationPassword to set.
   */
  public void setAuthenticationPassword( String authenticationPassword ) {
    this.authenticationPassword = authenticationPassword;
  }

  /**
   * @return Returns the authenticationUser.
   */
  public String getAuthenticationUser() {
    return this.authenticationUser;
  }

  /**
   * @param authenticationUser
   *          The authenticationUser to set.
   */
  public void setAuthenticationUser( String authenticationUser ) {
    this.authenticationUser = authenticationUser;
  }

  /**
   * @return Returns the usingAuthentication.
   */
  public boolean isUsingAuthentication() {
    return this.usingAuthentication;
  }

  /**
   * @param usingAuthentication
   *          The usingAuthentication to set.
   */
  public void setUsingAuthentication( boolean usingAuthentication ) {
    this.usingAuthentication = usingAuthentication;
  }

  /**
   * @return the onlySendComment flag
   */
  public boolean isOnlySendComment() {
    return this.onlySendComment;
  }

  /**
   * @param onlySendComment
   *          the onlySendComment flag to set
   */
  public void setOnlySendComment( boolean onlySendComment ) {
    this.onlySendComment = onlySendComment;
  }

  /**
   * @return the useHTML flag
   */
  public boolean isUseHTML() {
    return this.useHTML;
  }

  /**
   * @param useHTML
   *          the useHTML to set
   */
  public void setUseHTML( boolean useHTML ) {
    this.useHTML = useHTML;
  }

  /**
   * @return the encoding
   */
  public String getEncoding() {
    return this.encoding;
  }

  /**
   * @return the secure connection type
   */
  public String getSecureConnectionType() {
    return this.secureConnectionType;
  }

  /**
   * @param secureConnectionType
   *          the secureconnectiontype to set
   */
  public void setSecureConnectionType( String secureConnectionType ) {
    this.secureConnectionType = secureConnectionType;
  }

  /**
   * @param replyToAddresses
   *          the replyToAddresses to set
   */
  public void setReplyToAddresses( String replyToAddresses ) {
    this.replyToAddresses = replyToAddresses;
  }

  /**
   * @return the secure replyToAddresses
   */
  public String getReplyToAddresses() {
    return this.replyToAddresses;
  }

  /**
   * @param encoding
   *          the encoding to set
   */
  public void setEncoding( String encoding ) {
    this.encoding = encoding;
  }

  /**
   * @return the usingSecureAuthentication
   */
  public boolean isUsingSecureAuthentication() {
    return this.usingSecureAuthentication;
  }

  /**
   * @param usingSecureAuthentication
   *          the usingSecureAuthentication to set
   */
  public void setUsingSecureAuthentication( boolean usingSecureAuthentication ) {
    this.usingSecureAuthentication = usingSecureAuthentication;
  }

  /**
   * @return the port
   */
  public String getPort() {
    return this.port;
  }

  /**
   * @param port
   *          the port to set
   */
  public void setPort( String port ) {
    this.port = port;
  }

  /**
   * @param usePriority
   *          the usePriority to set
   */
  public void setUsePriority( boolean usePriority ) {
    this.usePriority = usePriority;
  }

  /**
   * @return the usePriority flag
   */
  public boolean isUsePriority() {
    return this.usePriority;
  }

  /**
   * @return the priority
   */
  public String getPriority() {
    return this.priority;
  }

  /**
   * @param importance
   *          the importance to set
   */
  public void setImportance( String importancein ) {
    this.importance = importancein;
  }

  /**
   * @return the importance
   */
  public String getImportance() {
    return this.importance;
  }

  public String getSensitivity() {
    return sensitivity;
  }

  public void setSensitivity( String sensitivity ) {
    this.sensitivity = sensitivity;
  }

  /**
   * @param priority
   *          the priority to set
   */
  public void setPriority( String priority ) {
    this.priority = priority;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.NotReceivingFields" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );
    }
    remarks.add( cr );

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.StepRecevingData2" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
    }
    remarks.add( cr );

    // Servername
    if ( Utils.isEmpty( server ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.ServerEmpty" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.ServerOk" ), stepMeta );
      remarks.add( cr );
      // is the field exists?
      if ( prev.indexOfValue( transMeta.environmentSubstitute( server ) ) < 0 ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
            PKG, "MailMeta.CheckResult.ServerFieldNotFound", server ), stepMeta );
      }
      remarks.add( cr );
    }

    // port number
    if ( Utils.isEmpty( port ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.PortEmpty" ), stepMeta );
    } else {
      cr =
        new CheckResult(
          CheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG, "MailMeta.CheckResult.PortOk" ), stepMeta );
    }
    remarks.add( cr );

    // reply address
    if ( Utils.isEmpty( replyAddress ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.ReplayAddressEmpty" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.ReplayAddressOk" ), stepMeta );
    }
    remarks.add( cr );

    // Destination
    if ( Utils.isEmpty( destination ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.DestinationEmpty" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.DestinationOk" ), stepMeta );
    }
    remarks.add( cr );

    // Subject
    if ( Utils.isEmpty( subject ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.SubjectEmpty" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.SubjectOk" ), stepMeta );
    }
    remarks.add( cr );

    // Comment
    if ( Utils.isEmpty( comment ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.CommentEmpty" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MailMeta.CheckResult.CommentEmpty" ), stepMeta );
    }
    remarks.add( cr );

    if ( isFilenameDynamic ) {
      // Dynamic Filename field
      if ( Utils.isEmpty( dynamicFieldname ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "MailMeta.CheckResult.DynamicFilenameFieldEmpty" ), stepMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "MailMeta.CheckResult.DynamicFilenameFieldOk" ), stepMeta );
      }
      remarks.add( cr );

    } else {
      // static filename
      if ( Utils.isEmpty( sourcefilefoldername ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "MailMeta.CheckResult.SourceFilenameEmpty" ), stepMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "MailMeta.CheckResult.SourceFilenameOk" ), stepMeta );
      }
      remarks.add( cr );
    }

    if ( isZipFiles() ) {
      if ( isFilenameDynamic ) {
        // dynamic zipfilename
        if ( Utils.isEmpty( getDynamicZipFilenameField() ) ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
              PKG, "MailMeta.CheckResult.DynamicZipfilenameEmpty" ), stepMeta );
        } else {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "MailMeta.CheckResult.DynamicZipfilenameOK" ), stepMeta );
        }
        remarks.add( cr );

      } else {
        // static zipfilename
        if ( Utils.isEmpty( zipFilename ) ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
              PKG, "MailMeta.CheckResult.ZipfilenameEmpty" ), stepMeta );
        } else {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
              PKG, "MailMeta.CheckResult.ZipfilenameOk" ), stepMeta );
        }
        remarks.add( cr );
      }
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
    Trans trans ) {
    return new Mail( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new MailData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
