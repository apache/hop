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

package org.apache.hop.workflow.actions.mail;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.activation.URLDataSource;
import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.SendFailedException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Describes a Mail Workflow Entry.
 *
 * @author Matt Created on 17-06-2003
 */

@Action(
  id = "MAIL",
  name = "i18n::ActionMail.Name",
  description = "i18n::ActionMail.Description",
  image = "Mail.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Mail",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/mail.html"
)
public class ActionMail extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionMail.class; // For Translator

  private String server;

  private String destination;

  private String destinationCc;

  private String destinationBCc;

  /**
   * Caution : It's sender address and NOT reply address
   **/
  private String replyAddress;

  /**
   * Caution : It's sender name name and NOT reply name
   **/
  private String replyName;

  private String subject;

  private boolean includeDate;

  private String contactPerson;

  private String contactPhone;

  private String comment;

  private boolean includingFiles;

  private int[] fileType;

  private boolean zipFiles;

  private String zipFilename;

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

  /**
   * The encoding to use for reading: null or empty string means system default encoding
   */
  private String encoding;

  /**
   * The reply to addresses
   */
  private String replyToAddresses;

  public String[] embeddedimages;

  public String[] contentids;

  public ActionMail( String n ) {
    super( n, "" );
    allocate( 0 );
  }

  public ActionMail() {
    this( "" );
    allocate( 0 );
  }

  public void allocate( int nrFileTypes ) {
    fileType = new int[ nrFileTypes ];
  }

  public void allocateImages( int nrImages ) {
    embeddedimages = new String[ nrImages ];
    contentids = new String[ nrImages ];
  }

  public Object clone() {
    ActionMail je = (ActionMail) super.clone();
    if ( fileType != null ) {
      int nrFileTypes = fileType.length;
      je.allocate( nrFileTypes );
      System.arraycopy( fileType, 0, je.fileType, 0, nrFileTypes );
    }
    if ( embeddedimages != null ) {
      int nrImages = embeddedimages.length;
      je.allocateImages( nrImages );
      System.arraycopy( embeddedimages, 0, je.embeddedimages, 0, nrImages );
      System.arraycopy( contentids, 0, je.contentids, 0, nrImages );
    }
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 600 );

    retval.append( super.getXml() );

    retval.append( "      " ).append( XmlHandler.addTagValue( "server", server ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "port", port ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "destination", destination ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "destinationCc", destinationCc ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "destinationBCc", destinationBCc ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "replyto", replyAddress ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "replytoname", replyName ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "subject", subject ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "include_date", includeDate ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "contact_person", contactPerson ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "contact_phone", contactPhone ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "comment", comment ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "include_files", includingFiles ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "zip_files", zipFiles ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "zip_name", zipFilename ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "use_auth", usingAuthentication ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "use_secure_auth", usingSecureAuthentication ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "auth_user", authenticationUser ) );
    retval.append( "      " ).append(
      XmlHandler
        .addTagValue( "auth_password", Encr.encryptPasswordIfNotUsingVariables( authenticationPassword ) ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "only_comment", onlySendComment ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "use_HTML", useHTML ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "use_Priority", usePriority ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "encoding", encoding ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "priority", priority ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "importance", importance ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "sensitivity", sensitivity ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "secureconnectiontype", secureConnectionType ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "replyToAddresses", replyToAddresses ) );

    retval.append( "      <filetypes>" );
    if ( fileType != null ) {
      for ( int i = 0; i < fileType.length; i++ ) {
        retval.append( "        " ).append(
          XmlHandler.addTagValue( "filetype", ResultFile.getTypeCode( fileType[ i ] ) ) );
      }
    }
    retval.append( "      </filetypes>" );

    retval.append( "      <embeddedimages>" ).append( Const.CR );
    if ( embeddedimages != null ) {
      for ( int i = 0; i < embeddedimages.length; i++ ) {
        retval.append( "        <embeddedimage>" ).append( Const.CR );
        retval.append( "          " ).append( XmlHandler.addTagValue( "image_name", embeddedimages[ i ] ) );
        retval.append( "          " ).append( XmlHandler.addTagValue( "content_id", contentids[ i ] ) );
        retval.append( "        </embeddedimage>" ).append( Const.CR );
      }
    }
    retval.append( "      </embeddedimages>" ).append( Const.CR );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      setServer( XmlHandler.getTagValue( entrynode, "server" ) );
      setPort( XmlHandler.getTagValue( entrynode, "port" ) );
      setDestination( XmlHandler.getTagValue( entrynode, "destination" ) );
      setDestinationCc( XmlHandler.getTagValue( entrynode, "destinationCc" ) );
      setDestinationBCc( XmlHandler.getTagValue( entrynode, "destinationBCc" ) );
      setReplyAddress( XmlHandler.getTagValue( entrynode, "replyto" ) );
      setReplyName( XmlHandler.getTagValue( entrynode, "replytoname" ) );
      setSubject( XmlHandler.getTagValue( entrynode, "subject" ) );
      setIncludeDate( "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "include_date" ) ) );
      setContactPerson( XmlHandler.getTagValue( entrynode, "contact_person" ) );
      setContactPhone( XmlHandler.getTagValue( entrynode, "contact_phone" ) );
      setComment( XmlHandler.getTagValue( entrynode, "comment" ) );
      setIncludingFiles( "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "include_files" ) ) );

      setUsingAuthentication( "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "use_auth" ) ) );
      setUsingSecureAuthentication( "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "use_secure_auth" ) ) );
      setAuthenticationUser( XmlHandler.getTagValue( entrynode, "auth_user" ) );
      setAuthenticationPassword( Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue(
        entrynode, "auth_password" ) ) );

      setOnlySendComment( "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "only_comment" ) ) );
      setUseHTML( "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "use_HTML" ) ) );

      setUsePriority( "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "use_Priority" ) ) );

      setEncoding( XmlHandler.getTagValue( entrynode, "encoding" ) );
      setPriority( XmlHandler.getTagValue( entrynode, "priority" ) );
      setImportance( XmlHandler.getTagValue( entrynode, "importance" ) );
      setSensitivity( XmlHandler.getTagValue( entrynode, "sensitivity" ) );
      setSecureConnectionType( XmlHandler.getTagValue( entrynode, "secureconnectiontype" ) );

      Node ftsnode = XmlHandler.getSubNode( entrynode, "filetypes" );
      int nrTypes = XmlHandler.countNodes( ftsnode, "filetype" );
      allocate( nrTypes );
      for ( int i = 0; i < nrTypes; i++ ) {
        Node ftnode = XmlHandler.getSubNodeByNr( ftsnode, "filetype", i );
        fileType[ i ] = ResultFile.getType( XmlHandler.getNodeValue( ftnode ) );
      }

      setZipFiles( "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "zip_files" ) ) );
      setZipFilename( XmlHandler.getTagValue( entrynode, "zip_name" ) );
      setReplyToAddresses( XmlHandler.getTagValue( entrynode, "replyToAddresses" ) );

      Node images = XmlHandler.getSubNode( entrynode, "embeddedimages" );

      // How many field embedded images ?
      int nrImages = XmlHandler.countNodes( images, "embeddedimage" );
      allocateImages( nrImages );

      // Read them all...
      for ( int i = 0; i < nrImages; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( images, "embeddedimage", i );

        embeddedimages[ i ] = XmlHandler.getTagValue( fnode, "image_name" );
        contentids[ i ] = XmlHandler.getTagValue( fnode, "content_id" );
      }

    } catch ( HopException xe ) {
      throw new HopXmlException( "Unable to load action of type 'mail' from XML node", xe );
    }
  }

  public void setServer( String s ) {
    server = s;
  }

  public String getServer() {
    return server;
  }

  public void setDestination( String dest ) {
    destination = dest;
  }

  public void setDestinationCc( String destCc ) {
    destinationCc = destCc;
  }

  public void setDestinationBCc( String destBCc ) {
    destinationBCc = destBCc;
  }

  public String getDestination() {
    return destination;
  }

  public String getDestinationCc() {
    return destinationCc;
  }

  public String getDestinationBCc() {

    return destinationBCc;
  }

  public void setReplyAddress( String reply ) {
    replyAddress = reply;
  }

  public String getReplyAddress() {
    return replyAddress;
  }

  public void setReplyName( String replyname ) {
    this.replyName = replyname;
  }

  public String getReplyName() {
    return replyName;
  }

  public void setSubject( String subj ) {
    subject = subj;
  }

  public String getSubject() {
    return subject;
  }

  public void setIncludeDate( boolean incl ) {
    includeDate = incl;
  }

  public boolean getIncludeDate() {
    return includeDate;
  }

  public void setContactPerson( String person ) {
    contactPerson = person;
  }

  public String getContactPerson() {
    return contactPerson;
  }

  public void setContactPhone( String phone ) {
    contactPhone = phone;
  }

  public String getContactPhone() {
    return contactPhone;
  }

  public void setComment( String comm ) {
    comment = comm;
  }

  public String getComment() {
    return comment;
  }

  /**
   * @return the result file types to select for attachment </b>
   * @see ResultFile
   */
  public int[] getFileType() {
    return fileType;
  }

  /**
   * @param fileType the result file types to select for attachment
   * @see ResultFile
   */
  public void setFileType( int[] fileType ) {
    this.fileType = fileType;
  }

  public boolean isIncludingFiles() {
    return includingFiles;
  }

  public void setIncludingFiles( boolean includeFiles ) {
    this.includingFiles = includeFiles;
  }

  /**
   * @return Returns the zipFilename.
   */
  public String getZipFilename() {
    return zipFilename;
  }

  /**
   * @param zipFilename The zipFilename to set.
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
   * @param zipFiles The zipFiles to set.
   */
  public void setZipFiles( boolean zipFiles ) {
    this.zipFiles = zipFiles;
  }

  /**
   * @return Returns the authenticationPassword.
   */
  public String getAuthenticationPassword() {
    return authenticationPassword;
  }

  /**
   * @param authenticationPassword The authenticationPassword to set.
   */
  public void setAuthenticationPassword( String authenticationPassword ) {
    this.authenticationPassword = authenticationPassword;
  }

  /**
   * @return Returns the authenticationUser.
   */
  public String getAuthenticationUser() {
    return authenticationUser;
  }

  /**
   * @param authenticationUser The authenticationUser to set.
   */
  public void setAuthenticationUser( String authenticationUser ) {
    this.authenticationUser = authenticationUser;
  }

  /**
   * @return Returns the usingAuthentication.
   */
  public boolean isUsingAuthentication() {
    return usingAuthentication;
  }

  /**
   * @param usingAuthentication The usingAuthentication to set.
   */
  public void setUsingAuthentication( boolean usingAuthentication ) {
    this.usingAuthentication = usingAuthentication;
  }

  /**
   * @return the onlySendComment flag
   */
  public boolean isOnlySendComment() {
    return onlySendComment;
  }

  /**
   * @param onlySendComment the onlySendComment flag to set
   */
  public void setOnlySendComment( boolean onlySendComment ) {
    this.onlySendComment = onlySendComment;
  }

  /**
   * @return the useHTML flag
   */
  public boolean isUseHTML() {
    return useHTML;
  }

  /**
   * @param useHTML the useHTML to set
   */
  public void setUseHTML( boolean useHTML ) {
    this.useHTML = useHTML;
  }

  /**
   * @return the encoding
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * @return the secure connection type
   */
  public String getSecureConnectionType() {
    return secureConnectionType;
  }

  /**
   * @param secureConnectionType the secure connection type to set
   */
  public void setSecureConnectionType( String secureConnectionType ) {
    this.secureConnectionType = secureConnectionType;
  }

  /**
   * @param encoding the encoding to set
   */
  public void setEncoding( String encoding ) {
    this.encoding = encoding;
  }

  /**
   * @param replyToAddresses the replayToAddresses to set
   */
  public void setReplyToAddresses( String replyToAddresses ) {
    this.replyToAddresses = replyToAddresses;
  }

  /**
   * @return replayToAddresses
   */
  public String getReplyToAddresses() {
    return this.replyToAddresses;
  }

  /**
   * @param usePriority the usePriority to set
   */
  public void setUsePriority( boolean usePriority ) {
    this.usePriority = usePriority;
  }

  /**
   * @return the usePriority flag
   */
  public boolean isUsePriority() {
    return usePriority;
  }

  /**
   * @return the priority
   */
  public String getPriority() {
    return priority;
  }

  /**
   * @param importance the importance to set
   */
  public void setImportance( String importance ) {
    this.importance = importance;
  }

  /**
   * @return the importance
   */
  public String getImportance() {
    return importance;
  }

  public String getSensitivity() {
    return sensitivity;
  }

  public void setSensitivity( String sensitivity ) {
    this.sensitivity = sensitivity;
  }

  /**
   * @param priority the priority to set
   */
  public void setPriority( String priority ) {
    this.priority = priority;
  }

  public Result execute( Result result, int nr ) {
    File masterZipfile = null;

    // Send an e-mail...
    // create some properties and get the default Session
    Properties props = new Properties();
    if ( Utils.isEmpty( server ) ) {
      logError( BaseMessages.getString( PKG, "JobMail.Error.HostNotSpecified" ) );

      result.setNrErrors( 1L );
      result.setResult( false );
      return result;
    }

    String protocol = "smtp";
    if ( usingSecureAuthentication ) {
      if ( secureConnectionType.equals( "TLS" ) ) {
        // Allow TLS authentication
        props.put( "mail.smtp.starttls.enable", "true" );
      } else if (secureConnectionType.equals("TLS 1.2")) {
        // Allow TLS 1.2 authentication
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.ssl.protocols", "TLSv1.2");
      } else {

        protocol = "smtps";
        // required to get rid of a SSL exception :
        // nested exception is:
        // javax.net.ssl.SSLException: Unsupported record version Unknown
        props.put( "mail.smtps.quitwait", "false" );
      }

    }

    props.put( "mail." + protocol + ".host", resolve( server ) );
    if ( !Utils.isEmpty( port ) ) {
      props.put( "mail." + protocol + ".port", resolve( port ) );
    }

    if ( log.isDebug() ) {
      props.put( "mail.debug", "true" );
    }

    if ( usingAuthentication ) {
      props.put( "mail." + protocol + ".auth", "true" );

      /*
       * authenticator = new Authenticator() { protected PasswordAuthentication getPasswordAuthentication() { return new
       * PasswordAuthentication( StringUtil.environmentSubstitute(Const.NVL(authenticationUser, "")),
       * StringUtil.environmentSubstitute(Const.NVL(authenticationPassword, "")) ); } };
       */
    }

    Session session = Session.getInstance( props );
    session.setDebug( log.isDebug() );

    try {
      // create a message
      Message msg = new MimeMessage( session );

      // set message priority
      if ( usePriority ) {
        String priorityInt = "1";
        if ( priority.equals( "low" ) ) {
          priorityInt = "3";
        }
        if ( priority.equals( "normal" ) ) {
          priorityInt = "2";
        }

        msg.setHeader( "X-Priority", priorityInt ); // (String)int between 1= high and 3 = low.
        msg.setHeader( "Importance", importance );
        // seems to be needed for MS Outlook.
        // where it returns a string of high /normal /low.
        msg.setHeader( "Sensitivity", sensitivity );
        // Possible values are normal, personal, private, company-confidential

      }

      // Set Mail sender (From)
      String senderAddress = resolve( replyAddress );
      if ( !Utils.isEmpty( senderAddress ) ) {
        String senderName = resolve( replyName );
        if ( !Utils.isEmpty( senderName ) ) {
          senderAddress = senderName + '<' + senderAddress + '>';
        }
        msg.setFrom( new InternetAddress( senderAddress ) );
      } else {
        throw new MessagingException( BaseMessages.getString( PKG, "JobMail.Error.ReplyEmailNotFilled" ) );
      }

      // set Reply to addresses
      String replyToAddress = resolve( replyToAddresses );
      if ( !Utils.isEmpty( replyToAddress ) ) {
        // Split the mail-address: variables separated
        String[] reply_Address_List = resolve( replyToAddress ).split( " " );
        InternetAddress[] address = new InternetAddress[ reply_Address_List.length ];
        for ( int i = 0; i < reply_Address_List.length; i++ ) {
          address[ i ] = new InternetAddress( reply_Address_List[ i ] );
        }
        msg.setReplyTo( address );
      }

      // Split the mail-address: variables separated
      String[] destinations = resolve( destination ).split( " " );
      InternetAddress[] address = new InternetAddress[ destinations.length ];
      for ( int i = 0; i < destinations.length; i++ ) {
        address[ i ] = new InternetAddress( destinations[ i ] );
      }
      msg.setRecipients( Message.RecipientType.TO, address );

      String realCC = resolve( getDestinationCc() );
      if ( !Utils.isEmpty( realCC ) ) {
        // Split the mail-address Cc: variables separated
        String[] destinationsCc = realCC.split( " " );
        InternetAddress[] addressCc = new InternetAddress[ destinationsCc.length ];
        for ( int i = 0; i < destinationsCc.length; i++ ) {
          addressCc[ i ] = new InternetAddress( destinationsCc[ i ] );
        }

        msg.setRecipients( Message.RecipientType.CC, addressCc );
      }

      String realBCc = resolve( getDestinationBCc() );
      if ( !Utils.isEmpty( realBCc ) ) {
        // Split the mail-address BCc: variables separated
        String[] destinationsBCc = realBCc.split( " " );
        InternetAddress[] addressBCc = new InternetAddress[ destinationsBCc.length ];
        for ( int i = 0; i < destinationsBCc.length; i++ ) {
          addressBCc[ i ] = new InternetAddress( destinationsBCc[ i ] );
        }

        msg.setRecipients( Message.RecipientType.BCC, addressBCc );
      }
      String realSubject = resolve( subject );
      if ( !Utils.isEmpty( realSubject ) ) {
        msg.setSubject( realSubject );
      }

      msg.setSentDate( new Date() );
      StringBuilder messageText = new StringBuilder();
      String endRow = isUseHTML() ? "<br>" : Const.CR;
      String realComment = resolve( comment );
      if ( !Utils.isEmpty( realComment ) ) {
        messageText.append( realComment ).append( Const.CR ).append( Const.CR );
      }
      if ( !onlySendComment ) {

        messageText.append( BaseMessages.getString( PKG, "JobMail.Log.Comment.Workflow" ) ).append( endRow );
        messageText.append( "-----" ).append( endRow );
        messageText.append( BaseMessages.getString( PKG, "JobMail.Log.Comment.JobName" ) + "    : " ).append(
          parentWorkflow.getWorkflowMeta().getName() ).append( endRow );
        messageText.append( BaseMessages.getString( PKG, "JobMail.Log.Comment.Action" ) + "   : " ).append(
          getName() ).append( endRow );
        messageText.append( Const.CR );
      }

      if ( includeDate ) {
        messageText
          .append( endRow ).append( BaseMessages.getString( PKG, "JobMail.Log.Comment.MsgDate" ) + ": " )
          .append( XmlHandler.date2string( new Date() ) ).append( endRow ).append( endRow );
      }
      if ( !onlySendComment && result != null ) {
        messageText.append( BaseMessages.getString( PKG, "JobMail.Log.Comment.PreviousResult" ) + ":" ).append(
          endRow );
        messageText.append( "-----------------" ).append( endRow );
        messageText
          .append( BaseMessages.getString( PKG, "JobMail.Log.Comment.ActionNr" ) + "         : " ).append(
          result.getEntryNr() ).append( endRow );
        messageText
          .append( BaseMessages.getString( PKG, "JobMail.Log.Comment.Errors" ) + "               : " ).append(
          result.getNrErrors() ).append( endRow );
        messageText
          .append( BaseMessages.getString( PKG, "JobMail.Log.Comment.LinesRead" ) + "           : " ).append(
          result.getNrLinesRead() ).append( endRow );
        messageText
          .append( BaseMessages.getString( PKG, "JobMail.Log.Comment.LinesWritten" ) + "        : " ).append(
          result.getNrLinesWritten() ).append( endRow );
        messageText
          .append( BaseMessages.getString( PKG, "JobMail.Log.Comment.LinesInput" ) + "          : " ).append(
          result.getNrLinesInput() ).append( endRow );
        messageText
          .append( BaseMessages.getString( PKG, "JobMail.Log.Comment.LinesOutput" ) + "         : " ).append(
          result.getNrLinesOutput() ).append( endRow );
        messageText
          .append( BaseMessages.getString( PKG, "JobMail.Log.Comment.LinesUpdated" ) + "        : " ).append(
          result.getNrLinesUpdated() ).append( endRow );
        messageText
          .append( BaseMessages.getString( PKG, "JobMail.Log.Comment.LinesRejected" ) + "       : " ).append(
          result.getNrLinesRejected() ).append( endRow );
        messageText.append( BaseMessages.getString( PKG, "JobMail.Log.Comment.Status" ) + "  : " ).append(
          result.getExitStatus() ).append( endRow );
        messageText
          .append( BaseMessages.getString( PKG, "JobMail.Log.Comment.Result" ) + "               : " ).append(
          result.getResult() ).append( endRow );
        messageText.append( endRow );
      }

      if ( !onlySendComment
        && ( !Utils.isEmpty( resolve( contactPerson ) ) || !Utils
        .isEmpty( resolve( contactPhone ) ) ) ) {
        messageText.append( BaseMessages.getString( PKG, "JobMail.Log.Comment.ContactInfo" ) + " :" ).append(
          endRow );
        messageText.append( "---------------------" ).append( endRow );
        messageText.append( BaseMessages.getString( PKG, "JobMail.Log.Comment.PersonToContact" ) + " : " ).append(
          resolve( contactPerson ) ).append( endRow );
        messageText.append( BaseMessages.getString( PKG, "JobMail.Log.Comment.Tel" ) + "  : " ).append(
          resolve( contactPhone ) ).append( endRow );
        messageText.append( endRow );
      }

      // Include the path to this action...
      if ( !onlySendComment ) {
        WorkflowTracker workflowTracker = parentWorkflow.getWorkflowTracker();
        if ( workflowTracker != null ) {
          messageText.append( BaseMessages.getString( PKG, "JobMail.Log.Comment.PathToJobentry" ) + ":" ).append(
            endRow );
          messageText.append( "------------------------" ).append( endRow );

          addBacktracking( workflowTracker, messageText );
          if ( isUseHTML() ) {
            messageText.replace( 0, messageText.length(), messageText.toString().replace( Const.CR, endRow ) );
          }
        }
      }

      MimeMultipart parts = new MimeMultipart();
      MimeBodyPart part1 = new MimeBodyPart(); // put the text in the
      // Attached files counter
      int nrattachedFiles = 0;

      // 1st part

      if ( useHTML ) {
        if ( !Utils.isEmpty( getEncoding() ) ) {
          part1.setContent( messageText.toString(), "text/html; " + "charset=" + getEncoding() );
        } else {
          part1.setContent( messageText.toString(), "text/html; " + "charset=ISO-8859-1" );
        }
      } else {
        part1.setText( messageText.toString() );
      }

      parts.addBodyPart( part1 );

      if ( includingFiles && result != null ) {
        List<ResultFile> resultFiles = result.getResultFilesList();
        if ( resultFiles != null && !resultFiles.isEmpty() ) {
          if ( !zipFiles ) {
            // Add all files to the message...
            //
            for ( ResultFile resultFile : resultFiles ) {
              FileObject file = resultFile.getFile();
              if ( file != null && file.exists() ) {
                boolean found = false;
                for ( int i = 0; i < fileType.length; i++ ) {
                  if ( fileType[ i ] == resultFile.getType() ) {
                    found = true;
                  }
                }
                if ( found ) {
                  // create a data source
                  MimeBodyPart files = new MimeBodyPart();
                  URLDataSource fds = new URLDataSource( file.getURL() );

                  // get a data IHandler to manipulate this file type;
                  files.setDataHandler( new DataHandler( fds ) );
                  // include the file in the data source
                  files.setFileName( file.getName().getBaseName() );

                  // insist on base64 to preserve line endings
                  files.addHeader( "Content-Transfer-Encoding", "base64" );

                  // add the part with the file in the BodyPart();
                  parts.addBodyPart( files );
                  nrattachedFiles++;
                  logBasic( "Added file '" + fds.getName() + "' to the mail message." );
                }
              }
            }
          } else {
            // create a single ZIP archive of all files
            masterZipfile =
              new File( System.getProperty( "java.io.tmpdir" )
                + Const.FILE_SEPARATOR + resolve( zipFilename ) );
            ZipOutputStream zipOutputStream = null;
            try {
              zipOutputStream = new ZipOutputStream( new FileOutputStream( masterZipfile ) );

              for ( ResultFile resultFile : resultFiles ) {
                boolean found = false;
                for ( int i = 0; i < fileType.length; i++ ) {
                  if ( fileType[ i ] == resultFile.getType() ) {
                    found = true;
                  }
                }
                if ( found ) {
                  FileObject file = resultFile.getFile();
                  ZipEntry zipEntry = new ZipEntry( file.getName().getBaseName() );
                  zipOutputStream.putNextEntry( zipEntry );

                  // Now put the content of this file into this archive...
                  BufferedInputStream inputStream = new BufferedInputStream( HopVfs.getInputStream( file ) );
                  try {
                    int c;
                    while ( ( c = inputStream.read() ) >= 0 ) {
                      zipOutputStream.write( c );
                    }
                  } finally {
                    inputStream.close();
                  }
                  zipOutputStream.closeEntry();
                  nrattachedFiles++;
                  logBasic( "Added file '" + file.getName().getURI() + "' to the mail message in a zip archive." );
                }
              }
            } catch ( Exception e ) {
              logError( "Error zipping attachement files into file ["
                + masterZipfile.getPath() + "] : " + e.toString() );
              logError( Const.getStackTracker( e ) );
              result.setNrErrors( 1 );
            } finally {
              if ( zipOutputStream != null ) {
                try {
                  zipOutputStream.finish();
                  zipOutputStream.close();
                } catch ( IOException e ) {
                  logError( "Unable to close attachement zip file archive : " + e.toString() );
                  logError( Const.getStackTracker( e ) );
                  result.setNrErrors( 1 );
                }
              }
            }

            // Now attach the master zip file to the message.
            if ( result.getNrErrors() == 0 ) {
              // create a data source
              MimeBodyPart files = new MimeBodyPart();
              FileDataSource fds = new FileDataSource( masterZipfile );
              // get a data IHandler to manipulate this file type;
              files.setDataHandler( new DataHandler( fds ) );
              // include the file in the data source
              files.setFileName( fds.getName() );
              // add the part with the file in the BodyPart();
              parts.addBodyPart( files );
            }
          }
        }
      }

      int nrEmbeddedImages = 0;
      if ( embeddedimages != null && embeddedimages.length > 0 ) {
        FileObject imageFile = null;
        for ( int i = 0; i < embeddedimages.length; i++ ) {
          String realImageFile = resolve( embeddedimages[ i ] );
          String realcontenID = resolve( contentids[ i ] );
          if ( messageText.indexOf( "cid:" + realcontenID ) < 0 ) {
            if ( log.isDebug() ) {
              log.logDebug( "Image [" + realImageFile + "] is not used in message body!" );
            }
          } else {
            try {
              boolean found = false;
              imageFile = HopVfs.getFileObject( realImageFile );
              if ( imageFile.exists() && imageFile.getType() == FileType.FILE ) {
                found = true;
              } else {
                log.logError( "We can not find [" + realImageFile + "] or it is not a file" );
              }
              if ( found ) {
                // Create part for the image
                MimeBodyPart messageBodyPart = new MimeBodyPart();
                // Load the image
                URLDataSource fds = new URLDataSource( imageFile.getURL() );
                messageBodyPart.setDataHandler( new DataHandler( fds ) );
                // Setting the header
                messageBodyPart.setHeader( "Content-ID", "<" + realcontenID + ">" );
                // Add part to multi-part
                parts.addBodyPart( messageBodyPart );
                nrEmbeddedImages++;
                log.logBasic( "Image '" + fds.getName() + "' was embedded in message." );
              }
            } catch ( Exception e ) {
              log.logError( "Error embedding image [" + realImageFile + "] in message : " + e.toString() );
              log.logError( Const.getStackTracker( e ) );
              result.setNrErrors( 1 );
            } finally {
              if ( imageFile != null ) {
                try {
                  imageFile.close();
                } catch ( Exception e ) { /* Ignore */
                }
              }
            }
          }
        }
      }

      if ( nrEmbeddedImages > 0 && nrattachedFiles == 0 ) {
        // If we need to embedd images...
        // We need to create a "multipart/related" message.
        // otherwise image will appear as attached file
        parts.setSubType( "related" );
      }
      // put all parts together
      msg.setContent( parts );

      Transport transport = null;
      try {
        transport = session.getTransport( protocol );
        String authPass = getPassword( authenticationPassword );

        if ( usingAuthentication ) {
          if ( !Utils.isEmpty( port ) ) {
            transport.connect(
              resolve( Const.NVL( server, "" ) ),
              Integer.parseInt( resolve( Const.NVL( port, "" ) ) ),
              resolve( Const.NVL( authenticationUser, "" ) ),
              authPass );
          } else {
            transport.connect(
              resolve( Const.NVL( server, "" ) ),
              resolve( Const.NVL( authenticationUser, "" ) ),
              authPass );
          }
        } else {
          transport.connect();
        }
        transport.sendMessage( msg, msg.getAllRecipients() );
      } finally {
        if ( transport != null ) {
          transport.close();
        }
      }
    } catch ( IOException e ) {
      logError( "Problem while sending message: " + e.toString() );
      result.setNrErrors( 1 );
    } catch ( MessagingException mex ) {
      logError( "Problem while sending message: " + mex.toString() );
      result.setNrErrors( 1 );

      Exception ex = mex;
      do {
        if ( ex instanceof SendFailedException ) {
          SendFailedException sfex = (SendFailedException) ex;

          Address[] invalid = sfex.getInvalidAddresses();
          if ( invalid != null ) {
            logError( "    ** Invalid Addresses" );
            for ( int i = 0; i < invalid.length; i++ ) {
              logError( "         " + invalid[ i ] );
              result.setNrErrors( 1 );
            }
          }

          Address[] validUnsent = sfex.getValidUnsentAddresses();
          if ( validUnsent != null ) {
            logError( "    ** ValidUnsent Addresses" );
            for ( int i = 0; i < validUnsent.length; i++ ) {
              logError( "         " + validUnsent[ i ] );
              result.setNrErrors( 1 );
            }
          }

          Address[] validSent = sfex.getValidSentAddresses();
          if ( validSent != null ) {
            for ( int i = 0; i < validSent.length; i++ ) {
              logError( "         " + validSent[ i ] );
              result.setNrErrors( 1 );
            }
          }
        }
        if ( ex instanceof MessagingException ) {
          ex = ( (MessagingException) ex ).getNextException();
        } else {
          ex = null;
        }
      } while ( ex != null );
    } finally {
      if ( masterZipfile != null && masterZipfile.exists() ) {
        masterZipfile.delete();
      }
    }

    if ( result.getNrErrors() > 0 ) {
      result.setResult( false );
    } else {
      result.setResult( true );
    }

    return result;
  }

  private void addBacktracking( WorkflowTracker workflowTracker, StringBuilder messageText ) {
    addBacktracking( workflowTracker, messageText, 0 );
  }

  private void addBacktracking( WorkflowTracker workflowTracker, StringBuilder messageText, int level ) {
    int nr = workflowTracker.nrWorkflowTrackers();

    messageText.append( Const.rightPad( " ", level * 2 ) );
    messageText.append( Const.NVL( workflowTracker.getWorkflowName(), "-" ) );
    ActionResult jer = workflowTracker.getActionResult();
    if ( jer != null ) {
      messageText.append( " : " );
      if ( jer.getActionName() != null ) {
        messageText.append( " : " );
        messageText.append( jer.getActionName() );
      }
      if ( jer.getResult() != null ) {
        messageText.append( " : " );
        messageText.append( "[" + jer.getResult().toString() + "]" );
      }
      if ( jer.getReason() != null ) {
        messageText.append( " : " );
        messageText.append( jer.getReason() );
      }
      if ( jer.getComment() != null ) {
        messageText.append( " : " );
        messageText.append( jer.getComment() );
      }
      if ( jer.getLogDate() != null ) {
        messageText.append( " (" );
        messageText.append( XmlHandler.date2string( jer.getLogDate() ) );
        messageText.append( ')' );
      }
    }
    messageText.append( Const.CR );

    for ( int i = 0; i < nr; i++ ) {
      WorkflowTracker jt = workflowTracker.getWorkflowTracker( i );
      addBacktracking( jt, messageText, level + 1 );
    }
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  public boolean isUnconditional() {
    return true;
  }

  /**
   * @return the usingSecureAuthentication
   */
  public boolean isUsingSecureAuthentication() {
    return usingSecureAuthentication;
  }

  /**
   * @param usingSecureAuthentication the usingSecureAuthentication to set
   */
  public void setUsingSecureAuthentication( boolean usingSecureAuthentication ) {
    this.usingSecureAuthentication = usingSecureAuthentication;
  }

  /**
   * @return the port
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port the port to set
   */
  public void setPort( String port ) {
    this.port = port;
  }

  public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( variables, workflowMeta );
    String realServername = resolve( server );
    ResourceReference reference = new ResourceReference( this );
    reference.getEntries().add( new ResourceEntry( realServername, ResourceType.SERVER ) );
    references.add( reference );
    return references;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {

    ActionValidatorUtils.andValidator().validate( this, "server", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    ActionValidatorUtils.andValidator()
      .validate( this, "replyAddress", remarks, AndValidator.putValidators(
        ActionValidatorUtils.notBlankValidator(), ActionValidatorUtils.emailValidator() ) );

    ActionValidatorUtils.andValidator().validate( this, "destination", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );

    if ( usingAuthentication ) {
      ActionValidatorUtils.andValidator().validate( this, "authenticationUser", remarks,
        AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
      ActionValidatorUtils.andValidator().validate( this, "authenticationPassword", remarks,
        AndValidator.putValidators( ActionValidatorUtils.notNullValidator() ) );
    }

    ActionValidatorUtils.andValidator().validate( this, "port", remarks,
      AndValidator.putValidators( ActionValidatorUtils.integerValidator() ) );

  }

  public String getPassword( String authPassword ) {
    return Encr.decryptPasswordOptionallyEncrypted(
      resolve( Const.NVL( authPassword, "" ) ) );
  }

}
