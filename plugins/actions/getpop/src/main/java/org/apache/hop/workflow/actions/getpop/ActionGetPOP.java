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

package org.apache.hop.workflow.actions.getpop;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import javax.mail.Flags.Flag;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This defines an get pop action.
 *
 * @author Samatar
 * @since 01-03-2007
 */

@Action(
  id = "GET_POP",
  name = "i18n::ActionGetPOP.Name",
  description = "i18n::ActionGetPOP.Description",
  image = "GetPOP.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Mail",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/getpop.html"
)
public class ActionGetPOP extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionGetPOP.class; // For Translator

  static final int FOLDER_OUTPUT = 0;
  static final int FOLDER_ATTACHMENTS = 1;

  public int actiontype;

  public int conditionReceivedDate;

  public int valueimaplist;

  public int aftergetimap;

  private String servername;
  private String username;
  private String password;
  private boolean usessl;
  private String sslport;
  private boolean useproxy;
  private String proxyusername;
  private String outputdirectory;
  private String filenamepattern;
  private String firstmails;
  public int retrievemails;
  private boolean delete;
  private String protocol;
  private boolean saveattachment;
  private boolean savemessage;
  private boolean usedifferentfolderforattachment;
  private String attachmentfolder;
  private String attachmentwildcard;
  private String imapfirstmails;
  private String imapfolder;
  // search term
  private String senderSearch;
  private boolean notTermSenderSearch;
  private String receipientSearch;
  private String subjectSearch;
  private String bodySearch;
  private boolean notTermBodySearch;
  private String receivedDate1;
  private String receivedDate2;
  private boolean notTermSubjectSearch;
  private boolean notTermReceipientSearch;
  private boolean notTermReceivedDateSearch;
  private boolean includesubfolders;
  private String moveToIMAPFolder;
  private boolean createmovetofolder;
  private boolean createlocalfolder;

  private static final String DEFAULT_FILE_NAME_PATTERN = "name_{SYS|hhmmss_MMddyyyy|}_#IdFile#.mail";

  public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
  private static final String FILENAME_ID_PATTERN = "#IdFile#";
  private static final String FILENAME_SYS_DATE_OPEN = "{SYS|";
  private static final String FILENAME_SYS_DATE_CLOSE = "|}";

  private Pattern attachementPattern;

  public ActionGetPOP( String n ) {
    super( n, "" );
    servername = null;
    username = null;
    password = null;
    usessl = false;
    sslport = null;
    useproxy = false;
    proxyusername = null;
    outputdirectory = null;
    filenamepattern = DEFAULT_FILE_NAME_PATTERN;
    retrievemails = 0;
    firstmails = null;
    delete = false;
    protocol = MailConnectionMeta.PROTOCOL_STRING_POP3;
    saveattachment = true;
    savemessage = true;
    usedifferentfolderforattachment = false;
    attachmentfolder = null;
    attachmentwildcard = null;
    imapfirstmails = "0";
    valueimaplist = MailConnectionMeta.VALUE_IMAP_LIST_ALL;
    imapfolder = null;
    // search term
    senderSearch = null;
    notTermSenderSearch = false;
    notTermReceipientSearch = false;
    notTermSubjectSearch = false;
    bodySearch = null;
    notTermBodySearch = false;
    receivedDate1 = null;
    receivedDate2 = null;
    notTermReceivedDateSearch = false;
    receipientSearch = null;
    subjectSearch = null;
    actiontype = MailConnectionMeta.ACTION_TYPE_GET;
    moveToIMAPFolder = null;
    createmovetofolder = false;
    createlocalfolder = false;
    aftergetimap = MailConnectionMeta.AFTER_GET_IMAP_NOTHING;
    includesubfolders = false;
  }

  public ActionGetPOP() {
    this( "" );
  }

  public Object clone() {
    ActionGetPOP je = (ActionGetPOP) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 550 );
    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "servername", servername ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "username", username ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( password ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "usessl", usessl ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "sslport", sslport ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "outputdirectory", outputdirectory ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "filenamepattern", filenamepattern ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "retrievemails", retrievemails ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "firstmails", firstmails ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "delete", delete ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "savemessage", savemessage ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "saveattachment", saveattachment ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "usedifferentfolderforattachment", usedifferentfolderforattachment ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "protocol", protocol ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "attachmentfolder", attachmentfolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "attachmentwildcard", attachmentwildcard ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "valueimaplist", MailConnectionMeta.getValueImapListCode( valueimaplist ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "imapfirstmails", imapfirstmails ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "imapfolder", imapfolder ) );
    // search term
    retval.append( "      " ).append( XmlHandler.addTagValue( "sendersearch", senderSearch ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "nottermsendersearch", notTermSenderSearch ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "receipientsearch", receipientSearch ) );
    retval
      .append( "      " ).append( XmlHandler.addTagValue( "nottermreceipientsearch", notTermReceipientSearch ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "subjectsearch", subjectSearch ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "nottermsubjectsearch", notTermSubjectSearch ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "bodysearch", bodySearch ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "nottermbodysearch", notTermBodySearch ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "conditionreceiveddate", MailConnectionMeta
        .getConditionDateCode( conditionReceivedDate ) ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "nottermreceiveddatesearch", notTermReceivedDateSearch ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "receiveddate1", receivedDate1 ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "receiveddate2", receivedDate2 ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "actiontype", MailConnectionMeta.getActionTypeCode( actiontype ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "movetoimapfolder", moveToIMAPFolder ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "createmovetofolder", createmovetofolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "createlocalfolder", createlocalfolder ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "aftergetimap", MailConnectionMeta.getAfterGetIMAPCode( aftergetimap ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "includesubfolders", includesubfolders ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "useproxy", useproxy ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "proxyusername", proxyusername ) );
    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      servername = XmlHandler.getTagValue( entrynode, "servername" );
      username = XmlHandler.getTagValue( entrynode, "username" );
      password = Encr.decryptPasswordOptionallyEncrypted( XmlHandler.getTagValue( entrynode, "password" ) );
      usessl = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "usessl" ) );
      sslport = XmlHandler.getTagValue( entrynode, "sslport" );
      outputdirectory = XmlHandler.getTagValue( entrynode, "outputdirectory" );
      filenamepattern = XmlHandler.getTagValue( entrynode, "filenamepattern" );
      if ( Utils.isEmpty( filenamepattern ) ) {
        filenamepattern = DEFAULT_FILE_NAME_PATTERN;
      }
      retrievemails = Const.toInt( XmlHandler.getTagValue( entrynode, "retrievemails" ), -1 );
      firstmails = XmlHandler.getTagValue( entrynode, "firstmails" );
      delete = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "delete" ) );

      protocol =
        Const.NVL( XmlHandler.getTagValue( entrynode, "protocol" ), MailConnectionMeta.PROTOCOL_STRING_POP3 );

      String sm = XmlHandler.getTagValue( entrynode, "savemessage" );
      if ( Utils.isEmpty( sm ) ) {
        savemessage = true;
      } else {
        savemessage = "Y".equalsIgnoreCase( sm );
      }

      String sa = XmlHandler.getTagValue( entrynode, "saveattachment" );
      if ( Utils.isEmpty( sa ) ) {
        saveattachment = true;
      } else {
        saveattachment = "Y".equalsIgnoreCase( sa );
      }

      usedifferentfolderforattachment =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "usedifferentfolderforattachment" ) );
      attachmentfolder = XmlHandler.getTagValue( entrynode, "attachmentfolder" );
      attachmentwildcard = XmlHandler.getTagValue( entrynode, "attachmentwildcard" );
      valueimaplist =
        MailConnectionMeta.getValueImapListByCode( Const.NVL( XmlHandler
          .getTagValue( entrynode, "valueimaplist" ), "" ) );
      imapfirstmails = XmlHandler.getTagValue( entrynode, "imapfirstmails" );
      imapfolder = XmlHandler.getTagValue( entrynode, "imapfolder" );
      // search term
      senderSearch = XmlHandler.getTagValue( entrynode, "sendersearch" );
      notTermSenderSearch = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "nottermsendersearch" ) );
      receipientSearch = XmlHandler.getTagValue( entrynode, "receipientsearch" );
      notTermReceipientSearch =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "nottermreceipientsearch" ) );
      subjectSearch = XmlHandler.getTagValue( entrynode, "subjectsearch" );
      notTermSubjectSearch = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "nottermsubjectsearch" ) );
      bodySearch = XmlHandler.getTagValue( entrynode, "bodysearch" );
      notTermBodySearch = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "nottermbodysearch" ) );
      conditionReceivedDate =
        MailConnectionMeta.getConditionByCode( Const.NVL( XmlHandler.getTagValue(
          entrynode, "conditionreceiveddate" ), "" ) );
      notTermReceivedDateSearch =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "nottermreceiveddatesearch" ) );
      receivedDate1 = XmlHandler.getTagValue( entrynode, "receivedDate1" );
      receivedDate2 = XmlHandler.getTagValue( entrynode, "receivedDate2" );
      actiontype =
        MailConnectionMeta.getActionTypeByCode( Const
          .NVL( XmlHandler.getTagValue( entrynode, "actiontype" ), "" ) );
      moveToIMAPFolder = XmlHandler.getTagValue( entrynode, "movetoimapfolder" );
      createmovetofolder = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "createmovetofolder" ) );
      createlocalfolder = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "createlocalfolder" ) );
      aftergetimap =
        MailConnectionMeta.getAfterGetIMAPByCode( Const.NVL(
          XmlHandler.getTagValue( entrynode, "aftergetimap" ), "" ) );
      includesubfolders = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "includesubfolders" ) );
      useproxy = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "useproxy" ) );
      proxyusername = XmlHandler.getTagValue( entrynode, "proxyusername" );
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( "Unable to load action of type 'get pop' from XML node", xe );
    }
  }

  public int getValueImapList() {
    return valueimaplist;
  }

  public void setValueImapList( int value ) {
    this.valueimaplist = value;
  }

  public String getPort() {
    return sslport;
  }

  public String getRealPort() {
    return resolve( getPort() );
  }

  public void setPort( String sslport ) {
    this.sslport = sslport;
  }

  public void setFirstMails( String firstmails ) {
    this.firstmails = firstmails;
  }

  public String getFirstMails() {
    return firstmails;
  }

  public boolean isIncludeSubFolders() {
    return includesubfolders;
  }

  public void setIncludeSubFolders( boolean includesubfolders ) {
    this.includesubfolders = includesubfolders;
  }

  public void setFirstIMAPMails( String firstmails ) {
    this.imapfirstmails = firstmails;
  }

  public String getFirstIMAPMails() {
    return imapfirstmails;
  }

  public void setSenderSearchTerm( String senderSearch ) {
    this.senderSearch = senderSearch;
  }

  public String getSenderSearchTerm() {
    return this.senderSearch;
  }

  public void setNotTermSenderSearch( boolean notTermSenderSearch ) {
    this.notTermSenderSearch = notTermSenderSearch;
  }

  public boolean isNotTermSenderSearch() {
    return this.notTermSenderSearch;
  }

  public void setNotTermSubjectSearch( boolean notTermSubjectSearch ) {
    this.notTermSubjectSearch = notTermSubjectSearch;
  }

  public void setNotTermBodySearch( boolean notTermBodySearch ) {
    this.notTermBodySearch = notTermBodySearch;
  }

  public boolean isNotTermSubjectSearch() {
    return this.notTermSubjectSearch;
  }

  public boolean isNotTermBodySearch() {
    return this.notTermBodySearch;
  }

  public void setNotTermReceivedDateSearch( boolean notTermReceivedDateSearch ) {
    this.notTermReceivedDateSearch = notTermReceivedDateSearch;
  }

  public boolean isNotTermReceivedDateSearch() {
    return this.notTermReceivedDateSearch;
  }

  public void setNotTermReceipientSearch( boolean notTermReceipientSearch ) {
    this.notTermReceipientSearch = notTermReceipientSearch;
  }

  public boolean isNotTermReceipientSearch() {
    return this.notTermReceipientSearch;
  }

  public void setCreateMoveToFolder( boolean createfolder ) {
    this.createmovetofolder = createfolder;
  }

  public boolean isCreateMoveToFolder() {
    return this.createmovetofolder;
  }

  public void setReceipientSearch( String receipientSearch ) {
    this.receipientSearch = receipientSearch;
  }

  public String getReceipientSearch() {
    return this.receipientSearch;
  }

  public void setSubjectSearch( String subjectSearch ) {
    this.subjectSearch = subjectSearch;
  }

  public String getSubjectSearch() {
    return this.subjectSearch;
  }

  public void setBodySearch( String bodySearch ) {
    this.bodySearch = bodySearch;
  }

  public String getBodySearch() {
    return this.bodySearch;
  }

  public String getReceivedDate1() {
    return this.receivedDate1;
  }

  public void setReceivedDate1( String inputDate ) {
    this.receivedDate1 = inputDate;
  }

  public String getReceivedDate2() {
    return this.receivedDate2;
  }

  public void setReceivedDate2( String inputDate ) {
    this.receivedDate2 = inputDate;
  }

  public void setMoveToIMAPFolder( String folderName ) {
    this.moveToIMAPFolder = folderName;
  }

  public String getMoveToIMAPFolder() {
    return this.moveToIMAPFolder;
  }

  public void setCreateLocalFolder( boolean createfolder ) {
    this.createlocalfolder = createfolder;
  }

  public boolean isCreateLocalFolder() {
    return this.createlocalfolder;
  }

  public void setConditionOnReceivedDate( int conditionReceivedDate ) {
    this.conditionReceivedDate = conditionReceivedDate;
  }

  public int getConditionOnReceivedDate() {
    return this.conditionReceivedDate;
  }

  public void setActionType( int actiontype ) {
    this.actiontype = actiontype;
  }

  public int getActionType() {
    return this.actiontype;
  }

  public void setAfterGetIMAP( int afterget ) {
    this.aftergetimap = afterget;
  }

  public int getAfterGetIMAP() {
    return this.aftergetimap;
  }

  public String getRealFirstMails() {
    return resolve( getFirstMails() );
  }

  public void setServerName( String servername ) {
    this.servername = servername;
  }

  public String getServerName() {
    return servername;
  }

  public void setUserName( String username ) {
    this.username = username;
  }

  public String getUserName() {
    return username;
  }

  public void setOutputDirectory( String outputdirectory ) {
    this.outputdirectory = outputdirectory;
  }

  public void setFilenamePattern( String filenamepattern ) {
    this.filenamepattern = filenamepattern;
  }

  /**
   * <li>0 = retrieve all <li>2 = retrieve unread
   *
   * @param nr
   * @see {@link #setValueImapList(int)}
   */
  public void setRetrievemails( int nr ) {
    retrievemails = nr;
  }

  public int getRetrievemails() {
    return this.retrievemails;
  }

  public String getFilenamePattern() {
    return filenamepattern;
  }

  public String getOutputDirectory() {
    return outputdirectory;
  }

  public String getRealOutputDirectory() {
    return resolve( getOutputDirectory() );
  }

  public String getRealFilenamePattern() {
    return resolve( getFilenamePattern() );
  }

  public String getRealUsername() {
    return resolve( getUserName() );
  }

  public String getRealServername() {
    return resolve( getServerName() );
  }

  public String getRealProxyUsername() {
    return resolve( geProxyUsername() );
  }

  public String geProxyUsername() {
    return this.proxyusername;
  }

  /**
   * @return Returns the password.
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password string for resolving
   * @return Returns resolved decrypted password or null
   * in case of param returns null.
   */
  public String getRealPassword( String password ) {
    return Utils.resolvePassword( getVariables(), password );
  }

  public String getAttachmentFolder() {
    return attachmentfolder;
  }

  public String getRealAttachmentFolder() {
    return resolve( getAttachmentFolder() );
  }

  public void setAttachmentFolder( String folderName ) {
    this.attachmentfolder = folderName;
  }

  /**
   * @param delete The delete to set.
   */
  public void setDelete( boolean delete ) {
    this.delete = delete;
  }

  /**
   * @return Returns the delete.
   */
  public boolean getDelete() {
    return delete;
  }

  public String getProtocol() {
    return protocol;
  }

  public void setProtocol( String protocol ) {
    this.protocol = protocol;
  }

  public String getIMAPFolder() {
    return imapfolder;
  }

  public void setIMAPFolder( String folder ) {
    this.imapfolder = folder;
  }

  public void setAttachmentWildcard( String wildcard ) {
    attachmentwildcard = wildcard;
  }

  public String getAttachmentWildcard() {
    return attachmentwildcard;
  }

  /**
   * @param usessl The usessl to set.
   */
  public void setUseSSL( boolean usessl ) {
    this.usessl = usessl;
  }

  /**
   * @return Returns the usessl.
   */
  public boolean isUseSSL() {
    return this.usessl;
  }

  /**
   * @return Returns the useproxy.
   */
  public boolean isUseProxy() {
    return this.useproxy;
  }

  public void setUseProxy( boolean useprox ) {
    this.useproxy = useprox;
  }

  public boolean isSaveAttachment() {
    return saveattachment;
  }

  public void setProxyUsername( String username ) {
    this.proxyusername = username;
  }

  public String getProxyUsername() {
    return this.proxyusername;
  }

  public void setSaveAttachment( boolean saveattachment ) {
    this.saveattachment = saveattachment;
  }

  public boolean isSaveMessage() {
    return savemessage;
  }

  public void setSaveMessage( boolean savemessage ) {
    this.savemessage = savemessage;
  }

  public void setDifferentFolderForAttachment( boolean usedifferentfolder ) {
    this.usedifferentfolderforattachment = usedifferentfolder;
  }

  public boolean isDifferentFolderForAttachment() {
    return this.usedifferentfolderforattachment;
  }

  /**
   * @param password The password to set.
   */
  public void setPassword( String password ) {
    this.password = password;
  }

  public Result execute( Result previousResult, int nr ) throws HopException {
    Result result = previousResult;
    result.setResult( false );

    //FileObject fileObject = null;
    MailConnection mailConn = null;
    Date beginDate = null;
    Date endDate = null;

    SimpleDateFormat df = new SimpleDateFormat( DATE_PATTERN );

    try {

      boolean usePOP3 = getProtocol().equals( MailConnectionMeta.PROTOCOL_STRING_POP3 );
      boolean moveafter = false;
      int nbrmailtoretrieve =
        usePOP3 ? ( getRetrievemails() == 2 ? Const.toInt( getFirstMails(), 0 ) : 0 ) : Const.toInt(
          getFirstIMAPMails(), 0 );

      String realOutputFolder = createOutputDirectory( ActionGetPOP.FOLDER_OUTPUT );
      String targetAttachmentFolder = createOutputDirectory( ActionGetPOP.FOLDER_ATTACHMENTS );

      // Check destination folder
      String realMoveToIMAPFolder = resolve( getMoveToIMAPFolder() );
      if ( getProtocol().equals( MailConnectionMeta.PROTOCOL_STRING_IMAP )
        && ( getActionType() == MailConnectionMeta.ACTION_TYPE_MOVE )
        || ( getActionType() == MailConnectionMeta.ACTION_TYPE_GET
        && getAfterGetIMAP() == MailConnectionMeta.AFTER_GET_IMAP_MOVE ) ) {
        if ( Utils.isEmpty( realMoveToIMAPFolder ) ) {
          throw new HopException( BaseMessages
            .getString( PKG, "ActionGetMailsFromPOP.Error.MoveToIMAPFolderEmpty" ) );
        }
        moveafter = true;
      }

      // check search terms
      // Received Date
      switch ( getConditionOnReceivedDate() ) {
        case MailConnectionMeta.CONDITION_DATE_EQUAL:
        case MailConnectionMeta.CONDITION_DATE_GREATER:
        case MailConnectionMeta.CONDITION_DATE_SMALLER:
          String realBeginDate = resolve( getReceivedDate1() );
          if ( Utils.isEmpty( realBeginDate ) ) {
            throw new HopException( BaseMessages.getString(
              PKG, "ActionGetMailsFromPOP.Error.ReceivedDateSearchTermEmpty" ) );
          }
          beginDate = df.parse( realBeginDate );
          break;
        case MailConnectionMeta.CONDITION_DATE_BETWEEN:
          realBeginDate = resolve( getReceivedDate1() );
          if ( Utils.isEmpty( realBeginDate ) ) {
            throw new HopException( BaseMessages.getString(
              PKG, "ActionGetMailsFromPOP.Error.ReceivedDatesSearchTermEmpty" ) );
          }
          beginDate = df.parse( realBeginDate );
          String realEndDate = resolve( getReceivedDate2() );
          if ( Utils.isEmpty( realEndDate ) ) {
            throw new HopException( BaseMessages.getString(
              PKG, "ActionGetMailsFromPOP.Error.ReceivedDatesSearchTermEmpty" ) );
          }
          endDate = df.parse( realEndDate );
          break;
        default:
          break;
      }

      String realserver = getRealServername();
      String realusername = getRealUsername();
      String realpassword = getRealPassword( getPassword() );
      String realFilenamePattern = getRealFilenamePattern();
      int realport = Const.toInt( resolve( sslport ), -1 );
      String realIMAPFolder = resolve( getIMAPFolder() );
      String realProxyUsername = getRealProxyUsername();

      initVariables();
      // create a mail connection object
      mailConn =
        new MailConnection(
          log, MailConnectionMeta.getProtocolFromString( getProtocol(), MailConnectionMeta.PROTOCOL_IMAP ),
          realserver, realport, realusername, realpassword, isUseSSL(), isUseProxy(), realProxyUsername );
      // connect
      mailConn.connect();

      if ( moveafter ) {
        // Set destination folder
        // Check if folder exists
        mailConn.setDestinationFolder( realMoveToIMAPFolder, isCreateMoveToFolder() );
      }

      // apply search term?
      String realSearchSender = resolve( getSenderSearchTerm() );
      if ( !Utils.isEmpty( realSearchSender ) ) {
        // apply FROM
        mailConn.setSenderTerm( realSearchSender, isNotTermSenderSearch() );
      }
      String realSearchReceipient = resolve( getReceipientSearch() );
      if ( !Utils.isEmpty( realSearchReceipient ) ) {
        // apply TO
        mailConn.setReceipientTerm( realSearchReceipient );
      }
      String realSearchSubject = resolve( getSubjectSearch() );
      if ( !Utils.isEmpty( realSearchSubject ) ) {
        // apply Subject
        mailConn.setSubjectTerm( realSearchSubject, isNotTermSubjectSearch() );
      }
      String realSearchBody = resolve( getBodySearch() );
      if ( !Utils.isEmpty( realSearchBody ) ) {
        // apply body
        mailConn.setBodyTerm( realSearchBody, isNotTermBodySearch() );
      }
      // Received Date
      switch ( getConditionOnReceivedDate() ) {
        case MailConnectionMeta.CONDITION_DATE_EQUAL:
          mailConn.setReceivedDateTermEQ( beginDate );
          break;
        case MailConnectionMeta.CONDITION_DATE_GREATER:
          mailConn.setReceivedDateTermGT( beginDate );
          break;
        case MailConnectionMeta.CONDITION_DATE_SMALLER:
          mailConn.setReceivedDateTermLT( beginDate );
          break;
        case MailConnectionMeta.CONDITION_DATE_BETWEEN:
          mailConn.setReceivedDateTermBetween( beginDate, endDate );
          break;
        default:
          break;
      }
      // set FlagTerm?
      if ( usePOP3 ) {
        // retrieve messages
        if ( getRetrievemails() == 1 ) {
          // New messages
          // POP doesn't support the concept of "new" messages!
          mailConn.setFlagTermUnread();
        }
      } else {
        switch ( getValueImapList() ) {
          case MailConnectionMeta.VALUE_IMAP_LIST_NEW:
            mailConn.setFlagTermNew();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_OLD:
            mailConn.setFlagTermOld();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_READ:
            mailConn.setFlagTermRead();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_UNREAD:
            mailConn.setFlagTermUnread();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_FLAGGED:
            mailConn.setFlagTermFlagged();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_NOT_FLAGGED:
            mailConn.setFlagTermNotFlagged();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_DRAFT:
            mailConn.setFlagTermDraft();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_NOT_DRAFT:
            mailConn.setFlagTermNotDraft();
            break;
          default:
            break;
        }
      }
      // open folder and retrieve messages
      fetchOneFolder(
        mailConn, usePOP3, realIMAPFolder, realOutputFolder, targetAttachmentFolder, realMoveToIMAPFolder,
        realFilenamePattern, nbrmailtoretrieve, df );

      if ( isIncludeSubFolders() ) {
        // Fetch also sub folders?
        if ( isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "ActionGetPOP.FetchingSubFolders" ) );
        }
        String[] subfolders = mailConn.returnAllFolders();
        if ( subfolders.length == 0 ) {
          if ( isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "ActionGetPOP.NoSubFolders" ) );
          }
        } else {
          for ( int i = 0; i < subfolders.length; i++ ) {
            fetchOneFolder(
              mailConn, usePOP3, subfolders[ i ], realOutputFolder, targetAttachmentFolder, realMoveToIMAPFolder,
              realFilenamePattern, nbrmailtoretrieve, df );
          }
        }
      }

      result.setResult( true );
      result.setNrFilesRetrieved( mailConn.getSavedAttachedFilesCounter() );
      result.setNrLinesWritten( mailConn.getSavedMessagesCounter() );
      result.setNrLinesDeleted( mailConn.getDeletedMessagesCounter() );
      result.setNrLinesUpdated( mailConn.getMovedMessagesCounter() );

      if ( isDetailed() ) {
        logDetailed( "=======================================" );
        logDetailed( BaseMessages.getString( PKG, "ActionGetPOP.Log.Info.SavedMessages", ""
          + mailConn.getSavedMessagesCounter() ) );
        logDetailed( BaseMessages.getString( PKG, "ActionGetPOP.Log.Info.DeletedMessages", ""
          + mailConn.getDeletedMessagesCounter() ) );
        logDetailed( BaseMessages.getString( PKG, "ActionGetPOP.Log.Info.MovedMessages", ""
          + mailConn.getMovedMessagesCounter() ) );
        if ( getActionType() == MailConnectionMeta.ACTION_TYPE_GET && isSaveAttachment() ) {
          logDetailed( BaseMessages.getString( PKG, "ActionGetPOP.Log.Info.AttachedMessagesSuccess", ""
            + mailConn.getSavedAttachedFilesCounter() ) );
        }
        logDetailed( "=======================================" );
      }
    } catch ( Exception e ) {
      result.setNrErrors( 1 );
      logError( "Unexpected error: " + e.getMessage() );
      logError( Const.getStackTracker( e ) );
    } finally {
      try {
        if ( mailConn != null ) {
          mailConn.disconnect();
          mailConn = null;
        }
      } catch ( Exception e ) { /* Ignore */
      }
    }

    return result;
  }

  void fetchOneFolder( MailConnection mailConn, boolean usePOP3, String realIMAPFolder,
                       String realOutputFolder, String targetAttachmentFolder, String realMoveToIMAPFolder,
                       String realFilenamePattern, int nbrmailtoretrieve, SimpleDateFormat df ) throws HopException {
    try {
      // if it is not pop3 and we have non-default imap folder...
      if ( !usePOP3 && !Utils.isEmpty( realIMAPFolder ) ) {
        mailConn.openFolder( realIMAPFolder, true );
      } else {
        mailConn.openFolder( true );
      }

      mailConn.retrieveMessages();

      int messagesCount = mailConn.getMessagesCount();

      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.TotalMessagesFolder.Label", ""
          + messagesCount, Const.NVL( mailConn.getFolderName(), MailConnectionMeta.INBOX_FOLDER ) ) );
      }

      messagesCount =
        nbrmailtoretrieve > 0
          ? ( nbrmailtoretrieve > messagesCount ? messagesCount : nbrmailtoretrieve ) : messagesCount;

      if ( messagesCount > 0 ) {
        switch ( getActionType() ) {
          case MailConnectionMeta.ACTION_TYPE_DELETE:
            if ( nbrmailtoretrieve > 0 ) {
              // We need to fetch all messages in order to retrieve
              // only the first nbrmailtoretrieve ...
              for ( int i = 0; i < messagesCount && !parentWorkflow.isStopped(); i++ ) {
                // Get next message
                mailConn.fetchNext();
                // Delete this message
                mailConn.deleteMessage();
                if ( isDebug() ) {
                  logDebug( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.MessageDeleted", "" + i ) );
                }
              }
            } else {
              // Delete messages
              mailConn.deleteMessages( true );
              if ( isDebug() ) {
                logDebug( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.MessagesDeleted", "" + messagesCount ) );
              }
            }
            break;
          case MailConnectionMeta.ACTION_TYPE_MOVE:
            if ( nbrmailtoretrieve > 0 ) {
              // We need to fetch all messages in order to retrieve
              // only the first nbrmailtoretrieve ...
              for ( int i = 0; i < messagesCount && !parentWorkflow.isStopped(); i++ ) {
                // Get next message
                mailConn.fetchNext();
                // Move this message
                mailConn.moveMessage();
                if ( isDebug() ) {
                  logDebug( BaseMessages.getString(
                    PKG, "ActionGetMailsFromPOP.MessageMoved", "" + i, realMoveToIMAPFolder ) );
                }
              }
            } else {
              // Move all messages
              mailConn.moveMessages();
              if ( isDebug() ) {
                logDebug( BaseMessages.getString(
                  PKG, "ActionGetMailsFromPOP.MessagesMoved", "" + messagesCount, realMoveToIMAPFolder ) );
              }
            }
            break;
          default:
            // Get messages and save it in a local file
            // also save attached files if needed
            for ( int i = 0; i < messagesCount && !parentWorkflow.isStopped(); i++ ) {
              // Get next message
              mailConn.fetchNext();
              int messagenumber = mailConn.getMessage().getMessageNumber();
              boolean okPOP3 = usePOP3 ? true : false; // (mailConn.getMessagesCounter()<nbrmailtoretrieve &&
              // retrievemails==2)||(retrievemails!=2):false;
              boolean okIMAP = !usePOP3;

              if ( okPOP3 || okIMAP ) {
                // display some infos on the current message
                //
                if ( isDebug() && mailConn.getMessage() != null ) {
                  logDebug( "--------------------------------------------------" );
                  logDebug( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.MessageNumber.Label", ""
                    + messagenumber ) );
                  if ( mailConn.getMessage().getReceivedDate() != null ) {
                    logDebug( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.ReceivedDate.Label", df
                      .format( mailConn.getMessage().getReceivedDate() ) ) );
                  }
                  logDebug( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.ContentType.Label", mailConn
                    .getMessage().getContentType() ) );
                  logDebug( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.EmailFrom.Label", Const.NVL( mailConn
                    .getMessage().getFrom()[ 0 ].toString(), "" ) ) );
                  logDebug( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.EmailSubject.Label", Const.NVL(
                    mailConn.getMessage().getSubject(), "" ) ) );
                }
                if ( isSaveMessage() ) {
                  // get local message filename
                  String localfilenameMessage = replaceTokens( realFilenamePattern, i );

                  if ( isDebug() ) {
                    logDebug( BaseMessages.getString(
                      PKG, "ActionGetMailsFromPOP.LocalFilename.Label", localfilenameMessage ) );
                  }

                  // save message content in the file
                  mailConn.saveMessageContentToFile( localfilenameMessage, realOutputFolder );
                  // PDI-10942 explicitly set message as read
                  mailConn.getMessage().setFlag( Flag.SEEN, true );

                  if ( isDetailed() ) {
                    logDetailed( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.MessageSaved.Label", ""
                      + messagenumber, localfilenameMessage, realOutputFolder ) );
                  }
                }

                // Do we need to save attached file?
                if ( isSaveAttachment() ) {
                  mailConn.saveAttachedFiles( targetAttachmentFolder, attachementPattern );
                }
                // We successfully retrieved message
                // do we need to make another action (delete, move)?
                if ( usePOP3 ) {
                  if ( getDelete() ) {
                    mailConn.deleteMessage();
                    if ( isDebug() ) {
                      logDebug( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.MessageDeleted", ""
                        + messagenumber ) );
                    }
                  }
                } else {
                  switch ( getAfterGetIMAP() ) {
                    case MailConnectionMeta.AFTER_GET_IMAP_DELETE:
                      // Delete messages
                      mailConn.deleteMessage();
                      if ( isDebug() ) {
                        logDebug( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.MessageDeleted", ""
                          + messagenumber ) );
                      }
                      break;
                    case MailConnectionMeta.AFTER_GET_IMAP_MOVE:
                      // Move messages
                      mailConn.moveMessage();
                      if ( isDebug() ) {
                        logDebug( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.MessageMoved", ""
                          + messagenumber, realMoveToIMAPFolder ) );
                      }
                      break;
                    default:
                  }
                }

              }
            }
            break;
        }
      }
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  private String replaceTokens( String aString, int idfile ) {
    String localfilenameMessage = aString;
    localfilenameMessage = localfilenameMessage.replaceAll( FILENAME_ID_PATTERN, "" + ( idfile + 1 ) );
    localfilenameMessage =
      substituteDate( localfilenameMessage, FILENAME_SYS_DATE_OPEN, FILENAME_SYS_DATE_CLOSE, new Date() );
    return localfilenameMessage;

  }

  private String substituteDate( String aString, String open, String close, Date datetime ) {
    if ( aString == null ) {
      return null;
    }
    StringBuilder buffer = new StringBuilder();
    String rest = aString;

    // search for closing string
    int i = rest.indexOf( open );
    while ( i > -1 ) {
      int j = rest.indexOf( close, i + open.length() );
      // search for closing string
      if ( j > -1 ) {
        String varName = rest.substring( i + open.length(), j );
        DateFormat dateFormat = new SimpleDateFormat( varName );
        Object Value = dateFormat.format( datetime );

        buffer.append( rest.substring( 0, i ) );
        buffer.append( Value );
        rest = rest.substring( j + close.length() );
      } else {
        // no closing tag found; end the search
        buffer.append( rest );
        rest = "";
      }
      // keep searching
      i = rest.indexOf( close );
    }
    buffer.append( rest );
    return buffer.toString();
  }

  private void initVariables() {
    // Attachment wildcard
    attachementPattern = null;
    String realAttachmentWildcard = resolve( getAttachmentWildcard() );
    if ( !Utils.isEmpty( realAttachmentWildcard ) ) {
      attachementPattern = Pattern.compile( realAttachmentWildcard );
    }
  }

  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ActionValidatorUtils.andValidator().validate( this, "serverName", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    ActionValidatorUtils.andValidator().validate( this, "userName", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    ActionValidatorUtils.andValidator().validate( this, "password", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notNullValidator() ) );

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, ActionValidatorUtils.notBlankValidator(),
      ActionValidatorUtils.fileExistsValidator() );
    ActionValidatorUtils.andValidator().validate( this, "outputDirectory", remarks, ctx );

    ActionValidatorUtils.andValidator().validate( this, "SSLPort", remarks,
      AndValidator.putValidators( ActionValidatorUtils.integerValidator() ) );
  }

  public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( variables, workflowMeta );
    if ( !Utils.isEmpty( servername ) ) {
      String realServername = resolve( servername );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realServername, ResourceType.SERVER ) );
      references.add( reference );
    }
    return references;
  }

  String createOutputDirectory( int folderType ) throws HopException, FileSystemException, IllegalArgumentException {
    if ( ( folderType != ActionGetPOP.FOLDER_OUTPUT ) && ( folderType != ActionGetPOP.FOLDER_ATTACHMENTS ) ) {
      throw new IllegalArgumentException( "Invalid folderType argument" );
    }
    String folderName = "";
    switch ( folderType ) {
      case ActionGetPOP.FOLDER_OUTPUT:
        folderName = getRealOutputDirectory();
        break;
      case ActionGetPOP.FOLDER_ATTACHMENTS:
        if ( isSaveAttachment() && isDifferentFolderForAttachment() ) {
          folderName = getRealAttachmentFolder();
        } else {
          folderName = getRealOutputDirectory();
        }
        break;
    }
    if ( Utils.isEmpty( folderName ) ) {
      switch ( folderType ) {
        case ActionGetPOP.FOLDER_OUTPUT:
          throw new HopException( BaseMessages
            .getString( PKG, "ActionGetMailsFromPOP.Error.OutputFolderEmpty" ) );
        case ActionGetPOP.FOLDER_ATTACHMENTS:
          throw new HopException( BaseMessages
            .getString( PKG, "ActionGetMailsFromPOP.Error.AttachmentFolderEmpty" ) );
      }
    }
    FileObject folder = HopVfs.getFileObject( folderName );
    if ( folder.exists() ) {
      if ( folder.getType() != FileType.FOLDER ) {
        switch ( folderType ) {
          case ActionGetPOP.FOLDER_OUTPUT:
            throw new HopException( BaseMessages.getString(
              PKG, "ActionGetMailsFromPOP.Error.NotAFolderNot", folderName ) );
          case ActionGetPOP.FOLDER_ATTACHMENTS:
            throw new HopException( BaseMessages.getString(
              PKG, "ActionGetMailsFromPOP.Error.AttachmentFolderNotAFolder", folderName ) );
        }
      }
      if ( isDebug() ) {
        switch ( folderType ) {
          case ActionGetPOP.FOLDER_OUTPUT:
            logDebug( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.Log.OutputFolderExists", folderName ) );
            break;
          case ActionGetPOP.FOLDER_ATTACHMENTS:
            logDebug( BaseMessages.getString( PKG, "ActionGetMailsFromPOP.Log.AttachmentFolderExists", folderName ) );
            break;
        }
      }
    } else {
      if ( isCreateLocalFolder() ) {
        folder.createFolder();
      } else {
        switch ( folderType ) {
          case ActionGetPOP.FOLDER_OUTPUT:
            throw new HopException( BaseMessages.getString(
              PKG, "ActionGetMailsFromPOP.Error.OutputFolderNotExist", folderName ) );
          case ActionGetPOP.FOLDER_ATTACHMENTS:
            throw new HopException( BaseMessages.getString(
              PKG, "ActionGetMailsFromPOP.Error.AttachmentFolderNotExist", folderName ) );
        }
      }
    }

    String returnValue = HopVfs.getFilename( folder );
    try {
      folder.close();
    } catch ( IOException ignore ) {
      //Ignore error, as the folder was created successfully
    }
    return returnValue;
  }
}
