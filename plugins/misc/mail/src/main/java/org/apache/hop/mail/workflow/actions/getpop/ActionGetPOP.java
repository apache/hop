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

package org.apache.hop.mail.workflow.actions.getpop;

import jakarta.mail.Flags.Flag;
import jakarta.mail.Session;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mail.metadata.MailServerConnection;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;

/** This defines an get pop action. */
@Getter
@Setter
@Action(
    id = "GET_POP",
    name = "i18n::ActionGetPOP.Name",
    description = "i18n::ActionGetPOP.Description",
    image = "GetPOP.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Mail",
    keywords = "i18n::ActionGetPOP.keyword",
    documentationUrl = "/workflow/actions/getpop.html",
    actionTransformTypes = {ActionTransformType.MAIL})
@SuppressWarnings("java:S1104")
public class ActionGetPOP extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionGetPOP.class;
  private static final String CONST_PASSWORD = "password";
  private static final String CONST_MESSAGE_DELETED = "ActionGetMailsFromPOP.MessageDeleted";

  static final int FOLDER_OUTPUT = 0;

  static final int FOLDER_ATTACHMENTS = 1;
  public static final String ACTION_GET_MAILS_FROM_POP_MESSAGE_MOVED =
      "ActionGetMailsFromPOP.MessageMoved";
  public static final String ACTION_GET_MAILS_FROM_POP_ERROR_RECEIVED_DATES_SEARCH_TERM_EMPTY =
      "ActionGetMailsFromPOP.Error.ReceivedDatesSearchTermEmpty";

  public int actionType;

  public int conditionReceivedDate;

  public int valueIMAPList;

  public int afterGetIMAP;

  @HopMetadataProperty(key = "servername")
  private String serverName;

  @HopMetadataProperty(key = "username")
  private String userName;

  @HopMetadataProperty(password = true)
  private String password;

  @HopMetadataProperty(key = "usessl")
  private boolean useSsl;

  @HopMetadataProperty(key = "sslport")
  private String sslPort;

  @HopMetadataProperty(key = "usexoauth2")
  private boolean useXOauth2;

  @HopMetadataProperty(key = "useproxy")
  private boolean useProxy;

  @HopMetadataProperty(key = "proxyusername")
  private String proxyUsername;

  @HopMetadataProperty(key = "outputdirectory")
  private String outputDirectory;

  @HopMetadataProperty(key = "filenamepattern")
  private String filenamePattern;

  @HopMetadataProperty(key = "firstmails")
  private String firstMails;

  @HopMetadataProperty(key = "retrievemails")
  public int retrieveMails;

  @HopMetadataProperty private boolean delete;

  @HopMetadataProperty private String protocol;

  @HopMetadataProperty(key = "saveattachment")
  private boolean saveAttachment;

  @HopMetadataProperty(key = "savemessage")
  private boolean saveMessage;

  @HopMetadataProperty(key = "usedifferentfolderforattachment")
  private boolean useDifferentFolderForAttachment;

  @HopMetadataProperty(key = "attachmentfolder")
  private String attachmentFolder;

  @HopMetadataProperty(key = "attachmentwildcard")
  private String attachmentWildcard;

  @HopMetadataProperty(key = "imapfirstmails")
  private String imapFirstMails;

  @HopMetadataProperty(key = "imapfolder")
  private String imapFolder;

  // search term
  @HopMetadataProperty(key = "sendersearch")
  private String senderSearch;

  @HopMetadataProperty(key = "nottermsendersearch")
  private boolean notTermSenderSearch;

  @HopMetadataProperty(key = "recipientsearch")
  private String recipientSearch;

  @HopMetadataProperty(key = "subjectsearch")
  private String subjectSearch;

  @HopMetadataProperty(key = "bodysearch")
  private String bodySearch;

  @HopMetadataProperty(key = "nottermbodysearch")
  private boolean notTermBodySearch;

  @HopMetadataProperty private String receivedDate1;

  @HopMetadataProperty private String receivedDate2;

  @HopMetadataProperty(key = "nottermsubjectsearch")
  private boolean notTermSubjectSearch;

  @HopMetadataProperty(key = "nottermreceipientsearh")
  private boolean notTermRecipientSearch;

  @HopMetadataProperty(key = "nottermreceiveddatesearch")
  private boolean notTermReceivedDateSearch;

  @HopMetadataProperty(key = "includesubfolders")
  private boolean includeSubFolders;

  @HopMetadataProperty(key = "movetoimapfolder")
  private String moveToIMAPFolder;

  @HopMetadataProperty(key = "createmovetofolder")
  private boolean createMoveToFolder;

  @HopMetadataProperty(key = "createlocalfolder")
  private boolean createLocalFolder;

  @HopMetadataProperty private String connectionName;

  private static final String DEFAULT_FILE_NAME_PATTERN =
      "name_{SYS|hhmmss_MMddyyyy|}_#IdFile#.mail";

  public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
  private static final String FILENAME_ID_PATTERN = "#IdFile#";
  private static final String FILENAME_SYS_DATE_OPEN = "{SYS|";
  private static final String FILENAME_SYS_DATE_CLOSE = "|}";

  private MailServerConnection connection;
  private MailConnection mailConn;

  private Pattern attachementPattern;

  public ActionGetPOP(String n) {
    super(n, "");
    serverName = null;
    userName = null;
    password = null;
    useSsl = false;
    sslPort = null;
    useXOauth2 = false;
    useProxy = false;
    proxyUsername = null;
    outputDirectory = null;
    filenamePattern = DEFAULT_FILE_NAME_PATTERN;
    retrieveMails = 0;
    firstMails = null;
    delete = false;
    if (!StringUtils.isEmpty(getConnectionName())) {
      protocol = MailServerConnection.PROTOCOL_STRING_POP3;
    } else {
      protocol = MailConnectionMeta.PROTOCOL_STRING_POP3;
    }
    saveAttachment = true;
    saveMessage = true;
    useDifferentFolderForAttachment = false;
    attachmentFolder = null;
    attachmentWildcard = null;
    imapFirstMails = "0";
    if (!StringUtils.isEmpty(getConnectionName())) {
      valueIMAPList = MailServerConnection.VALUE_IMAP_LIST_ALL;
    } else {
      valueIMAPList = MailConnectionMeta.VALUE_IMAP_LIST_ALL;
    }
    imapFolder = null;
    // search term
    senderSearch = null;
    notTermSenderSearch = false;
    notTermRecipientSearch = false;
    notTermSubjectSearch = false;
    bodySearch = null;
    notTermBodySearch = false;
    receivedDate1 = null;
    receivedDate2 = null;
    notTermReceivedDateSearch = false;
    recipientSearch = null;
    subjectSearch = null;
    actionType = MailConnectionMeta.ACTION_TYPE_GET;
    moveToIMAPFolder = null;
    createMoveToFolder = false;
    createLocalFolder = false;
    afterGetIMAP = MailConnectionMeta.AFTER_GET_IMAP_NOTHING;
    includeSubFolders = false;
  }

  public ActionGetPOP() {
    this("");
  }

  @Override
  public Object clone() {
    ActionGetPOP je = (ActionGetPOP) super.clone();
    return je;
  }

  public String getRealPort() {
    return resolve(getSslPort());
  }

  public String getRealOutputDirectory() {
    return resolve(getOutputDirectory());
  }

  public String getRealFilenamePattern() {
    return resolve(getFilenamePattern());
  }

  public String getRealUsername() {
    return resolve(getUserName());
  }

  public String getRealServername() {
    return resolve(getServerName());
  }

  public String getRealProxyUsername() {
    return resolve(getProxyUsername());
  }

  /**
   * @param password string for resolving
   * @return Returns resolved decrypted password or null in case of param returns null.
   */
  public String getRealPassword(String password) {
    return Utils.resolvePassword(getVariables(), password);
  }

  public String getRealAttachmentFolder() {
    return resolve(getAttachmentFolder());
  }

  @Override
  public Result execute(Result previousResult, int nr) throws HopException {
    Result result = previousResult;
    result.setResult(false);

    connection = null;
    Session session = null;
    mailConn = null;
    Date beginDate = null;
    Date endDate = null;

    SimpleDateFormat df = new SimpleDateFormat(DATE_PATTERN);

    try {

      boolean usePOP3;
      if (connectionName != null) {
        usePOP3 = getProtocol().equals(MailServerConnection.PROTOCOL_STRING_POP3);
      } else {
        usePOP3 = getProtocol().equals(MailConnectionMeta.PROTOCOL_STRING_POP3);
      }
      boolean moveafter = false;
      int nbrmailtoretrieve =
          usePOP3
              ? (getRetrieveMails() == 2 ? Const.toInt(getFirstMails(), 0) : 0)
              : Const.toInt(getImapFirstMails(), 0);

      String realOutputFolder = createOutputDirectory(ActionGetPOP.FOLDER_OUTPUT);
      String targetAttachmentFolder = createOutputDirectory(ActionGetPOP.FOLDER_ATTACHMENTS);

      // Check destination folder
      String realMoveToIMAPFolder = resolve(getMoveToIMAPFolder());
      if (StringUtils.isEmpty(connectionName)) {
        if (getProtocol().equals(MailServerConnection.PROTOCOL_STRING_IMAP)
                && (getActionType() == MailServerConnection.ACTION_TYPE_MOVE)
            || (getActionType() == MailServerConnection.ACTION_TYPE_GET
                && getAfterGetIMAP() == MailServerConnection.AFTER_GET_IMAP_MOVE)) {
          if (Utils.isEmpty(realMoveToIMAPFolder)) {
            throw new HopException(
                BaseMessages.getString(PKG, "ActionGetMailsFromPOP.Error.MoveToIMAPFolderEmpty"));
          }
          moveafter = true;
        }
      } else {
        if (getProtocol().equals(MailConnectionMeta.PROTOCOL_STRING_IMAP)
                && (getActionType() == MailConnectionMeta.ACTION_TYPE_MOVE)
            || (getActionType() == MailConnectionMeta.ACTION_TYPE_GET
                && getAfterGetIMAP() == MailConnectionMeta.AFTER_GET_IMAP_MOVE)) {
          if (Utils.isEmpty(realMoveToIMAPFolder)) {
            throw new HopException(
                BaseMessages.getString(PKG, "ActionGetMailsFromPOP.Error.MoveToIMAPFolderEmpty"));
          }
          moveafter = true;
        }
      }

      // check search terms
      // Received Date
      if (StringUtils.isEmpty(connectionName)) {
        switch (getConditionReceivedDate()) {
          case MailServerConnection.CONDITION_DATE_EQUAL,
              MailServerConnection.CONDITION_DATE_GREATER,
              MailServerConnection.CONDITION_DATE_SMALLER:
            String realBeginDate = resolve(getReceivedDate1());
            if (Utils.isEmpty(realBeginDate)) {
              throw new HopException(
                  BaseMessages.getString(
                      PKG, "ActionGetMailsFromPOP.Error.ReceivedDateSearchTermEmpty"));
            }
            beginDate = df.parse(realBeginDate);
            break;
          case MailServerConnection.CONDITION_DATE_BETWEEN:
            realBeginDate = resolve(getReceivedDate1());
            if (Utils.isEmpty(realBeginDate)) {
              throw new HopException(
                  BaseMessages.getString(
                      PKG, ACTION_GET_MAILS_FROM_POP_ERROR_RECEIVED_DATES_SEARCH_TERM_EMPTY));
            }
            beginDate = df.parse(realBeginDate);
            String realEndDate = resolve(getReceivedDate2());
            if (Utils.isEmpty(realEndDate)) {
              throw new HopException(
                  BaseMessages.getString(
                      PKG, ACTION_GET_MAILS_FROM_POP_ERROR_RECEIVED_DATES_SEARCH_TERM_EMPTY));
            }
            endDate = df.parse(realEndDate);
            break;
          default:
            break;
        }
      } else {
        switch (getConditionReceivedDate()) {
          case MailConnectionMeta.CONDITION_DATE_EQUAL,
              MailConnectionMeta.CONDITION_DATE_GREATER,
              MailConnectionMeta.CONDITION_DATE_SMALLER:
            String realBeginDate = resolve(getReceivedDate1());
            if (Utils.isEmpty(realBeginDate)) {
              throw new HopException(
                  BaseMessages.getString(
                      PKG, "ActionGetMailsFromPOP.Error.ReceivedDateSearchTermEmpty"));
            }
            beginDate = df.parse(realBeginDate);
            break;
          case MailConnectionMeta.CONDITION_DATE_BETWEEN:
            realBeginDate = resolve(getReceivedDate1());
            if (Utils.isEmpty(realBeginDate)) {
              throw new HopException(
                  BaseMessages.getString(
                      PKG, ACTION_GET_MAILS_FROM_POP_ERROR_RECEIVED_DATES_SEARCH_TERM_EMPTY));
            }
            beginDate = df.parse(realBeginDate);
            String realEndDate = resolve(getReceivedDate2());
            if (Utils.isEmpty(realEndDate)) {
              throw new HopException(
                  BaseMessages.getString(
                      PKG, ACTION_GET_MAILS_FROM_POP_ERROR_RECEIVED_DATES_SEARCH_TERM_EMPTY));
            }
            endDate = df.parse(realEndDate);
            break;
          default:
            break;
        }
      }

      String realserver = getRealServername();
      String realusername = getRealUsername();
      String realpassword = getRealPassword(getPassword());
      String realFilenamePattern = getRealFilenamePattern();
      int realport = Const.toInt(resolve(sslPort), -1);
      String realIMAPFolder = resolve(getImapFolder());
      String realProxyUsername = getRealProxyUsername();

      initVariables();

      // Check if we have a connection name, create a MailServerConnection (metadata item) if we
      // have.
      // Create a mail connection object (locally in the action) if we don't.
      if (!StringUtils.isEmpty(connectionName)) {
        try {
          connection =
              getMetadataProvider().getSerializer(MailServerConnection.class).load(connectionName);
        } catch (HopException e) {
          throw new RuntimeException(
              "Mail server connection '" + connectionName + "' could not be found", e);
        }
        try {
          connection.getSession(getVariables());
          connection.getStore().connect();
        } catch (Exception e) {
          throw new RuntimeException(
              "A connection to mail server connection '"
                  + connectionName
                  + "' could not be established",
              e);
        }
      } else {
        mailConn =
            new MailConnection(
                getLogChannel(),
                MailConnectionMeta.getProtocolFromString(
                    getProtocol(), MailConnectionMeta.PROTOCOL_IMAP),
                realserver,
                realport,
                realusername,
                realpassword,
                isUseSsl(),
                isUseXOauth2(),
                isUseProxy(),
                realProxyUsername);
        // connect
        mailConn.connect();
      }

      if (moveafter) {
        if (connection != null) {
          connection.setDestinationFolder(realMoveToIMAPFolder, isCreateMoveToFolder());
        } else {
          // Set destination folder
          // Check if folder exists
          mailConn.setDestinationFolder(realMoveToIMAPFolder, isCreateMoveToFolder());
        }
      }

      // apply search term?
      String realSearchSender = resolve(getSenderSearch());
      if (!Utils.isEmpty(realSearchSender)) {
        // apply FROM
        if (connection != null) {
          connection.setSenderTerm(realSearchSender, isNotTermSenderSearch());
        } else {
          mailConn.setSenderTerm(realSearchSender, isNotTermSenderSearch());
        }
      }

      String realSearchReceipient = resolve(getRecipientSearch());
      if (!Utils.isEmpty(realSearchReceipient)) {
        // apply TO
        if (connection != null) {
          connection.setReceipientTerm(realSearchReceipient);
        } else {
          mailConn.setReceipientTerm(realSearchReceipient);
        }
      }
      String realSearchSubject = resolve(getSubjectSearch());
      if (!Utils.isEmpty(realSearchSubject)) {
        // apply Subject
        if (connection != null) {
          connection.setSubjectTerm(realSearchSubject, isNotTermSubjectSearch());
        } else {
          mailConn.setSubjectTerm(realSearchSubject, isNotTermSubjectSearch());
        }
      }

      String realSearchBody = resolve(getBodySearch());
      if (!Utils.isEmpty(realSearchBody)) {
        // apply body
        if (connection != null) {
          connection.setBodyTerm(realSearchBody, isNotTermBodySearch());
        } else {
          mailConn.setBodyTerm(realSearchBody, isNotTermBodySearch());
        }
      }

      // Received Date
      if (connection != null) {
        switch (getConditionReceivedDate()) {
          case MailServerConnection.CONDITION_DATE_EQUAL:
            connection.setReceivedDateTermEQ(beginDate);
            break;
          case MailServerConnection.CONDITION_DATE_GREATER:
            connection.setReceivedDateTermGT(beginDate);
            break;
          case MailServerConnection.CONDITION_DATE_SMALLER:
            connection.setReceivedDateTermLT(beginDate);
            break;
          case MailServerConnection.CONDITION_DATE_BETWEEN:
            connection.setReceivedDateTermBetween(beginDate, endDate);
            break;
          default:
            break;
        }
      } else {
        switch (getConditionReceivedDate()) {
          case MailConnectionMeta.CONDITION_DATE_EQUAL:
            mailConn.setReceivedDateTermEQ(beginDate);
            break;
          case MailConnectionMeta.CONDITION_DATE_GREATER:
            mailConn.setReceivedDateTermGT(beginDate);
            break;
          case MailConnectionMeta.CONDITION_DATE_SMALLER:
            mailConn.setReceivedDateTermLT(beginDate);
            break;
          case MailConnectionMeta.CONDITION_DATE_BETWEEN:
            mailConn.setReceivedDateTermBetween(beginDate, endDate);
            break;
          default:
            break;
        }
      }
      // set FlagTerm?
      if (usePOP3) {
        // retrieve messages
        if (getRetrieveMails() == 1) {
          // New messages
          // POP doesn't support the concept of "new" messages!
          if (connection != null) {
            connection.setFlagTermUnread();
          } else {
            mailConn.setFlagTermUnread();
          }
        }
      } else {
        if (connection != null) {
          switch (getValueIMAPList()) {
            case MailServerConnection.VALUE_IMAP_LIST_NEW:
              connection.setFlagTermNew();
              break;
            case MailServerConnection.VALUE_IMAP_LIST_OLD:
              connection.setFlagTermOld();
              break;
            case MailServerConnection.VALUE_IMAP_LIST_READ:
              connection.setFlagTermRead();
              break;
            case MailServerConnection.VALUE_IMAP_LIST_UNREAD:
              connection.setFlagTermUnread();
              break;
            case MailServerConnection.VALUE_IMAP_LIST_FLAGGED:
              connection.setFlagTermFlagged();
              break;
            case MailServerConnection.VALUE_IMAP_LIST_NOT_FLAGGED:
              connection.setFlagTermNotFlagged();
              break;
            case MailServerConnection.VALUE_IMAP_LIST_DRAFT:
              connection.setFlagTermDraft();
              break;
            case MailServerConnection.VALUE_IMAP_LIST_NOT_DRAFT:
              connection.setFlagTermNotDraft();
              break;
            default:
              break;
          }
        } else {
          switch (getValueIMAPList()) {
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
      }
      // open folder and retrieve messages
      fetchOneFolder(
          usePOP3,
          realIMAPFolder,
          realOutputFolder,
          targetAttachmentFolder,
          realMoveToIMAPFolder,
          realFilenamePattern,
          nbrmailtoretrieve,
          df);

      if (isIncludeSubFolders()) {
        // Fetch also sub folders?
        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "ActionGetPOP.FetchingSubFolders"));
        }
        String[] subfolders = null;
        if (connection != null) {
          subfolders = connection.returnAllFolders();
        } else {
          subfolders = mailConn.returnAllFolders();
        }
        if (subfolders.length == 0) {
          if (isDebug()) {
            logDebug(BaseMessages.getString(PKG, "ActionGetPOP.NoSubFolders"));
          }
        } else {
          for (String subfolder : subfolders) {
            fetchOneFolder(
                usePOP3,
                subfolder,
                realOutputFolder,
                targetAttachmentFolder,
                realMoveToIMAPFolder,
                realFilenamePattern,
                nbrmailtoretrieve,
                df);
          }
        }
      }

      result.setResult(true);
      if (connection != null) {
        result.setNrFilesRetrieved(connection.getSavedAttachedFilesCounter());
        result.setNrLinesWritten(connection.getSavedMessagesCounter());
        result.setNrLinesDeleted(connection.getDeletedMessagesCounter());
        result.setNrLinesUpdated(connection.getMovedMessagesCounter());
      } else {
        result.setNrFilesRetrieved(mailConn.getSavedAttachedFilesCounter());
        result.setNrLinesWritten(mailConn.getSavedMessagesCounter());
        result.setNrLinesDeleted(mailConn.getDeletedMessagesCounter());
        result.setNrLinesUpdated(mailConn.getMovedMessagesCounter());
      }

      if (isDetailed()) {
        logDetailed("=======================================");
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionGetPOP.Log.Info.SavedMessages", "" + result.getNrLinesWritten()));
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionGetPOP.Log.Info.DeletedMessages", "" + result.getNrLinesDeleted()));
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionGetPOP.Log.Info.MovedMessages", "" + result.getNrLinesUpdated()));

        if (((connection != null && getActionType() == MailServerConnection.ACTION_TYPE_GET)
                || (connection == null && getActionType() == MailConnectionMeta.ACTION_TYPE_GET))
            && isSaveAttachment()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionGetPOP.Log.Info.AttachedMessagesSuccess",
                  "" + result.getNrFilesRetrieved()));
        }
        logDetailed("=======================================");
      }
    } catch (Exception e) {
      result.setNrErrors(1);
      logError("Unexpected error: " + e.getMessage());
      logError(Const.getStackTracker(e));
    } finally {
      try {
        if (connection != null) {
          connection.disconnect();
          connection = null;
        } else {
          if (mailConn != null) {
            mailConn.disconnect();
            mailConn = null;
          }
        }
      } catch (Exception e) {
        /* Ignore */
      }
    }

    return result;
  }

  void fetchOneFolder(
      boolean usePOP3,
      String realIMAPFolder,
      String realOutputFolder,
      String targetAttachmentFolder,
      String realMoveToIMAPFolder,
      String realFilenamePattern,
      int nbrmailtoretrieve,
      SimpleDateFormat df)
      throws HopException {
    try {
      // if it is not pop3 and we have non-default imap folder...
      if (!usePOP3 && !Utils.isEmpty(realIMAPFolder)) {
        if (connection != null) {
          connection.openFolder(realIMAPFolder, true);
        } else {
          mailConn.openFolder(realIMAPFolder, true);
        }
      } else {
        if (connection != null) {
          connection.openFolder(true);
        } else {
          mailConn.openFolder(true);
        }
      }

      if (connection != null) {
        connection.retrieveMessages();
      } else {
        mailConn.retrieveMessages();
      }

      int messagesCount;
      if (connection != null) {
        messagesCount = connection.getMessagesCount();
      } else {
        messagesCount = mailConn.getMessagesCount();
      }

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ActionGetMailsFromPOP.TotalMessagesFolder.Label",
                "" + messagesCount,
                (connection != null)
                    ? Const.NVL(connection.getFolderName(), MailServerConnection.INBOX_FOLDER)
                    : Const.NVL(mailConn.getFolderName(), MailConnectionMeta.INBOX_FOLDER)));
      }

      messagesCount =
          nbrmailtoretrieve > 0
              ? (nbrmailtoretrieve > messagesCount ? messagesCount : nbrmailtoretrieve)
              : messagesCount;

      if (messagesCount > 0) {
        if (connection != null) {
          switch (getActionType()) {
            case MailServerConnection.ACTION_TYPE_DELETE:
              if (nbrmailtoretrieve > 0) {
                // We need to fetch all messages in order to retrieve
                // only the first nbrmailtoretrieve ...
                for (int i = 0; i < messagesCount && !parentWorkflow.isStopped(); i++) {
                  // Get next message
                  connection.fetchNext();
                  // Delete this message
                  connection.deleteMessage();
                  if (isDebug()) {
                    logDebug(BaseMessages.getString(PKG, CONST_MESSAGE_DELETED, "" + i));
                  }
                }
              } else {
                // Delete messages
                connection.deleteMessages(true);
                if (isDebug()) {
                  logDebug(
                      BaseMessages.getString(
                          PKG, "ActionGetMailsFromPOP.MessagesDeleted", "" + messagesCount));
                }
              }
              break;
            case MailServerConnection.ACTION_TYPE_MOVE:
              if (nbrmailtoretrieve > 0) {
                // We need to fetch all messages in order to retrieve
                // only the first nbrmailtoretrieve ...
                for (int i = 0; i < messagesCount && !parentWorkflow.isStopped(); i++) {
                  // Get next message
                  connection.fetchNext();
                  // Move this message
                  connection.moveMessage();
                  if (isDebug()) {
                    logDebug(
                        BaseMessages.getString(
                            PKG,
                            ACTION_GET_MAILS_FROM_POP_MESSAGE_MOVED,
                            "" + i,
                            realMoveToIMAPFolder));
                  }
                }
              } else {
                // Move all messages
                connection.moveMessages();
                if (isDebug()) {
                  logDebug(
                      BaseMessages.getString(
                          PKG,
                          "ActionGetMailsFromPOP.MessagesMoved",
                          "" + messagesCount,
                          realMoveToIMAPFolder));
                }
              }
              break;
            default:
              // Get messages and save it in a local file
              // also save attached files if needed
              for (int i = 0; i < messagesCount && !parentWorkflow.isStopped(); i++) {
                // Get next message
                connection.fetchNext();
                int messagenumber = connection.getMessage().getMessageNumber();
                boolean okPOP3 = usePOP3 ? true : false;
                boolean okIMAP = !usePOP3;

                if (okPOP3 || okIMAP) {
                  // display some infos on the current message
                  //
                  if (isDebug() && connection.getMessage() != null) {
                    logDebug("--------------------------------------------------");
                    logDebug(
                        BaseMessages.getString(
                            PKG, "ActionGetMailsFromPOP.MessageNumber.Label", "" + messagenumber));
                    if (connection.getMessage().getReceivedDate() != null) {
                      logDebug(
                          BaseMessages.getString(
                              PKG,
                              "ActionGetMailsFromPOP.ReceivedDate.Label",
                              df.format(connection.getMessage().getReceivedDate())));
                    }
                    logDebug(
                        BaseMessages.getString(
                            PKG,
                            "ActionGetMailsFromPOP.ContentType.Label",
                            connection.getMessage().getContentType()));
                    logDebug(
                        BaseMessages.getString(
                            PKG,
                            "ActionGetMailsFromPOP.EmailFrom.Label",
                            Const.NVL(connection.getMessage().getFrom()[0].toString(), "")));
                    logDebug(
                        BaseMessages.getString(
                            PKG,
                            "ActionGetMailsFromPOP.EmailSubject.Label",
                            Const.NVL(connection.getMessage().getSubject(), "")));
                  }
                  if (isSaveMessage()) {
                    // get local message filename
                    String localfilenameMessage = replaceTokens(realFilenamePattern, i);

                    if (isDebug()) {
                      logDebug(
                          BaseMessages.getString(
                              PKG,
                              "ActionGetMailsFromPOP.LocalFilename.Label",
                              localfilenameMessage));
                    }

                    // save message content in the file
                    connection.saveMessageContentToFile(localfilenameMessage, realOutputFolder);
                    // explicitly set message as read
                    connection.getMessage().setFlag(Flag.SEEN, true);

                    if (isDetailed()) {
                      logDetailed(
                          BaseMessages.getString(
                              PKG,
                              "ActionGetMailsFromPOP.MessageSaved.Label",
                              "" + messagenumber,
                              localfilenameMessage,
                              realOutputFolder));
                    }
                  }

                  // Do we need to save attached file?
                  if (isSaveAttachment()) {
                    connection.saveAttachedFiles(targetAttachmentFolder, attachementPattern);
                  }
                  // We successfully retrieved message
                  // do we need to make another action (delete, move)?
                  if (usePOP3) {
                    if (isDelete()) {
                      connection.deleteMessage();
                      if (isDebug()) {
                        logDebug(
                            BaseMessages.getString(PKG, CONST_MESSAGE_DELETED, "" + messagenumber));
                      }
                    }
                  } else {
                    switch (getAfterGetIMAP()) {
                      case MailServerConnection.AFTER_GET_IMAP_DELETE:
                        // Delete messages
                        connection.deleteMessage();
                        if (isDebug()) {
                          logDebug(
                              BaseMessages.getString(
                                  PKG, CONST_MESSAGE_DELETED, "" + messagenumber));
                        }
                        break;
                      case MailServerConnection.AFTER_GET_IMAP_MOVE:
                        // Move messages
                        connection.moveMessage();
                        if (isDebug()) {
                          logDebug(
                              BaseMessages.getString(
                                  PKG,
                                  ACTION_GET_MAILS_FROM_POP_MESSAGE_MOVED,
                                  "" + messagenumber,
                                  realMoveToIMAPFolder));
                        }
                        break;
                      default:
                    }
                  }
                }
              }
              break;
          }
        } else {
          switch (getActionType()) {
            case MailConnectionMeta.ACTION_TYPE_DELETE:
              if (nbrmailtoretrieve > 0) {
                // We need to fetch all messages in order to retrieve
                // only the first nbrmailtoretrieve ...
                for (int i = 0; i < messagesCount && !parentWorkflow.isStopped(); i++) {
                  // Get next message
                  mailConn.fetchNext();
                  // Delete this message
                  mailConn.deleteMessage();
                  if (isDebug()) {
                    logDebug(BaseMessages.getString(PKG, CONST_MESSAGE_DELETED, "" + i));
                  }
                }
              } else {
                // Delete messages
                mailConn.deleteMessages(true);
                if (isDebug()) {
                  logDebug(
                      BaseMessages.getString(
                          PKG, "ActionGetMailsFromPOP.MessagesDeleted", "" + messagesCount));
                }
              }
              break;
            case MailConnectionMeta.ACTION_TYPE_MOVE:
              if (nbrmailtoretrieve > 0) {
                // We need to fetch all messages in order to retrieve
                // only the first nbrmailtoretrieve ...
                for (int i = 0; i < messagesCount && !parentWorkflow.isStopped(); i++) {
                  // Get next message
                  mailConn.fetchNext();
                  // Move this message
                  mailConn.moveMessage();
                  if (isDebug()) {
                    logDebug(
                        BaseMessages.getString(
                            PKG,
                            ACTION_GET_MAILS_FROM_POP_MESSAGE_MOVED,
                            "" + i,
                            realMoveToIMAPFolder));
                  }
                }
              } else {
                // Move all messages
                mailConn.moveMessages();
                if (isDebug()) {
                  logDebug(
                      BaseMessages.getString(
                          PKG,
                          "ActionGetMailsFromPOP.MessagesMoved",
                          "" + messagesCount,
                          realMoveToIMAPFolder));
                }
              }
              break;
            default:
              // Get messages and save it in a local file
              // also save attached files if needed
              for (int i = 0; i < messagesCount && !parentWorkflow.isStopped(); i++) {
                // Get next message
                mailConn.fetchNext();
                int messagenumber = mailConn.getMessage().getMessageNumber();
                boolean okPOP3 = usePOP3 ? true : false;
                boolean okIMAP = !usePOP3;

                if (okPOP3 || okIMAP) {
                  // display some infos on the current message
                  //
                  if (isDebug() && mailConn.getMessage() != null) {
                    logDebug("--------------------------------------------------");
                    logDebug(
                        BaseMessages.getString(
                            PKG, "ActionGetMailsFromPOP.MessageNumber.Label", "" + messagenumber));
                    if (mailConn.getMessage().getReceivedDate() != null) {
                      logDebug(
                          BaseMessages.getString(
                              PKG,
                              "ActionGetMailsFromPOP.ReceivedDate.Label",
                              df.format(mailConn.getMessage().getReceivedDate())));
                    }
                    logDebug(
                        BaseMessages.getString(
                            PKG,
                            "ActionGetMailsFromPOP.ContentType.Label",
                            mailConn.getMessage().getContentType()));
                    logDebug(
                        BaseMessages.getString(
                            PKG,
                            "ActionGetMailsFromPOP.EmailFrom.Label",
                            Const.NVL(mailConn.getMessage().getFrom()[0].toString(), "")));
                    logDebug(
                        BaseMessages.getString(
                            PKG,
                            "ActionGetMailsFromPOP.EmailSubject.Label",
                            Const.NVL(mailConn.getMessage().getSubject(), "")));
                  }
                  if (isSaveMessage()) {
                    // get local message filename
                    String localfilenameMessage = replaceTokens(realFilenamePattern, i);

                    if (isDebug()) {
                      logDebug(
                          BaseMessages.getString(
                              PKG,
                              "ActionGetMailsFromPOP.LocalFilename.Label",
                              localfilenameMessage));
                    }

                    // save message content in the file
                    mailConn.saveMessageContentToFile(localfilenameMessage, realOutputFolder);
                    // explicitly set message as read
                    mailConn.getMessage().setFlag(Flag.SEEN, true);

                    if (isDetailed()) {
                      logDetailed(
                          BaseMessages.getString(
                              PKG,
                              "ActionGetMailsFromPOP.MessageSaved.Label",
                              "" + messagenumber,
                              localfilenameMessage,
                              realOutputFolder));
                    }
                  }

                  // Do we need to save attached file?
                  if (isSaveAttachment()) {
                    mailConn.saveAttachedFiles(targetAttachmentFolder, attachementPattern);
                  }
                  // We successfully retrieved message
                  // do we need to make another action (delete, move)?
                  if (usePOP3) {
                    if (isDelete()) {
                      mailConn.deleteMessage();
                      if (isDebug()) {
                        logDebug(
                            BaseMessages.getString(PKG, CONST_MESSAGE_DELETED, "" + messagenumber));
                      }
                    }
                  } else {
                    switch (getAfterGetIMAP()) {
                      case MailConnectionMeta.AFTER_GET_IMAP_DELETE:
                        // Delete messages
                        mailConn.deleteMessage();
                        if (isDebug()) {
                          logDebug(
                              BaseMessages.getString(
                                  PKG, CONST_MESSAGE_DELETED, "" + messagenumber));
                        }
                        break;
                      case MailConnectionMeta.AFTER_GET_IMAP_MOVE:
                        // Move messages
                        mailConn.moveMessage();
                        if (isDebug()) {
                          logDebug(
                              BaseMessages.getString(
                                  PKG,
                                  ACTION_GET_MAILS_FROM_POP_MESSAGE_MOVED,
                                  "" + messagenumber,
                                  realMoveToIMAPFolder));
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
      }
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  private String replaceTokens(String aString, int idfile) {
    String localfilenameMessage = aString;
    localfilenameMessage = localfilenameMessage.replaceAll(FILENAME_ID_PATTERN, "" + (idfile + 1));
    localfilenameMessage =
        substituteDate(
            localfilenameMessage, FILENAME_SYS_DATE_OPEN, FILENAME_SYS_DATE_CLOSE, new Date());
    return localfilenameMessage;
  }

  private String substituteDate(String aString, String open, String close, Date datetime) {
    if (aString == null) {
      return null;
    }
    StringBuilder buffer = new StringBuilder();
    String rest = aString;

    // search for closing string
    int i = rest.indexOf(open);
    while (i > -1) {
      int j = rest.indexOf(close, i + open.length());
      // search for closing string
      if (j > -1) {
        String varName = rest.substring(i + open.length(), j);
        DateFormat dateFormat = new SimpleDateFormat(varName);
        Object value = dateFormat.format(datetime);

        buffer.append(rest.substring(0, i));
        buffer.append(value);
        rest = rest.substring(j + close.length());
      } else {
        // no closing tag found; end the search
        buffer.append(rest);
        rest = "";
      }
      // keep searching
      i = rest.indexOf(close);
    }
    buffer.append(rest);
    return buffer.toString();
  }

  private void initVariables() {
    // Attachment wildcard
    attachementPattern = null;
    String realAttachmentWildcard = resolve(getAttachmentWildcard());
    if (!Utils.isEmpty(realAttachmentWildcard)) {
      attachementPattern = Pattern.compile(realAttachmentWildcard);
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "serverName",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "userName",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            CONST_PASSWORD,
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notBlankValidator(), ActionValidatorUtils.fileExistsValidator());
    ActionValidatorUtils.andValidator().validate(this, "outputDirectory", remarks, ctx);

    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "SSLPort",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.integerValidator()));
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (connection != null && !Utils.isEmpty(serverName)) {
      String realServername = resolve(serverName);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realServername, ResourceType.SERVER));
      references.add(reference);
    }
    return references;
  }

  String createOutputDirectory(int folderType)
      throws HopException, FileSystemException, IllegalArgumentException {
    if ((folderType != ActionGetPOP.FOLDER_OUTPUT)
        && (folderType != ActionGetPOP.FOLDER_ATTACHMENTS)) {
      throw new IllegalArgumentException("Invalid folderType argument");
    }
    String folderName = "";
    switch (folderType) {
      case ActionGetPOP.FOLDER_OUTPUT:
        folderName = getRealOutputDirectory();
        break;
      case ActionGetPOP.FOLDER_ATTACHMENTS:
        if (isSaveAttachment() && isUseDifferentFolderForAttachment()) {
          folderName = getRealAttachmentFolder();
        } else {
          folderName = getRealOutputDirectory();
        }
        break;
      default:
        break;
    }
    if (Utils.isEmpty(folderName)) {
      switch (folderType) {
        case ActionGetPOP.FOLDER_OUTPUT:
          throw new HopException(
              BaseMessages.getString(PKG, "ActionGetMailsFromPOP.Error.OutputFolderEmpty"));
        case ActionGetPOP.FOLDER_ATTACHMENTS:
          throw new HopException(
              BaseMessages.getString(PKG, "ActionGetMailsFromPOP.Error.AttachmentFolderEmpty"));
        default:
          break;
      }
    }
    FileObject folder = HopVfs.getFileObject(folderName);
    if (folder.exists()) {
      if (folder.getType() != FileType.FOLDER) {
        switch (folderType) {
          case ActionGetPOP.FOLDER_OUTPUT:
            throw new HopException(
                BaseMessages.getString(
                    PKG, "ActionGetMailsFromPOP.Error.NotAFolderNot", folderName));
          case ActionGetPOP.FOLDER_ATTACHMENTS:
            throw new HopException(
                BaseMessages.getString(
                    PKG, "ActionGetMailsFromPOP.Error.AttachmentFolderNotAFolder", folderName));
          default:
            break;
        }
      }
      if (isDebug()) {
        switch (folderType) {
          case ActionGetPOP.FOLDER_OUTPUT:
            logDebug(
                BaseMessages.getString(
                    PKG, "ActionGetMailsFromPOP.Log.OutputFolderExists", folderName));
            break;
          case ActionGetPOP.FOLDER_ATTACHMENTS:
            logDebug(
                BaseMessages.getString(
                    PKG, "ActionGetMailsFromPOP.Log.AttachmentFolderExists", folderName));
            break;
          default:
            break;
        }
      }
    } else {
      if (isCreateLocalFolder()) {
        folder.createFolder();
      } else {
        switch (folderType) {
          case ActionGetPOP.FOLDER_OUTPUT:
            throw new HopException(
                BaseMessages.getString(
                    PKG, "ActionGetMailsFromPOP.Error.OutputFolderNotExist", folderName));
          case ActionGetPOP.FOLDER_ATTACHMENTS:
            throw new HopException(
                BaseMessages.getString(
                    PKG, "ActionGetMailsFromPOP.Error.AttachmentFolderNotExist", folderName));
          default:
            break;
        }
      }
    }

    String returnValue = HopVfs.getFilename(folder);
    try {
      folder.close();
    } catch (IOException ignore) {
      // Ignore error, as the folder was created successfully
    }
    return returnValue;
  }
}
