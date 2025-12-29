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

package org.apache.hop.mail.pipeline.transforms.mailinput;

import jakarta.mail.Header;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import org.apache.commons.collections4.iterators.ArrayIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mail.metadata.MailServerConnection;
import org.apache.hop.mail.workflow.actions.getpop.MailConnection;
import org.apache.hop.mail.workflow.actions.getpop.MailConnectionMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Read data from POP3/IMAP server and input data to the next transforms. */
public class MailInput extends BaseTransform<MailInputMeta, MailInputData> {
  private static final Class<?> PKG = MailInputMeta.class;
  public static final String CONST_MAIL_INPUT_LOG_FINISHED_PROCESSING =
      "MailInput.Log.FinishedProcessing";

  private MessageParser instance = new MessageParser();

  private MailServerConnection connection;

  private boolean connected = false;

  public MailInput(
      TransformMeta transformMeta,
      MailInputMeta meta,
      MailInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] outputRowData = getOneRow();

    if (outputRowData == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if (isRowLevel()) {
      logRowlevel(
          toString(),
          BaseMessages.getString(
              PKG, "MailInput.Log.OutputRow", data.outputRowMeta.getString(outputRowData)));
    }
    putRow(data.outputRowMeta, outputRowData); // copy row to output rowset(s)

    if (data.rowlimit > 0 && data.rownr >= data.rowlimit) { // limit has been reached: stop now.
      setOutputDone();
      return false;
    }

    return true;
  }

  public String[] getFolders(String realIMAPFolder) throws HopException {
    data.folderenr = 0;
    data.messagesCount = 0;
    data.rownr = 0;
    String[] folderslist = null;
    if (meta.isIncludeSubFolders()) {
      String[] folderslist0 = null;
      if (connection != null) {
        folderslist0 = connection.returnAllFolders(realIMAPFolder);
      } else {
        folderslist0 = data.mailConn.returnAllFolders(realIMAPFolder);
      }
      if (folderslist0 == null || folderslist0.length == 0) {
        if (connection != null) {
          folderslist =
              connection.getProtocol().equals(MailServerConnection.PROTOCOL_MBOX)
                  ? new String[] {""}
                  : new String[] {Const.NVL(realIMAPFolder, MailServerConnection.INBOX_FOLDER)};
        } else {
          // mstor's default folder has no name
          folderslist =
              data.mailConn.getProtocol() == MailConnectionMeta.PROTOCOL_MBOX
                  ? new String[] {""}
                  : new String[] {Const.NVL(realIMAPFolder, MailConnectionMeta.INBOX_FOLDER)};
        }
      } else {
        folderslist = new String[folderslist0.length + 1];
        if (connection != null) {
          folderslist[0] = Const.NVL(realIMAPFolder, MailServerConnection.INBOX_FOLDER);
        } else {
          folderslist[0] = Const.NVL(realIMAPFolder, MailConnectionMeta.INBOX_FOLDER);
        }
        for (int i = 0; i < folderslist0.length; i++) {
          folderslist[i + 1] = folderslist0[i];
        }
      }
    } else {
      if (connection != null) {
        folderslist =
            connection.getProtocol().equals(MailServerConnection.PROTOCOL_MBOX)
                ? new String[] {""}
                : new String[] {Const.NVL(realIMAPFolder, MailServerConnection.INBOX_FOLDER)};
      } else {
        folderslist =
            data.mailConn.getProtocol() == MailConnectionMeta.PROTOCOL_MBOX
                ? new String[] {""}
                : new String[] {Const.NVL(realIMAPFolder, MailConnectionMeta.INBOX_FOLDER)};
      }
    }
    return folderslist;
  }

  private void applySearch(Date beginDate, Date endDate) {
    // apply search term?
    String realSearchSender = resolve(meta.getSenderSearch());
    if (!Utils.isEmpty(realSearchSender)) {
      // apply FROM
      if (connection != null) {
        connection.setSenderTerm(realSearchSender, meta.isNotTermSenderSearch());
      } else {
        data.mailConn.setSenderTerm(realSearchSender, meta.isNotTermSenderSearch());
      }
    }

    String realSearchReceipient = resolve(meta.getRecipientSearch());
    if (!Utils.isEmpty(realSearchReceipient)) {
      // apply TO
      if (connection != null) {
        connection.setReceipientTerm(realSearchReceipient);
      } else {
        data.mailConn.setReceipientTerm(realSearchReceipient);
      }
    }

    String realSearchSubject = resolve(meta.getSubjectSearch());
    if (!Utils.isEmpty(realSearchSubject)) {
      // apply Subject
      if (connection != null) {
        connection.setSubjectTerm(realSearchSubject, meta.isNotTermSubjectSearch());
      } else {
        data.mailConn.setSubjectTerm(realSearchSubject, meta.isNotTermSubjectSearch());
      }
    }

    // Received Date
    if (connection != null) {
      switch (meta.getConditionReceivedDate()) {
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
      switch (meta.getConditionReceivedDate()) {
        case MailConnectionMeta.CONDITION_DATE_EQUAL:
          data.mailConn.setReceivedDateTermEQ(beginDate);
          break;
        case MailConnectionMeta.CONDITION_DATE_GREATER:
          data.mailConn.setReceivedDateTermGT(beginDate);
          break;
        case MailConnectionMeta.CONDITION_DATE_SMALLER:
          data.mailConn.setReceivedDateTermLT(beginDate);
          break;
        case MailConnectionMeta.CONDITION_DATE_BETWEEN:
          data.mailConn.setReceivedDateTermBetween(beginDate, endDate);
          break;
        default:
          break;
      }
    }

    // set FlagTerm?
    if (!data.usePOP) {
      // POP3 does not support any flags.
      // but still use ones for IMAP and maybe for MBOX?
      if (connection != null) {
        switch (meta.getValueImapList()) {
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
        switch (meta.getValueImapList()) {
          case MailConnectionMeta.VALUE_IMAP_LIST_NEW:
            data.mailConn.setFlagTermNew();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_OLD:
            data.mailConn.setFlagTermOld();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_READ:
            data.mailConn.setFlagTermRead();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_UNREAD:
            data.mailConn.setFlagTermUnread();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_FLAGGED:
            data.mailConn.setFlagTermFlagged();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_NOT_FLAGGED:
            data.mailConn.setFlagTermNotFlagged();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_DRAFT:
            data.mailConn.setFlagTermDraft();
            break;
          case MailConnectionMeta.VALUE_IMAP_LIST_NOT_DRAFT:
            data.mailConn.setFlagTermNotDraft();
            break;
          default:
            break;
        }
      }
    }
  }

  /**
   * Build an empty row based on the meta-data...
   *
   * @return
   */
  private Object[] buildEmptyRow() {
    return RowDataUtil.allocateRowData(data.outputRowMeta.size());
  }

  private boolean isFolderExausted() {
    return data.folder == null || !data.folderIterator.hasNext();
  }

  private Object[] getOneRow() throws HopException {

    while (isFolderExausted()) {
      if (!openNextFolder()) {
        return null;
      }
    }

    Object[] r = buildEmptyRow();
    if (meta.isUseDynamicFolder()) {
      System.arraycopy(data.readrow, 0, r, 0, data.readrow.length);
    }

    try {

      Message message = data.folderIterator.next();

      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG, "MailInput.Log.FetchingMessage", message.getMessageNumber()));
      }

      try {
        instance.parseToArray(r, message);
      } catch (Exception e) {
        String msg = e.getMessage();
        if (meta.isStopOnError()) {
          throw new HopException(msg, e);
        } else {
          logError(msg, e);
        }
      }

      incrementLinesInput();
      data.rownr++;

    } catch (Exception e) {
      throw new HopException("Error adding values to row!", e);
    }

    return r;
  }

  private boolean openNextFolder() {
    try {
      if (!meta.isUseDynamicFolder()) {
        // static folders list
        // let's check if we fetched all values in list
        if (data.folderenr >= data.folders.length) {
          // We have fetched all folders
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, CONST_MAIL_INPUT_LOG_FINISHED_PROCESSING));
          }
          return false;
        }
      } else {
        // dynamic folders
        if (first) {
          first = false;

          data.readrow = getRow(); // Get row from input rowset & set row busy!
          if (data.readrow == null) {
            if (isDetailed()) {
              logDetailed(BaseMessages.getString(PKG, CONST_MAIL_INPUT_LOG_FINISHED_PROCESSING));
            }
            return false;
          }

          data.inputRowMeta = getInputRowMeta();
          data.outputRowMeta = data.inputRowMeta.clone();
          meta.getFields(
              data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

          // Get total previous fields
          data.totalpreviousfields = data.inputRowMeta.size();

          if (Utils.isEmpty(meta.getFolderField())) {
            logError(BaseMessages.getString(PKG, "MailInput.Error.DynamicFolderFieldMissing"));
            stopAll();
            setErrors(1);
            return false;
          }

          data.indexOfFolderField = data.inputRowMeta.indexOfValue(meta.getFolderField());
          if (data.indexOfFolderField < 0) {
            logError(
                BaseMessages.getString(
                    PKG, "MailInput.Error.DynamicFolderUnreachable", meta.getFolderField()));
            stopAll();
            setErrors(1);
            return false;
          }

          // get folder
          String folderName = data.inputRowMeta.getString(data.readrow, data.indexOfFolderField);
          if (isDebug()) {
            logDebug(
                BaseMessages.getString(
                    PKG, "MailInput.Log.FoldernameInStream", meta.getFolderField(), folderName));
          }
          data.folders = getFolders(folderName);
        } // end if first

        if (data.folderenr >= data.folders.length) {
          // we have fetched all values for input row
          // grab another row
          data.readrow = getRow(); // Get row from input rowset & set row busy!
          if (data.readrow == null) {
            if (isDetailed()) {
              logDetailed(BaseMessages.getString(PKG, CONST_MAIL_INPUT_LOG_FINISHED_PROCESSING));
            }
            return false;
          }
          // get folder
          String folderName = data.inputRowMeta.getString(data.readrow, data.indexOfFolderField);
          data.folders = getFolders(folderName);
        }
      }

      data.start = parseIntWithSubstitute(meta.getStart());
      data.end = parseIntWithSubstitute(meta.getEnd());
      // Get the current folder
      data.folder = data.folders[data.folderenr];

      // Move folder pointer ahead!
      data.folderenr++;

      // open folder
      if (!data.usePOP && !Utils.isEmpty(data.folder)) {
        if (connection != null) {
          connection.openFolder(data.folder, true);
        } else {
          data.mailConn.openFolder(data.folder, false);
        }
      } else {
        if (connection != null) {
          connection.openFolder(false);
        } else {
          data.mailConn.openFolder(false);
        }
      }

      if (meta.isUseBatch()
          || (!Utils.isEmpty(resolve(meta.getFirstMails()))
              && Integer.parseInt(resolve(meta.getFirstMails())) > 0)) {
        // get data by pieces
        Integer batchSize =
            meta.isUseBatch()
                ? meta.getBatchSize()
                : Integer.parseInt(resolve(meta.getFirstMails()));
        Integer start = meta.isUseBatch() ? data.start : 1;
        Integer end = meta.isUseBatch() ? data.end : batchSize;

        if (connection != null) {
          data.folderIterator =
              new BatchFolderIterator(
                  data.mailConn.getFolder(), batchSize, start, end); // TODO:args

          if (data.mailConn.getSearchTerm() != null) { // add search filter
            data.folderIterator =
                new SearchEnabledFolderIterator(data.folderIterator, data.mailConn.getSearchTerm());
          }
        } else {
          data.folderIterator =
              new BatchFolderIterator(connection.getFolder(), batchSize, start, end);
          if (connection.getSearchTerm() != null) {
            data.folderIterator =
                new SearchEnabledFolderIterator(data.folderIterator, connection.getSearchTerm());
          }
        }
      } else { // fetch all
        if (connection != null) {
          connection.retrieveMessages();
          data.folderIterator = new ArrayIterator(connection.getMessages());
        } else {
          data.mailConn.retrieveMessages();
          data.folderIterator = new ArrayIterator(data.mailConn.getMessages());
        }
      }

      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG, "MailInput.Log.MessagesInFolder", data.folder, data.messagesCount));
      }

    } catch (Exception e) {
      logError("Error opening folder " + data.folderenr + " " + data.folder + ": " + e.toString());
      logError(Const.getStackTracker(e));
      stopAll();
      setErrors(1);
      return false;
    }
    return true;
  }

  @Override
  public boolean init() {

    if (!super.init()) {
      return false;
    }

    if (!meta.isUseDynamicFolder()) {
      try {
        // Create the output row meta-data
        data.outputRowMeta = new RowMeta();
        meta.getFields(
            data.outputRowMeta, getTransformName(), null, null, this, metadataProvider); // get the
        // metadata
        // populated

      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "MailInput.ErrorInit", e.toString()));
        logError(Const.getStackTracker(e));
        return false;
      }
    }

    Date beginDate = null;
    Date endDate = null;
    SimpleDateFormat df = new SimpleDateFormat(MailInputMeta.DATE_PATTERN);

    if (!StringUtils.isEmpty(meta.getConnectionName())) {
      try {
        connection =
            metadataProvider
                .getSerializer(MailServerConnection.class)
                .load(meta.getConnectionName());
      } catch (HopException e) {
        throw new RuntimeException(
            "Mail Server Connection " + meta.getConnectionName() + " could not be found");
      }
      try {
        connection.getSession(variables);
        connection.getStore().connect();
        connected = true;
      } catch (MessagingException e) {
        logError(
            "A connection to mail server connection '"
                + meta.getConnectionName()
                + "' could not be established",
            e);
      }
    } else {
      data.usePOP = meta.getProtocol().equals(MailConnectionMeta.PROTOCOL_STRING_POP3);

      String realserver = resolve(meta.getServerName());
      if (meta.getProtocol().equals(MailConnectionMeta.PROTOCOL_STRING_MBOX)
          && StringUtils.startsWith(realserver, "file://")) {
        realserver = StringUtils.remove(realserver, "file://");
      }

      String realusername = resolve(meta.getUsername());
      String realpassword = Utils.resolvePassword(variables, meta.getPassword());
      int realport = Const.toInt(resolve(meta.getSslPort()), -1);
      String realProxyUsername = resolve(meta.getProxyUsername());

      try {
        // create a mail connection object
        data.mailConn =
            new MailConnection(
                getLogChannel(),
                MailConnectionMeta.getProtocolFromString(
                    meta.getProtocol(), MailConnectionMeta.PROTOCOL_IMAP),
                realserver,
                realport,
                realusername,
                realpassword,
                meta.isUseSsl(),
                meta.isUseXOAuth2(),
                meta.isUseProxy(),
                realProxyUsername);
        // connect
        data.mailConn.connect();
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "MailInput.Error.OpeningConnection", e.getMessage()));
        setErrors(1);
        stopAll();
      }
    }

    if (!meta.isUseDynamicFolder()) {
      // Limit field has absolute priority
      String reallimitrow = resolve(meta.getRowLimit());
      int limit = Const.toInt(reallimitrow, 0);
      // Limit field has absolute priority
      if (limit == 0) {
        limit = getReadFirst(meta.getProtocol());
      }
      data.rowlimit = limit;
    }

    // check search terms
    // Received Date
    try {
      if (connection != null) {
        switch (meta.getConditionReceivedDate()) {
          case MailServerConnection.CONDITION_DATE_EQUAL,
              MailServerConnection.CONDITION_DATE_GREATER,
              MailServerConnection.CONDITION_DATE_SMALLER:
            String realBeginDate = resolve(meta.getReceivedDate1());
            if (Utils.isEmpty(realBeginDate)) {
              throw new HopException(
                  BaseMessages.getString(PKG, "MailInput.Error.ReceivedDateSearchTermEmpty"));
            }
            beginDate = df.parse(realBeginDate);
            break;
          case MailServerConnection.CONDITION_DATE_BETWEEN:
            realBeginDate = resolve(meta.getReceivedDate1());
            if (Utils.isEmpty(realBeginDate)) {
              throw new HopException(
                  BaseMessages.getString(PKG, "MailInput.Error.ReceivedDatesSearchTermEmpty"));
            }
            beginDate = df.parse(realBeginDate);
            String realEndDate = resolve(meta.getReceivedDate2());
            if (Utils.isEmpty(realEndDate)) {
              throw new HopException(
                  BaseMessages.getString(PKG, "MailInput.Error.ReceivedDatesSearchTermEmpty"));
            }
            endDate = df.parse(realEndDate);
            break;
          default:
            break;
        }
      } else {
        switch (meta.getConditionReceivedDate()) {
          case MailConnectionMeta.CONDITION_DATE_EQUAL,
              MailConnectionMeta.CONDITION_DATE_GREATER,
              MailConnectionMeta.CONDITION_DATE_SMALLER:
            String realBeginDate = resolve(meta.getReceivedDate1());
            if (Utils.isEmpty(realBeginDate)) {
              throw new HopException(
                  BaseMessages.getString(PKG, "MailInput.Error.ReceivedDateSearchTermEmpty"));
            }
            beginDate = df.parse(realBeginDate);
            break;
          case MailConnectionMeta.CONDITION_DATE_BETWEEN:
            realBeginDate = resolve(meta.getReceivedDate1());
            if (Utils.isEmpty(realBeginDate)) {
              throw new HopException(
                  BaseMessages.getString(PKG, "MailInput.Error.ReceivedDatesSearchTermEmpty"));
            }
            beginDate = df.parse(realBeginDate);
            String realEndDate = resolve(meta.getReceivedDate2());
            if (Utils.isEmpty(realEndDate)) {
              throw new HopException(
                  BaseMessages.getString(PKG, "MailInput.Error.ReceivedDatesSearchTermEmpty"));
            }
            endDate = df.parse(realEndDate);
            break;
          default:
            break;
        }
      }
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "MailInput.Error.SettingSearchTerms", e.getMessage()));
      setErrors(1);
      stopAll();
    }

    // Need to apply search filters?
    applySearch(beginDate, endDate);

    try {
      if (!meta.isUseDynamicFolder()) {
        // pass static folder name
        String realIMAPFolder = resolve(meta.getImapFolder());
        // return folders list
        // including sub folders if necessary
        data.folders = getFolders(realIMAPFolder);
      }
    } catch (HopException e) {
      logError(BaseMessages.getString(PKG, "MailInput.Error.GettingFolders", e.getMessage()));
    }

    data.nrFields = meta.getInputFields() != null ? meta.getInputFields().size() : 0;

    return true;
  }

  private int getReadFirst(String protocol) {
    if (connection != null) {
      if (protocol.equals(MailServerConnection.PROTOCOL_STRING_IMAP)
          || protocol.equals(MailServerConnection.PROTOCOL_STRING_POP3)) {
        return Const.toInt(meta.getFirstMails(), 0);
      }
    } else {
      if (protocol.equals(MailConnectionMeta.PROTOCOL_STRING_POP3)) {
        return Const.toInt(meta.getFirstMails(), 0);
      }
      if (protocol.equals(MailConnectionMeta.PROTOCOL_STRING_IMAP)) {
        return Const.toInt(meta.getImapFirstMails(), 0);
      }
    }
    // and we do not have this option for MBOX on UI.
    return 0;
  }

  @Override
  public void dispose() {

    if (connection != null) {
      try {
        connection.disconnect();
      } catch (HopException e) {
        logError(BaseMessages.getString(PKG, "MailInput.Error.Disconnecting", e.getMessage()));
      }
    } else {
      if (data.mailConn != null) {
        try {
          data.mailConn.disconnect();
          data.mailConn = null;
        } catch (Exception e) {
          /* Ignore */
        }
      }
    }

    super.dispose();
  }

  private Integer parseIntWithSubstitute(String toParse) {
    toParse = resolve(toParse);
    if (!StringUtils.isEmpty(toParse)) {
      try {
        return Integer.parseInt(toParse);
      } catch (NumberFormatException e) {
        logError(e.getLocalizedMessage());
      }
    }
    return null;
  }

  /** Extracted message parse algorithm to be able to unit test separately */
  class MessageParser {

    Object[] parseToArray(Object[] r, Message message) throws Exception {

      // Execute for each Input field...
      for (int i = 0; i < data.nrFields; i++) {
        int index = data.totalpreviousfields + i;

        try {
          switch (meta.getInputFields().get(i).getColumn()) {
            case MailInputField.COLUMN_MESSAGE_NR:
              r[index] = (long) message.getMessageNumber();
              break;
            case MailInputField.COLUMN_SUBJECT:
              r[index] = message.getSubject();
              break;
            case MailInputField.COLUMN_SENDER:
              r[index] = StringUtils.join(message.getFrom(), ";");
              break;
            case MailInputField.COLUMN_REPLY_TO:
              r[index] = StringUtils.join(message.getReplyTo(), ";");
              break;
            case MailInputField.COLUMN_RECIPIENTS:
              r[index] = StringUtils.join(message.getAllRecipients(), ";");
              break;
            case MailInputField.COLUMN_DESCRIPTION:
              r[index] = message.getDescription();
              break;
            case MailInputField.COLUMN_BODY:
              if (connection != null) {
                r[index] = connection.getMessageBody(message);
              } else {
                r[index] = data.mailConn.getMessageBody(message);
              }
              break;
            case MailInputField.COLUMN_RECEIVED_DATE:
              Date receivedDate = message.getReceivedDate();
              r[index] = receivedDate != null ? new Date(receivedDate.getTime()) : null;
              break;
            case MailInputField.COLUMN_SENT_DATE:
              Date sentDate = message.getSentDate();
              r[index] = sentDate != null ? new Date(sentDate.getTime()) : null;
              break;
            case MailInputField.COLUMN_CONTENT_TYPE:
              r[index] = message.getContentType();
              break;
            case MailInputField.COLUMN_FOLDER_NAME:
              if (connection != null) {
                r[index] = connection.getFolderName();
              } else {
                r[index] = data.mailConn.getFolderName();
              }
              break;
            case MailInputField.COLUMN_SIZE:
              r[index] = (long) message.getSize();
              break;
            case MailInputField.COLUMN_FLAG_DRAFT:
              if (connection != null) {
                r[index] = connection.isMessageDraft(message);
              } else {
                r[index] = data.mailConn.isMessageDraft(message);
              }
              break;
            case MailInputField.COLUMN_FLAG_FLAGGED:
              if (connection != null) {
                r[index] = connection.isMessageFlagged(message);
              } else {
                r[index] = data.mailConn.isMessageFlagged(message);
              }
              break;
            case MailInputField.COLUMN_FLAG_NEW:
              if (connection != null) {
                r[index] = connection.isMessageNew(message);
              } else {
                r[index] = data.mailConn.isMessageNew(message);
              }
              break;
            case MailInputField.COLUMN_FLAG_READ:
              if (connection != null) {
                r[index] = connection.isMessageRead(message);
              } else {
                r[index] = data.mailConn.isMessageRead(message);
              }
              break;
            case MailInputField.COLUMN_FLAG_DELETED:
              if (connection != null) {
                r[index] = connection.isMessageDeleted(message);
              } else {
                r[index] = data.mailConn.isMessageDeleted(message);
              }
              break;
            case MailInputField.COLUMN_ATTACHED_FILES_COUNT:
              if (connection != null) {
                r[index] = (long) connection.getAttachedFilesCount(message, null);
              } else {
                r[index] = (long) data.mailConn.getAttachedFilesCount(message, null);
              }
              break;
            case MailInputField.COLUMN_HEADER:
              String name = meta.getInputFields().get(i).getName();
              // *only one name
              String[] arr = {name};
              // this code was before generic epoch
              Enumeration<?> en = message.getMatchingHeaders(arr);
              if (en == null) {
                r[index] = "";
                break;
              }
              List<String> headers = new ArrayList<>();
              while (en.hasMoreElements()) {
                Header next = Header.class.cast(en.nextElement());
                headers.add(next.getValue());
              }
              // if there is no matching headers return empty String
              r[index] = headers.isEmpty() ? "" : StringUtils.join(headers, ";");
              break;
            case MailInputField.COLUMN_BODY_CONTENT_TYPE:
              if (connection != null) {
                r[index] = connection.getMessageBodyContentType(message);
              } else {
                r[index] = data.mailConn.getMessageBodyContentType(message);
              }
              break;
            default:
              break;
          }
        } catch (Exception e) {
          String errMsg = "Error adding value for field " + meta.getInputFields().get(i).getName();
          throw new Exception(errMsg, e);
        }
      }
      return r;
    }
  }
}
