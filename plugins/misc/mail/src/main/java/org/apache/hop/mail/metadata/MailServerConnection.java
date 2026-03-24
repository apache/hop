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

package org.apache.hop.mail.metadata;

import com.google.common.annotations.VisibleForTesting;
import jakarta.mail.Flags;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.NoSuchProviderException;
import jakarta.mail.Part;
import jakarta.mail.Session;
import jakarta.mail.Store;
import jakarta.mail.Transport;
import jakarta.mail.URLName;
import jakarta.mail.internet.MimeUtility;
import jakarta.mail.search.AndTerm;
import jakarta.mail.search.BodyTerm;
import jakarta.mail.search.ComparisonTerm;
import jakarta.mail.search.FlagTerm;
import jakarta.mail.search.FromStringTerm;
import jakarta.mail.search.NotTerm;
import jakarta.mail.search.ReceivedDateTerm;
import jakarta.mail.search.RecipientStringTerm;
import jakarta.mail.search.SearchTerm;
import jakarta.mail.search.SubjectTerm;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mail.common.MailConst;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.eclipse.angus.mail.imap.IMAPSSLStore;
import org.eclipse.angus.mail.pop3.POP3SSLStore;

@Getter
@Setter
@HopMetadata(
    key = "MailServerConnection",
    name = "i18n::MailServerConnection.name",
    description = "i18n::MailServerConnection.description",
    image = "mail.svg",
    documentationUrl = "/metadata-types/mail-server-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.MAIL_SERVER_CONNECTION,
    supportsGlobalReplace = true)
public class MailServerConnection extends HopMetadataBase implements IHopMetadata {
  private static final Class<?> PKG = MailServerConnection.class;

  public static final int PROTOCOL_MBOX = 2;

  public static final int CONDITION_DATE_EQUAL = 1;
  public static final int CONDITION_DATE_SMALLER = 2;
  public static final int CONDITION_DATE_GREATER = 3;
  public static final int CONDITION_DATE_BETWEEN = 4;

  public static final int ACTION_TYPE_GET = 0;
  public static final int ACTION_TYPE_MOVE = 1;
  public static final int ACTION_TYPE_DELETE = 2;

  public static final int VALUE_IMAP_LIST_ALL = 0;
  public static final int VALUE_IMAP_LIST_NEW = 1;
  public static final int VALUE_IMAP_LIST_OLD = 2;
  public static final int VALUE_IMAP_LIST_READ = 3;
  public static final int VALUE_IMAP_LIST_UNREAD = 4;
  public static final int VALUE_IMAP_LIST_FLAGGED = 5;
  public static final int VALUE_IMAP_LIST_NOT_FLAGGED = 6;
  public static final int VALUE_IMAP_LIST_DRAFT = 7;
  public static final int VALUE_IMAP_LIST_NOT_DRAFT = 8;

  public static final int AFTER_GET_IMAP_DELETE = 1;
  public static final int AFTER_GET_IMAP_MOVE = 2;

  private Session session;
  private IVariables variables;
  private Properties props;
  private Store store;
  private Folder folder = null;

  /** Counts the number of message saved in a file */
  private int nrSavedMessages = 0;

  /** Counts the number of message move to a folder */
  private int nrMovedMessages = 0;

  /** Counts the number of message deleted */
  private int nrDeletedMessages = 0;

  /** Counts the number of attached files saved in a file */
  private int nrSavedAttachedFiles = 0;

  /** Counts the number of message fetched */
  private int messagenr = -1;

  /** Contains the list of retrieved messages */
  private Message[] messages;

  /** Contains the current message */
  private Message message;

  private SearchTerm searchTerm = null;

  @HopMetadataProperty private String protocol;

  @HopMetadataProperty private String serverHost;

  @HopMetadataProperty private String serverPort;

  @HopMetadataProperty private boolean useAuthentication;

  @HopMetadataProperty private String username;

  @HopMetadataProperty(password = true)
  private String password;

  @HopMetadataProperty private boolean useXOAuth2;

  @HopMetadataProperty private boolean useSecureAuthentication;

  @HopMetadataProperty private String secureConnectionType;

  @HopMetadataProperty private boolean useProxy;

  @HopMetadataProperty private String proxyHost;

  @HopMetadataProperty private String proxyUsername;

  @HopMetadataProperty private String trustedHosts;

  @HopMetadataProperty private boolean checkServerIdentity;

  /** IMAP folder if user want to move some messages */
  private Folder destinationIMAPFolder = null;

  public MailServerConnection() {
    super();
    props = new Properties();
  }

  public MailServerConnection(IVariables variables) {
    this();
    this.variables = variables;
  }

  public Session getSession(IVariables variables) {
    this.variables = variables;

    if (isSmtp()) {
      buildSmtpProps();
    } else {
      buildStoreProps();
    }

    applyCommonSecurity();
    applyTrustConfig();

    session = Session.getInstance(props);
    return session;
  }

  // SMTP
  public Transport getTransport() throws MessagingException {
    Transport transport = session.getTransport(protocol.toLowerCase());
    String authPass = getPassword(password);

    if (useAuthentication) {
      if (!Utils.isEmpty(serverPort)) {
        transport.connect(
            variables.resolve(Const.NVL(serverHost, "")),
            Integer.parseInt(variables.resolve(Const.NVL(serverPort, ""))),
            variables.resolve(Const.NVL(username, "")),
            authPass);
      } else {
        transport.connect(
            variables.resolve(Const.NVL(serverHost, "")),
            variables.resolve(Const.NVL(username, "")),
            authPass);
      }
    } else {
      transport.connect();
    }

    return transport;
  }

  // IMAP, POP, MBOX
  public Store getStore() throws MessagingException {
    if (useSecureAuthentication && !protocol.equals(MailConst.PROTOCOL_MBOX)) {
      URLName url =
          new URLName(
              protocol.toLowerCase(),
              variables.resolve(serverHost),
              Integer.parseInt(variables.resolve(serverPort)),
              "",
              variables.resolve(username),
              variables.resolve(password));

      switch (protocol) {
        case MailConst.PROTOCOL_STRING_POP3:
          store = new POP3SSLStore(session, url);
          break;
        case MailConst.PROTOCOL_STRING_IMAP:
          store = new IMAPSSLStore(session, url);
          break;
        default:
          break;
      }
    } else {
      if (protocol.equalsIgnoreCase(MailConst.PROTOCOL_MBOX)) {
        this.store = this.session.getStore(new URLName(MailConst.PROTOCOL_MSTOR + ":file:///"));
      } else {
        this.store = this.session.getStore(protocol.toLowerCase());
      }
    }

    return store;
  }

  @Override
  public String toString() {
    return name == null ? super.toString() : name;
  }

  @Override
  public int hashCode() {
    return name == null ? super.hashCode() : name.hashCode();
  }

  @Override
  public boolean equals(Object object) {

    if (object == this) {
      return true;
    }
    if (!(object instanceof MailServerConnection)) {
      return false;
    }

    MailServerConnection connection = (MailServerConnection) object;

    return name != null && name.equalsIgnoreCase(connection.name);
  }

  /**
   * Open the connection.
   *
   * @throws HopException if something went wrong.
   */
  public void connect() throws HopException, NoSuchProviderException {
    try {
      if (this.useSecureAuthentication || this.protocol.equals(MailConst.PROTOCOL_MBOX)) {
        // Supports IMAP/POP3 connection with SSL,
        // the connection is established via SSL.
        this.store.connect();
      } else {
        if (Integer.parseInt(variables.resolve(this.serverPort)) > -1) {
          this.store.connect(
              variables.resolve(this.serverHost),
              Integer.parseInt(variables.resolve(this.serverPort)),
              variables.resolve(this.username),
              variables.resolve(this.password));
        } else {
          this.store.connect(
              variables.resolve(this.serverHost),
              variables.resolve(this.username),
              variables.resolve(this.password));
        }
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "ActionGetMailsFromPOP.Error.Connecting",
              this.serverHost,
              this.username,
              Const.NVL(this.serverPort, "")),
          e);
    }
  }

  public void testConnection(Session session) {
    try {
      this.session = session;

      // mbox[mstor], Currently not supported.
      if (protocol.equalsIgnoreCase(MailConst.PROTOCOL_MBOX)) {
        return;
      }

      // send mail
      if (protocol.equalsIgnoreCase(MailConst.PROTOCOL_SMTP)) {
        getTransport();
        return;
      }

      // receive mail
      String host = variables.resolve(this.serverHost);
      int port = Integer.parseInt(variables.resolve(Const.NVL(this.serverPort, "0")));
      String user = variables.resolve(this.username);
      String pwd = variables.resolve(this.password);

      Store theStore = getStore();
      theStore.connect(host, port, user, pwd);
    } catch (MessagingException e) {
      throw new RuntimeException(e);
    }
  }

  public String getPassword(String authPassword) {
    return Encr.decryptPasswordOptionallyEncrypted(variables.resolve(Const.NVL(authPassword, "")));
  }

  /**
   * Set destination folder
   *
   * @param folderName destination folder name
   * @param createFolder flag create folder if needed
   */
  public void setDestinationFolder(String folderName, boolean createFolder) throws HopException {
    try {
      String[] folderParts = folderName.split("/");
      if (!store.isConnected()) {
        store.connect();
      }
      Folder f = store.getDefaultFolder();
      // Open destination folder
      for (String folderpart : folderParts) {
        f = f.getFolder(folderpart);
        if (!f.exists()) {
          if (createFolder) {
            // Create folder
            f.create(Folder.HOLDS_MESSAGES);
          } else {
            throw new HopException(
                BaseMessages.getString(PKG, "MailConnection.Error.FolderNotFound", folderName));
          }
        }
      }
      this.destinationIMAPFolder = f;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * Set filter on message sender.
   *
   * @param sender messages will be filtered on sender
   * @param notTerm negate condition
   */
  public void setSenderTerm(String sender, boolean notTerm) {
    if (!Utils.isEmpty(sender)) {
      if (notTerm) {
        addSearchTerm(new NotTerm(new FromStringTerm(sender)));
      } else {
        addSearchTerm(new FromStringTerm(sender));
      }
    }
  }

  /**
   * Add search term.
   *
   * @param term search term to add
   */
  private void addSearchTerm(SearchTerm term) {
    if (this.searchTerm != null) {
      this.searchTerm = new AndTerm(this.searchTerm, term);
    } else {
      this.searchTerm = term;
    }
  }

  /**
   * Set filter on recipient.
   *
   * @param recipient messages will be filtered on recipient
   */
  public void setRecipientTerm(String recipient) {
    if (!Utils.isEmpty(recipient)) {
      addSearchTerm(new RecipientStringTerm(Message.RecipientType.TO, recipient));
    }
  }

  /**
   * Set filter on subject.
   *
   * @param subject messages will be filtered on subject
   * @param notTerm negate condition
   */
  public void setSubjectTerm(String subject, boolean notTerm) {
    if (!Utils.isEmpty(subject)) {
      if (notTerm) {
        addSearchTerm(new NotTerm(new SubjectTerm(subject)));
      } else {
        addSearchTerm(new SubjectTerm(subject));
      }
    }
  }

  /**
   * Search all messages with body containing the word body filter
   *
   * @param bodyFilter bodyFilter
   * @param notTerm negate condition
   */
  public void setBodyTerm(String bodyFilter, boolean notTerm) {
    if (!Utils.isEmpty(bodyFilter)) {
      if (notTerm) {
        addSearchTerm(new NotTerm(new BodyTerm(bodyFilter)));
      } else {
        addSearchTerm(new BodyTerm(bodyFilter));
      }
    }
  }

  /**
   * Set filter on message received date.
   *
   * @param receivedDate messages will be filtered on received date
   */
  public void setReceivedDateTermEQ(Date receivedDate) {
    if (!this.protocol.equals(MailConst.PROTOCOL_STRING_POP3)) {
      addSearchTerm(new ReceivedDateTerm(ComparisonTerm.EQ, receivedDate));
    }
  }

  /**
   * Set filter on message received date.
   *
   * @param futureDate messages will be filtered on futureDate
   */
  public void setReceivedDateTermLT(Date futureDate) {
    if (!this.protocol.equals(MailConst.PROTOCOL_STRING_POP3)) {
      addSearchTerm(new ReceivedDateTerm(ComparisonTerm.LT, futureDate));
    }
  }

  /**
   * Set filter on message received date.
   *
   * @param pastDate messages will be filtered on pastDate
   */
  public void setReceivedDateTermGT(Date pastDate) {
    if (!this.protocol.equals(MailConst.PROTOCOL_STRING_POP3)) {
      addSearchTerm(new ReceivedDateTerm(ComparisonTerm.GT, pastDate));
    }
  }

  public void setReceivedDateTermBetween(Date beginDate, Date endDate) {
    if (!this.protocol.equals(MailConst.PROTOCOL_STRING_POP3)) {
      addSearchTerm(
          new AndTerm(
              new ReceivedDateTerm(ComparisonTerm.LT, endDate),
              new ReceivedDateTerm(ComparisonTerm.GT, beginDate)));
    }
  }

  public void setFlagTermNew() {
    addSearchTerm(new FlagTerm(new Flags(Flags.Flag.RECENT), true));
  }

  public void setFlagTermOld() {
    addSearchTerm(new FlagTerm(new Flags(Flags.Flag.RECENT), false));
  }

  public void setFlagTermRead() {
    addSearchTerm(new FlagTerm(new Flags(Flags.Flag.SEEN), true));
  }

  public void setFlagTermUnread() {
    addSearchTerm(new FlagTerm(new Flags(Flags.Flag.SEEN), false));
  }

  public void setFlagTermFlagged() {
    addSearchTerm(new FlagTerm(new Flags(Flags.Flag.FLAGGED), true));
  }

  public void setFlagTermNotFlagged() {
    addSearchTerm(new FlagTerm(new Flags(Flags.Flag.FLAGGED), false));
  }

  public void setFlagTermDraft() {
    addSearchTerm(new FlagTerm(new Flags(Flags.Flag.DRAFT), true));
  }

  public void setFlagTermNotDraft() {
    addSearchTerm(new FlagTerm(new Flags(Flags.Flag.DRAFT), false));
  }

  private HashSet<String> returnSubfolders(Folder folder) throws HopException {
    HashSet<String> list = new HashSet<>();
    try {
      if ((folder.getType() & Folder.HOLDS_FOLDERS) != 0) {
        Folder[] f = folder.list();
        for (Folder value : f) {
          // Search for sub folders
          if ((value.getType() & Folder.HOLDS_FOLDERS) != 0) {
            list.add(value.getFullName());
            list.addAll(returnSubfolders(value));
          }
        }
      }
    } catch (MessagingException m) {
      throw new HopException(m);
    }
    return list;
  }

  /**
   * Returns all subfolders of the specified folder
   *
   * @param folder parent folder
   * @return sub folders
   */
  public String[] returnAllFolders(Folder folder) throws HopException {
    HashSet<String> list = returnSubfolders(folder);
    return list.toArray(new String[0]);
  }

  /**
   * Returns all subfolders of the current folder
   *
   * @return sub folders
   */
  public String[] returnAllFolders() throws HopException {
    return returnAllFolders(getFolder());
  }

  /**
   * Returns all subfolders of the folder folder
   *
   * @param folder target folder
   * @return sub folders
   */
  public String[] returnAllFolders(String folder) throws HopException {
    Folder dfolder = null;
    String[] retval = null;
    try {
      if (Utils.isEmpty(folder)) {
        // Default folder
        dfolder = getStore().getDefaultFolder();
      } else {
        dfolder = getStore().getFolder(folder);
      }
      retval = returnAllFolders(dfolder);
    } catch (Exception e) {
      // Ignore errors
    } finally {
      try {
        if (dfolder != null) {
          dfolder.close(false);
        }
      } catch (Exception e) {
        /* Ignore */
      }
    }
    return retval;
  }

  public int getSavedAttachedFilesCounter() {
    return this.nrSavedAttachedFiles;
  }

  public void updateSavedAttachedFilesCounter() {
    this.nrSavedAttachedFiles++;
  }

  public int getSavedMessagesCounter() {
    return this.nrSavedMessages;
  }

  public int getDeletedMessagesCounter() {
    return this.nrDeletedMessages;
  }

  /**
   * Returns count of moved messages.
   *
   * @return count of moved messages
   */
  public int getMovedMessagesCounter() {
    return this.nrMovedMessages;
  }

  private void updateMessageNr() {
    this.messagenr++;
  }

  private int getMessageNr() {
    return this.messagenr;
  }

  private void updateDeletedMessagesCounter() {
    this.nrDeletedMessages++;
  }

  private void setDeletedMessagesCounter() {
    this.nrDeletedMessages = getMessagesCount();
  }

  /** Update count of moved messages. */
  private void updatedMovedMessagesCounter() {
    this.nrMovedMessages++;
  }

  /** Set count of moved messages. */
  private void setMovedMessagesCounter() {
    this.nrMovedMessages = getMessagesCount();
  }

  public void updateSavedMessagesCounter() {
    this.nrSavedMessages++;
  }

  /**
   * Disconnect from the server and close folder, connection.
   *
   * @param expunge expunge folder
   * @throws HopException ex
   */
  public void disconnect(boolean expunge) throws HopException {
    try {
      // close the folder, passing in a true value to expunge the deleted message
      closeFolder(expunge);
      clearFilters();
      if (this.store != null) {
        this.store.close();
        this.store = null;
      }
      if (this.session != null) {
        this.session = null;
      }
      if (this.destinationIMAPFolder != null) {
        this.destinationIMAPFolder.close(expunge);
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionGetMailsFromPOP.Error.ClosingConnection"), e);
    }
  }

  /**
   * Close folder.
   *
   * @param expunge expunge folder
   * @throws HopException ex
   */
  public void closeFolder(boolean expunge) throws HopException {
    try {
      if (this.folder != null && this.folder.isOpen()) {
        this.folder.close(expunge);
        this.folder = null;
        this.messages = null;
        this.message = null;
        this.messagenr = -1;
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionGetMailsFromPOP.Error.ClosingFolder", getFolderName()),
          e);
    }
  }

  /** Clear search terms. */
  public void clearFilters() {
    this.nrSavedMessages = 0;
    this.nrDeletedMessages = 0;
    this.nrMovedMessages = 0;
    this.nrSavedAttachedFiles = 0;
    if (this.searchTerm != null) {
      this.searchTerm = null;
    }
  }

  /**
   * Disconnect from the server and close folder, connection.
   *
   * @throws HopException ex
   */
  public void disconnect() throws HopException {
    disconnect(true);
  }

  /**
   * Returns the folder name.
   *
   * @return folder name
   */
  public String getFolderName() {
    if (this.folder == null) {
      return "";
    }
    return this.folder.getName();
  }

  /**
   * Open the default folder (INBOX)
   *
   * @param write open the folder in write mode
   * @throws HopException if something went wrong.
   */
  public void openFolder(boolean write) throws HopException {
    openFolder(null, true, write);
  }

  /**
   * Open the folder.
   *
   * @param folderName the name of the folder to open
   * @param write open the folder in write mode
   * @throws HopException if something went wrong.
   */
  public void openFolder(String folderName, boolean write) throws HopException {
    openFolder(folderName, false, write);
  }

  /**
   * Open the folder.
   *
   * @param folderName the name of the folder to open
   * @param defaultFolder true to open the default folder (INBOX)
   * @param write open the folder in write mode
   * @throws HopException if something went wrong.
   */
  public void openFolder(String folderName, boolean defaultFolder, boolean write)
      throws HopException {
    try {
      if (getFolder() != null) {
        // A folder is already opened
        // before make sure to close it
        closeFolder(true);
      }

      if (defaultFolder) {
        this.folder = getDefaultFolder(protocol, this.store);
      } else {
        // Open specified Folder (for IMAP/MBOX)
        if (this.protocol.equals(MailConst.PROTOCOL_STRING_IMAP)
            || this.protocol.equals(MailConst.PROTOCOL_MBOX)) {
          this.folder = getRecursiveFolder(folderName);
        }
        if (this.folder == null || !this.folder.exists()) {
          throw new HopException(
              BaseMessages.getString(PKG, "ActionGetMailsFromPOP.InvalidFolder.Label"));
        }
      }
      if (write) {
        this.folder.open(Folder.READ_WRITE);
      } else {
        this.folder.open(Folder.READ_ONLY);
      }

    } catch (Exception e) {
      throw new HopException(
          defaultFolder
              ? BaseMessages.getString(PKG, "ActionGetMailsFromPOP.Error.OpeningDefaultFolder")
              : BaseMessages.getString(
                  PKG, "ActionGetMailsFromPOP.Error.OpeningFolder", folderName),
          e);
    }
  }

  private Folder getDefaultFolder(String protocol, Store store)
      throws HopException, MessagingException {
    Folder defaultFolder;
    if (protocol.equals(MailConst.PROTOCOL_MBOX)) {
      defaultFolder = store.getDefaultFolder();
    } else {
      // get the default folder
      defaultFolder = getRecursiveFolder(MailConst.INBOX_FOLDER);
    }

    if (defaultFolder == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionGetMailsFromPOP.InvalidDefaultFolder.Label"));
    }

    if ((defaultFolder.getType() & Folder.HOLDS_MESSAGES) == 0) {
      throw new HopException(
          BaseMessages.getString(PKG, "MailConnection.DefaultFolderCanNotHoldMessage"));
    }
    return defaultFolder;
  }

  private Folder getRecursiveFolder(String folderName) throws MessagingException {
    Folder dfolder;
    String[] folderParts = folderName.split("/");
    if (!store.isConnected()) {
      store.connect();
    }
    dfolder = store.getDefaultFolder();
    // Open destination folder
    for (String folderPart : folderParts) {
      dfolder = dfolder.getFolder(folderPart);
    }
    return dfolder;
  }

  /**
   * Retrieve all messages from server
   *
   * @throws HopException ex
   */
  public void retrieveMessages() throws HopException {
    try {
      // search term?
      if (this.searchTerm != null) {
        this.messages = this.folder.search(this.searchTerm);
      } else {
        this.messages = this.folder.getMessages();
      }
    } catch (Exception e) {
      this.messages = null;
      throw new HopException(
          BaseMessages.getString(PKG, "MailConnection.Error.RetrieveMessages", getFolderName()), e);
    }
  }

  /**
   * Returns the number of messages.
   *
   * @return messages count
   */
  public int getMessagesCount() {
    return this.messages.length;
  }

  /**
   * Get next message.
   *
   * @throws HopException ex
   */
  public void fetchNext() throws HopException {
    updateMessageNr();
    try {
      this.message = this.messages[getMessageNr()];
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "MailConnection.Error.FetchingMessages"), e);
    }
  }

  /**
   * Delete current fetched message
   *
   * @throws HopException ex
   */
  public void deleteMessage() throws HopException {
    try {
      this.message.setFlag(Flags.Flag.DELETED, true);
      updateDeletedMessagesCounter();
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "MailConnection.Error.DeletingMessage", "" + getMessage().getMessageNumber()),
          e);
    }
  }

  /**
   * Delete messages.
   *
   * @throws HopException ex
   */
  public void deleteMessages(boolean setCounter) throws HopException {
    try {
      this.folder.setFlags(this.messages, new Flags(Flags.Flag.DELETED), true);
      if (setCounter) {
        setDeletedMessagesCounter();
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "MailConnection.Error.DeletingMessage"), e);
    }
  }

  /**
   * Move current message to a target folder. (IMAP) You must call setDestinationFolder before
   * calling this method
   *
   * @throws HopException ex
   */
  public void moveMessage() throws HopException {
    try {
      // move all messages
      this.folder.copyMessages(new Message[] {this.message}, this.destinationIMAPFolder);
      updatedMovedMessagesCounter();
      // Make sure to delete messages
      deleteMessage();
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "MailConnection.Error.MovingMessage",
              "" + getMessage().getMessageNumber(),
              this.destinationIMAPFolder.getName()),
          e);
    }
  }

  /**
   * Move messages to a folder. You must call setDestinationFolder before calling this method
   *
   * @throws HopException ex
   */
  public void moveMessages() throws HopException {
    try {
      this.folder.copyMessages(this.messages, this.destinationIMAPFolder);
      deleteMessages(false);
      setMovedMessagesCounter();
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "MailConnection.Error.MovingMessages", this.destinationIMAPFolder.getName()),
          e);
    }
  }

  /**
   * Export message content to a filename.
   *
   * @param filename the target filename
   * @param folderName the parent folder of filename
   * @throws HopException ex
   */
  public void saveMessageContentToFile(String filename, String folderName) throws HopException {
    OutputStream os = null;
    try {
      os =
          HopVfs.getOutputStream(
              folderName + (folderName.endsWith("/") ? "" : "/") + filename, false);
      getMessage().writeTo(os);
      updateSavedMessagesCounter();
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "MailConnection.Error.SavingMessageContent",
              "" + this.message.getMessageNumber(),
              filename,
              folderName),
          e);
    } finally {
      if (os != null) {
        IOUtils.closeQuietly(os);
      }
    }
  }

  /**
   * Save attached files to a folder.
   *
   * @param folderName the target folder name
   * @param pattern regular expression to filter on files
   * @throws HopException ex
   */
  public void saveAttachedFiles(String folderName, Pattern pattern) throws HopException {
    Object content = null;
    try {
      content = getMessage().getContent();
      if (content instanceof Multipart multipart) {
        handleMultipart(folderName, multipart, pattern);
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "MailConnection.Error.SavingAttachedFiles",
              "" + this.message.getMessageNumber(),
              folderName),
          e);
    } finally {
      if (content != null) {
        content = null;
      }
    }
  }

  private void handleMultipart(String folderName, Multipart multipart, Pattern pattern)
      throws HopException {
    try {
      for (int i = 0, n = multipart.getCount(); i < n; i++) {
        handlePart(folderName, multipart.getBodyPart(i), pattern);
      }
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  private void handlePart(String folderName, Part part, Pattern pattern) throws HopException {
    try {
      String disposition = part.getDisposition();

      // The RFC2183 doesn't REQUIRE Content-Disposition header field so we'll create one to
      // fake out the code below.
      if (Utils.isEmpty(disposition)) {
        disposition = Part.ATTACHMENT;
      }

      if (disposition.equalsIgnoreCase(Part.ATTACHMENT)
          || disposition.equalsIgnoreCase(Part.INLINE)) {
        String mimeText = decodeText(part);
        if (mimeText != null) {
          String filename = MimeUtility.decodeText(part.getFileName());
          if (isWildcardMatch(filename, pattern)) {
            // Save file
            saveFile(folderName, filename, part.getInputStream());
            updateSavedAttachedFilesCounter();
          }
        }
      }
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  private static void saveFile(String folderName, String filename, InputStream input)
      throws HopException {
    OutputStream fos = null;
    BufferedOutputStream bos = null;
    BufferedInputStream bis = null;
    try {
      // Do no overwrite existing file
      String targetFileName;
      if (filename == null) {
        File f = File.createTempFile("xx", ".out");
        f.deleteOnExit(); // Clean up file
        filename = f.getName();
        targetFileName =
            folderName + "/" + filename; // Note - createTempFile Used - so will be unique
      } else {
        targetFileName = findValidTarget(folderName, filename);
      }
      fos = HopVfs.getOutputStream(targetFileName, false);
      bos = new BufferedOutputStream(fos);
      bis = new BufferedInputStream(input);
      IOUtils.copy(bis, bos);
      bos.flush();
    } catch (Exception e) {
      throw new HopException(e);
    } finally {
      if (bis != null) {
        IOUtils.closeQuietly(bis);
        // Help the GC
        bis = null;
      }
      if (bos != null) {
        IOUtils.closeQuietly(bos);
        // Help the GC
        bos = null;
        // Note - closing the BufferedOuputStream closes the underlying output stream according to
        // the Javadoc
      }
    }
  }

  private boolean isWildcardMatch(String filename, Pattern pattern) {
    boolean retval = true;
    if (pattern != null) {
      Matcher matcher = pattern.matcher(filename);
      retval = (matcher.matches());
    }
    return retval;
  }

  @VisibleForTesting
  static String findValidTarget(String folderName, final String fileName) throws HopException {
    if (fileName == null || folderName == null) {
      throw new IllegalArgumentException("Cannot have null arguments to findValidTarget");
    }
    String fileNameRoot = FilenameUtils.getBaseName(fileName);
    // only a "."
    String ext = "." + FilenameUtils.getExtension(fileName);
    if ((ext.length() == 1)) {
      ext = "";
    }
    String rtn = "";
    String base = FilenameUtils.concat(folderName, fileNameRoot);
    int baseSz = base.length();
    StringBuilder build = new StringBuilder(baseSz).append(base);
    int i = -1;
    do {
      i++;
      // bring string back to size
      build.setLength(baseSz);
      build.append(i > 0 ? Integer.toString(i) : "").append(ext);
      rtn = build.toString();
    } while (HopVfs.fileExists(rtn));

    return rtn;
  }

  public String getMessageBody() throws Exception {
    return getMessageBody(getMessage());
  }

  /** Return the primary text content of the message. */
  public String getMessageBody(Message m) throws MessagingException, IOException {
    return getMessageBodyOrContentType(m, false);
  }

  public String getMessageBodyContentType(Message m) throws MessagingException, IOException {
    return getMessageBodyOrContentType(m, true);
  }

  private String getMessageBodyOrContentType(Part p, final boolean returnContentType)
      throws MessagingException, IOException {
    if (p.isMimeType("text/*")) {
      String s = (String) p.getContent();
      return returnContentType ? p.getContentType() : s;
    }

    if (p.isMimeType("multipart/alternative")) {
      return multipartAlternative(p, returnContentType);
    } else if (p.isMimeType("multipart/*")) {
      Multipart mp = (Multipart) p.getContent();
      for (int i = 0; i < mp.getCount(); i++) {
        String s = getMessageBodyOrContentType(mp.getBodyPart(i), returnContentType);
        if (s != null) {
          return s;
        }
      }
    }

    return null;
  }

  private String multipartAlternative(Part p, boolean returnContentType)
      throws IOException, MessagingException {
    // prefer html text over plain text
    Multipart mp = (Multipart) p.getContent();
    String text = null;
    for (int i = 0; i < mp.getCount(); i++) {
      Part bp = mp.getBodyPart(i);
      if (bp.isMimeType("text/plain") && text == null) {
        text = getMessageBodyOrContentType(bp, returnContentType);
      }
    }
    return text;
  }

  public boolean isMessageDraft(Message msg) {
    try {
      return msg.isSet(Flags.Flag.DRAFT);
    } catch (MessagingException e) {
      return false;
    }
  }

  /**
   * Returns if message is read
   *
   * @return true if message is flagged
   */
  public boolean isMessageFlagged() {
    return isMessageFlagged(getMessage());
  }

  public boolean isMessageFlagged(Message msg) {
    try {
      return msg.isSet(Flags.Flag.FLAGGED);
    } catch (MessagingException e) {
      return false;
    }
  }

  /**
   * Returns if message is new
   *
   * @return true if new message
   */
  public boolean isMessageNew() {
    return isMessageNew(getMessage());
  }

  public boolean isMessageNew(Message msg) {
    try {
      return msg.isSet(Flags.Flag.RECENT);
    } catch (MessagingException e) {
      return false;
    }
  }

  /**
   * Returns if message is read
   *
   * @return true if message is read
   */
  public boolean isMessageRead() {
    return isMessageRead(getMessage());
  }

  public boolean isMessageRead(Message msg) {
    try {
      return msg.isSet(Flags.Flag.SEEN);
    } catch (MessagingException e) {
      return false;
    }
  }

  /**
   * Returns if message is deleted
   *
   * @return true if message is deleted
   */
  public boolean isMessageDeleted() {
    return isMessageDeleted(getMessage());
  }

  public boolean isMessageDeleted(Message msg) {
    try {
      return msg.isSet(Flags.Flag.DELETED);
    } catch (MessagingException e) {
      return false;
    }
  }

  /**
   * Returns attached files count for the current message
   *
   * @param pattern (optional)
   * @return 1 if message is Draft
   */
  public int getAttachedFilesCount(Pattern pattern) throws HopException {
    return getAttachedFilesCount(getMessage(), pattern);
  }

  public int getAttachedFilesCount(Message message, Pattern pattern) throws HopException {
    Object content = null;
    int retval = 0;
    try {
      content = message.getContent();
      if (content instanceof Multipart multipart) {
        for (int i = 0, n = multipart.getCount(); i < n; i++) {
          Part part = multipart.getBodyPart(i);
          String disposition = part.getDisposition();

          if ((disposition != null)
              && (disposition.equalsIgnoreCase(Part.ATTACHMENT)
                  || disposition.equalsIgnoreCase(Part.INLINE))) {
            String mimeText = decodeText(part);
            if (mimeText != null) {
              String filename = MimeUtility.decodeText(part.getFileName());
              if (isWildcardMatch(filename, pattern)) {
                retval++;
              }
            }
          }
        }
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "MailConnection.Error.CountingAttachedFiles",
              "" + this.message.getMessageNumber()),
          e);
    } finally {
      if (content != null) {
        content = null;
      }
    }
    return retval;
  }

  /** builder smtp properties */
  private void buildSmtpProps() {
    String proto = protocol.toLowerCase();

    if (useSecureAuthentication) {
      applySmtpSecurity();
    }

    setHostPort(proto);
    if (useAuthentication) {
      props.put(mailKey(proto, "auth"), "true");
    }
  }

  /** builder smtp security */
  private void applySmtpSecurity() {
    if (useXOAuth2) {
      props.put("mail.smtp.auth.mechanisms", "XOAUTH2");
    }

    if (MailConst.SSL_TLS.equals(secureConnectionType)) {
      props.put("mail.smtp.starttls.enable", "true");
    } else if (MailConst.SSL_TLS_12.equals(secureConnectionType)) {
      props.put("mail.smtp.starttls.enable", "true");
      props.put("mail.smtp.ssl.protocols", MailConst.SSL_TLS_V12);
    } else {
      protocol = MailConst.PROTOCOL_SSL_SMTP;
      props.put("mail.smtps.quitwait", "false");
    }

    props.put("mail.smtp.ssl.checkServerIdentity", isCheckServerIdentity());
    if (!Utils.isEmpty(trustedHosts)) {
      props.put("mail.smtp.ssl.trust", variables.resolve(trustedHosts));
    }
  }

  /** builder store properties */
  private void buildStoreProps() {
    String proto = resolveProtocol(protocol);

    if (isUseProxy()) {
      props.put("mail.imap.sasl.enable", "true");
      props.put("mail.imap.sasl.authorizationid", proxyUsername);
    }

    handleSpecialProtocols();
    if (useSecureAuthentication && !isMbox()) {
      applyStoreSSL(proto);
    }
  }

  /** put store ssl */
  private void applyStoreSSL(String proto) {
    props.put(mailKey(proto, "socketFactory.class"), "javax.net.ssl.SSLSocketFactory");
    props.put(mailKey(proto, "socketFactory.fallback"), "false");
    props.put(mailKey(proto, "port"), variables.resolve(serverPort));
    props.put(mailKey(proto, "socketFactory.port"), variables.resolve(serverPort));

    if (useXOAuth2) {
      props.put(mailKey(proto, "ssl.enable"), "true");
      props.put(mailKey(proto, "auth.mechanisms"), "XOAUTH2");
    }
  }

  /** apply common security */
  private void applyCommonSecurity() {
    String[] protocols = {"imap", "imaps", "pop3", "pop3s"};

    for (String p : protocols) {
      props.put(
          MailConst.MAIL_PREFIX + p + ".ssl.checkServerIdentity",
          String.valueOf(isCheckServerIdentity()));
    }
  }

  /** apply trust config */
  private void applyTrustConfig() {
    if (Utils.isEmpty(trustedHosts)) {
      return;
    }

    String trusted = variables.resolve(trustedHosts);
    String[] protocols = {"imap", "imaps", "pop3", "pop3s"};

    for (String p : protocols) {
      props.put(MailConst.MAIL_PREFIX + p + ".ssl.trust", trusted);
    }
  }

  /** handler special protocol */
  private void handleSpecialProtocols() {
    if (MailConst.PROTOCOL_STRING_POP3.equals(protocol)) {
      props.put("mail.pop3.rsetbeforequit", "true");
      props.put("mail.pop3s.rsetbeforequit", "true");
    } else if (MailConst.PROTOCOL_MBOX.equals(protocol)) {
      props.put("mstor.mbox.metadataStrategy", "none");
      props.put("mstor.cache.disabled", "true");
    }
  }

  /** resolve protocol */
  private String resolveProtocol(String protocol) {
    if (MailConst.PROTOCOL_STRING_POP3.equals(protocol)) {
      return MailConst.PROTOCOL_STRING_POP3.toLowerCase();
    } else if (MailConst.PROTOCOL_MBOX.equals(protocol)) {
      return MailConst.PROTOCOL_MSTOR;
    } else {
      return MailConst.PROTOCOL_STRING_IMAP.toLowerCase();
    }
  }

  private void setHostPort(String proto) {
    props.put(mailKey(proto, "host"), variables.resolve(serverHost));

    if (!Utils.isEmpty(serverPort)) {
      props.put(mailKey(proto, "port"), variables.resolve(serverPort));
    }
  }

  private boolean isSmtp() {
    return MailConst.PROTOCOL_SMTP.equalsIgnoreCase(protocol);
  }

  private boolean isMbox() {
    return MailConst.PROTOCOL_MBOX.equalsIgnoreCase(protocol);
  }

  private String mailKey(String protocol, String key) {
    return MailConst.MAIL_PREFIX + protocol + "." + key;
  }

  private String decodeText(Part part) {
    try {
      return MimeUtility.decodeText(part.getFileName());
    } catch (Exception e) {
      // Ignore errors
    }
    return null;
  }
}
