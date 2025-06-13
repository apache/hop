/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.mail.workflow.actions.mail;

import jakarta.activation.DataHandler;
import jakarta.activation.FileDataSource;
import jakarta.activation.URLDataSource;
import jakarta.mail.Address;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.SendFailedException;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mail.metadata.MailServerConnection;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

/** Describes a Mail Workflow Entry. */
@Getter
@Setter
@Action(
    id = "MAIL",
    name = "i18n::ActionMail.Name",
    description = "i18n::ActionMail.Description",
    image = "mail.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Mail",
    keywords = "i18n::ActionMail.keyword",
    documentationUrl = "/workflow/actions/mail.html")
public class ActionMail extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionMail.class;

  public static final String CONST_FILETYPE = "filetype";
  public static final String CONST_MAIL = "mail.";
  public static final String CONST_SPACES = "      ";
  public static final String CONST_SPACES_LONG = "         ";
  public static final String CONST_SERVER = "server";
  public static final String CONST_DESTINATION = "destination";

  private MailServerConnection connection;
  private Session session;

  @HopMetadataProperty private String server;

  @HopMetadataProperty private String destination;

  @HopMetadataProperty private String destinationCc;

  @HopMetadataProperty private String destinationBCc;

  /** Caution : It's sender address and NOT reply address */
  @HopMetadataProperty(key = "replyto")
  private String replyAddress;

  /** Caution : It's sender name name and NOT reply name */
  @HopMetadataProperty(key = "replytoname")
  private String replyName;

  @HopMetadataProperty private String subject;

  @HopMetadataProperty(key = "include_date")
  private boolean includeDate;

  @HopMetadataProperty(key = "contact_persion")
  private String contactPerson;

  @HopMetadataProperty(key = "contact_phone")
  private String contactPhone;

  @HopMetadataProperty private String comment;

  @HopMetadataProperty(key = "include_files")
  private boolean includingFiles;

  @HopMetadataProperty(groupKey = "fileTypes", key = "fileType")
  private List<String> fileTypes;

  @HopMetadataProperty(key = "zip_files")
  private boolean zipFiles;

  @HopMetadataProperty(key = "zip_name")
  private String zipFilename;

  @HopMetadataProperty(key = "use_auth")
  private boolean usingAuthentication;

  @HopMetadataProperty private boolean usexoauth2;

  @HopMetadataProperty(key = "auth_user")
  private String authenticationUser;

  @HopMetadataProperty(key = "auth_password")
  private String authenticationPassword;

  @HopMetadataProperty(key = "only_comment")
  private boolean onlySendComment;

  @HopMetadataProperty(key = "use_HTML")
  private boolean useHTML;

  @HopMetadataProperty(key = "use_secure_auth")
  private boolean usingSecureAuthentication;

  @HopMetadataProperty(key = "use_Priority")
  private boolean usePriority;

  @HopMetadataProperty(key = "trusted_hosts")
  private String trustedHosts;

  @HopMetadataProperty(key = "check_server_identity")
  private boolean checkServerIdentity;

  @HopMetadataProperty private String port;

  @HopMetadataProperty private String priority;

  @HopMetadataProperty private String importance;

  @HopMetadataProperty private String sensitivity;

  @HopMetadataProperty(key = "secureconnectiontype")
  private String secureConnectionType;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty private String encoding;

  /** The reply to addresses */
  @HopMetadataProperty private String replyToAddresses;

  @HopMetadataProperty(key = "embeddedimage", groupKey = "embeddedimages")
  public List<MailEmbeddedImageField> embeddedimages;

  @HopMetadataProperty private String connectionName;

  public ActionMail(String n) {
    super(n, "");
  }

  public ActionMail() {
    this("");
  }

  @Override
  public Object clone() {
    ActionMail je = (ActionMail) super.clone();

    fileTypes = je.fileTypes;
    embeddedimages = je.embeddedimages;

    return je;
  }

  @Override
  public Result execute(Result result, int nr) {
    File masterZipfile = null;

    session = null;
    String protocol = "smtp";

    if (!StringUtils.isEmpty(connectionName)) {
      try {
        connection =
            getMetadataProvider().getSerializer(MailServerConnection.class).load(connectionName);
        session = connection.getSession(getVariables() /*, getLogChannel()*/);
      } catch (HopException e) {
        throw new RuntimeException(
            "Mail server connection '" + connectionName + "' could not be found", e);
      }
    } else {
      // Send an e-mail...
      // create some properties and get the default Session
      Properties props = new Properties();
      if (Utils.isEmpty(server)) {
        logError(BaseMessages.getString(PKG, "ActionMail.Error.HostNotSpecified"));

        result.setNrErrors(1L);
        result.setResult(false);
        return result;
      }

      if (usingSecureAuthentication) {
        if (usexoauth2) {
          props.put("mail.smtp.auth.mechanisms", "XOAUTH2");
        }
        if (secureConnectionType.equals("TLS")) {
          // Allow TLS authentication
          props.put("mail.smtp.starttls.enable", "true");
        } else if (secureConnectionType.equals("TLS 1.2")) {
          // Allow TLS 1.2 authentication
          props.put("mail.smtp.starttls.enable", "true");
          props.put("mail.smtp.ssl.protocols", "TLSv1.2");
        } else {

          protocol = "smtps";
          // required to get rid of a SSL exception :
          // nested exception is:
          // javax.net.ssl.SSLException: Unsupported record version Unknown
          props.put("mail.smtps.quitwait", "false");
        }
        props.put("mail.smtp.ssl.checkServerIdentity", isCheckServerIdentity());
        if (!Utils.isEmpty(trustedHosts)) {
          props.put("mail.smtp.ssl.trust", resolve(trustedHosts));
        }
      }

      props.put(CONST_MAIL + protocol + ".host", resolve(server));
      if (!Utils.isEmpty(port)) {
        props.put(CONST_MAIL + protocol + ".port", resolve(port));
      }

      if (isDebug()) {
        props.put("mail.debug", "true");
      }

      if (usingAuthentication) {
        props.put(CONST_MAIL + protocol + ".auth", "true");
      }

      session = Session.getInstance(props);
    }

    session.setDebug(isDebug());

    try {
      // create a message
      Message msg = new MimeMessage(session);

      // set message priority
      if (usePriority) {
        String priorityInt = "1";
        if (priority.equals("low")) {
          priorityInt = "3";
        }
        if (priority.equals("normal")) {
          priorityInt = "2";
        }

        msg.setHeader("X-Priority", priorityInt); // (String)int between 1= high and 3 = low.
        msg.setHeader("Importance", importance);
        // seems to be needed for MS Outlook.
        // where it returns a string of high /normal /low.
        msg.setHeader("Sensitivity", sensitivity);
        // Possible values are normal, personal, private, company-confidential

      }

      // Set Mail sender (From)
      String senderAddress = resolve(replyAddress);
      if (!Utils.isEmpty(senderAddress)) {
        String senderName = resolve(replyName);
        if (!Utils.isEmpty(senderName)) {
          senderAddress = senderName + '<' + senderAddress + '>';
        }
        msg.setFrom(new InternetAddress(senderAddress));
      } else {
        throw new MessagingException(
            BaseMessages.getString(PKG, "ActionMail.Error.ReplyEmailNotFilled"));
      }

      // set Reply to addresses
      String replyToAddress = resolve(replyToAddresses);
      if (!Utils.isEmpty(replyToAddress)) {
        // Split the mail-address: variables separated
        String[] replyAddressList = resolve(replyToAddress).split(" ");
        InternetAddress[] address = new InternetAddress[replyAddressList.length];
        for (int i = 0; i < replyAddressList.length; i++) {
          address[i] = new InternetAddress(replyAddressList[i]);
        }
        msg.setReplyTo(address);
      }

      // Split the mail-address: variables separated
      String[] destinations = resolve(destination).split(" ");
      InternetAddress[] address = new InternetAddress[destinations.length];
      for (int i = 0; i < destinations.length; i++) {
        address[i] = new InternetAddress(destinations[i]);
      }
      msg.setRecipients(Message.RecipientType.TO, address);

      String realCC = resolve(getDestinationCc());
      if (!Utils.isEmpty(realCC)) {
        // Split the mail-address Cc: variables separated
        String[] destinationsCc = realCC.split(" ");
        InternetAddress[] addressCc = new InternetAddress[destinationsCc.length];
        for (int i = 0; i < destinationsCc.length; i++) {
          addressCc[i] = new InternetAddress(destinationsCc[i]);
        }

        msg.setRecipients(Message.RecipientType.CC, addressCc);
      }

      String realBCc = resolve(getDestinationBCc());
      if (!Utils.isEmpty(realBCc)) {
        // Split the mail-address BCc: variables separated
        String[] destinationsBCc = realBCc.split(" ");
        InternetAddress[] addressBCc = new InternetAddress[destinationsBCc.length];
        for (int i = 0; i < destinationsBCc.length; i++) {
          addressBCc[i] = new InternetAddress(destinationsBCc[i]);
        }

        msg.setRecipients(Message.RecipientType.BCC, addressBCc);
      }
      String realSubject = resolve(subject);
      if (!Utils.isEmpty(realSubject)) {
        msg.setSubject(realSubject);
      }

      msg.setSentDate(new Date());
      StringBuilder messageText = new StringBuilder();
      String endRow = isUseHTML() ? "<br>" : Const.CR;
      String realComment = resolve(comment);
      if (!Utils.isEmpty(realComment)) {
        messageText.append(realComment).append(Const.CR).append(Const.CR);
      }
      if (!onlySendComment) {

        messageText
            .append(BaseMessages.getString(PKG, "ActionMail.Log.Comment.Workflow"))
            .append(endRow);
        messageText.append("-----").append(endRow);
        messageText
            .append(BaseMessages.getString(PKG, "ActionMail.Log.Comment.JobName") + "    : ")
            .append(parentWorkflow.getWorkflowMeta().getName())
            .append(endRow);
        messageText
            .append(BaseMessages.getString(PKG, "ActionMail.Log.Comment.Action") + "   : ")
            .append(getName())
            .append(endRow);
        messageText.append(Const.CR);
      }

      if (includeDate) {
        messageText
            .append(endRow)
            .append(BaseMessages.getString(PKG, "ActionMail.Log.Comment.MsgDate") + ": ")
            .append(XmlHandler.date2string(new Date()))
            .append(endRow)
            .append(endRow);
      }
      if (!onlySendComment && result != null) {
        messageText
            .append(BaseMessages.getString(PKG, "ActionMail.Log.Comment.PreviousResult") + ":")
            .append(endRow);
        messageText.append("-----------------").append(endRow);
        messageText
            .append(BaseMessages.getString(PKG, "ActionMail.Log.Comment.ActionNr") + "         : ")
            .append(result.getEntryNr())
            .append(endRow);
        messageText
            .append(
                BaseMessages.getString(PKG, "ActionMail.Log.Comment.Errors") + "               : ")
            .append(result.getNrErrors())
            .append(endRow);
        messageText
            .append(
                BaseMessages.getString(PKG, "ActionMail.Log.Comment.LinesRead") + "           : ")
            .append(result.getNrLinesRead())
            .append(endRow);
        messageText
            .append(
                BaseMessages.getString(PKG, "ActionMail.Log.Comment.LinesWritten") + "        : ")
            .append(result.getNrLinesWritten())
            .append(endRow);
        messageText
            .append(
                BaseMessages.getString(PKG, "ActionMail.Log.Comment.LinesInput") + "          : ")
            .append(result.getNrLinesInput())
            .append(endRow);
        messageText
            .append(
                BaseMessages.getString(PKG, "ActionMail.Log.Comment.LinesOutput") + "         : ")
            .append(result.getNrLinesOutput())
            .append(endRow);
        messageText
            .append(
                BaseMessages.getString(PKG, "ActionMail.Log.Comment.LinesUpdated") + "        : ")
            .append(result.getNrLinesUpdated())
            .append(endRow);
        messageText
            .append(
                BaseMessages.getString(PKG, "ActionMail.Log.Comment.LinesRejected") + "       : ")
            .append(result.getNrLinesRejected())
            .append(endRow);
        messageText
            .append(BaseMessages.getString(PKG, "ActionMail.Log.Comment.Status") + "  : ")
            .append(result.getExitStatus())
            .append(endRow);
        messageText
            .append(
                BaseMessages.getString(PKG, "ActionMail.Log.Comment.Result") + "               : ")
            .append(result.getResult())
            .append(endRow);
        messageText.append(endRow);
      }

      if (!onlySendComment
          && (!Utils.isEmpty(resolve(contactPerson)) || !Utils.isEmpty(resolve(contactPhone)))) {
        messageText
            .append(BaseMessages.getString(PKG, "ActionMail.Log.Comment.ContactInfo") + " :")
            .append(endRow);
        messageText.append("---------------------").append(endRow);
        messageText
            .append(BaseMessages.getString(PKG, "ActionMail.Log.Comment.PersonToContact") + " : ")
            .append(resolve(contactPerson))
            .append(endRow);
        messageText
            .append(BaseMessages.getString(PKG, "ActionMail.Log.Comment.Tel") + "  : ")
            .append(resolve(contactPhone))
            .append(endRow);
        messageText.append(endRow);
      }

      // Include the path to this action...
      if (!onlySendComment) {
        WorkflowTracker workflowTracker = parentWorkflow.getWorkflowTracker();
        if (workflowTracker != null) {
          messageText
              .append(BaseMessages.getString(PKG, "ActionMail.Log.Comment.PathToJobentry") + ":")
              .append(endRow);
          messageText.append("------------------------").append(endRow);

          addBacktracking(workflowTracker, messageText);
          if (isUseHTML()) {
            messageText.replace(
                0, messageText.length(), messageText.toString().replace(Const.CR, endRow));
          }
        }
      }

      MimeMultipart parts = new MimeMultipart();
      MimeBodyPart part1 = new MimeBodyPart(); // put the text in the
      // Attached files counter
      int nrattachedFiles = 0;

      // 1st part

      if (useHTML) {
        if (!Utils.isEmpty(getEncoding())) {
          part1.setContent(messageText.toString(), "text/html; " + "charset=" + getEncoding());
        } else {
          part1.setContent(messageText.toString(), "text/html; " + "charset=ISO-8859-1");
        }
      } else {
        part1.setText(messageText.toString());
      }

      parts.addBodyPart(part1);

      if (includingFiles && result != null) {
        List<ResultFile> resultFiles = result.getResultFilesList();
        if (resultFiles != null && !resultFiles.isEmpty()) {
          if (!zipFiles) {
            // Add all files to the message...
            //
            for (ResultFile resultFile : resultFiles) {
              FileObject file = resultFile.getFile();
              if (file != null && file.exists()) {
                boolean found = false;
                for (String fileTypeField : fileTypes) {
                  if (fileTypeField.equals(resultFile.getTypeDesc())) {
                    found = true;
                  }
                }
                if (found) {
                  // create a data source
                  MimeBodyPart files = new MimeBodyPart();
                  URLDataSource fds = new URLDataSource(file.getURL());

                  // get a data IHandler to manipulate this file type
                  files.setDataHandler(new DataHandler(fds));
                  // include the file in the data source
                  files.setFileName(file.getName().getBaseName());

                  // insist on base64 to preserve line endings
                  files.addHeader("Content-Transfer-Encoding", "base64");

                  // add the part with the file in the BodyPart()
                  parts.addBodyPart(files);
                  nrattachedFiles++;
                  logBasic("Added file '" + fds.getName() + "' to the mail message.");
                }
              }
            }
          } else {
            // create a single ZIP archive of all files
            masterZipfile =
                new File(
                    System.getProperty("java.io.tmpdir")
                        + Const.FILE_SEPARATOR
                        + resolve(zipFilename));
            ZipOutputStream zipOutputStream = null;
            try {
              zipOutputStream = new ZipOutputStream(new FileOutputStream(masterZipfile));

              for (ResultFile resultFile : resultFiles) {
                boolean found = false;
                for (int i = 0; i < fileTypes.size(); i++) {
                  if (fileTypes.get(i).equals(resultFile.getTypeDesc())) {
                    found = true;
                  }
                }
                if (found) {
                  FileObject file = resultFile.getFile();
                  ZipEntry zipEntry = new ZipEntry(file.getName().getBaseName());
                  zipOutputStream.putNextEntry(zipEntry);

                  // Now put the content of this file into this archive...
                  BufferedInputStream inputStream =
                      new BufferedInputStream(HopVfs.getInputStream(file));
                  try {
                    int c;
                    while ((c = inputStream.read()) >= 0) {
                      zipOutputStream.write(c);
                    }
                  } finally {
                    inputStream.close();
                  }
                  zipOutputStream.closeEntry();
                  nrattachedFiles++;
                  logBasic(
                      "Added file '"
                          + file.getName().getURI()
                          + "' to the mail message in a zip archive.");
                }
              }
            } catch (Exception e) {
              logError(
                  "Error zipping attachement files into file ["
                      + masterZipfile.getPath()
                      + "] : "
                      + e.toString());
              logError(Const.getStackTracker(e));
              result.setNrErrors(1);
            } finally {
              if (zipOutputStream != null) {
                try {
                  zipOutputStream.finish();
                  zipOutputStream.close();
                } catch (IOException e) {
                  logError("Unable to close attachement zip file archive : " + e.toString());
                  logError(Const.getStackTracker(e));
                  result.setNrErrors(1);
                }
              }
            }

            // Now attach the master zip file to the message.
            if (result.getNrErrors() == 0) {
              // create a data source
              MimeBodyPart files = new MimeBodyPart();
              FileDataSource fds = new FileDataSource(masterZipfile);
              // get a data IHandler to manipulate this file type
              files.setDataHandler(new DataHandler(fds));
              // include the file in the data source
              files.setFileName(fds.getName());
              // add the part with the file in the BodyPart()
              parts.addBodyPart(files);
            }
          }
        }
      }

      int nrEmbeddedImages = 0;
      if (embeddedimages != null && embeddedimages.size() > 0) {
        FileObject imageFile = null;
        for (int i = 0; i < embeddedimages.size(); i++) {
          String realImageFile = resolve(embeddedimages.get(i).getEmbeddedimage());
          String realcontenID = resolve(embeddedimages.get(i).getContentId());
          if (messageText.indexOf("cid:" + realcontenID) < 0) {
            if (isDebug()) {
              logDebug("Image [" + realImageFile + "] is not used in message body!");
            }
          } else {
            try {
              boolean found = false;
              imageFile = HopVfs.getFileObject(realImageFile);
              if (imageFile.exists() && imageFile.getType() == FileType.FILE) {
                found = true;
              } else {
                logError("We can not find [" + realImageFile + "] or it is not a file");
              }
              if (found) {
                // Create part for the image
                MimeBodyPart messageBodyPart = new MimeBodyPart();
                // Load the image
                URLDataSource fds = new URLDataSource(imageFile.getURL());
                messageBodyPart.setDataHandler(new DataHandler(fds));
                // Setting the header
                messageBodyPart.setHeader("Content-ID", "<" + realcontenID + ">");
                // Add part to multi-part
                parts.addBodyPart(messageBodyPart);
                nrEmbeddedImages++;
                logBasic("Image '" + fds.getName() + "' was embedded in message.");
              }
            } catch (Exception e) {
              logError(
                  "Error embedding image [" + realImageFile + "] in message : " + e.toString());
              logError(Const.getStackTracker(e));
              result.setNrErrors(1);
            } finally {
              if (imageFile != null) {
                try {
                  imageFile.close();
                } catch (Exception e) {
                  /* Ignore */
                }
              }
            }
          }
        }
      }

      if (nrEmbeddedImages > 0 && nrattachedFiles == 0) {
        // If we need to embedd images...
        // We need to create a "multipart/related" message.
        // otherwise image will appear as attached file
        parts.setSubType("related");
      }
      // put all parts together
      msg.setContent(parts);

      Transport transport = null;
      try {
        if (!StringUtils.isEmpty(connectionName)) {
          transport = connection.getTransport();
        } else {
          transport = session.getTransport(protocol);
          String authPass = getPassword(authenticationPassword);

          if (usingAuthentication) {
            if (!Utils.isEmpty(port)) {
              transport.connect(
                  resolve(Const.NVL(server, "")),
                  Integer.parseInt(resolve(Const.NVL(port, ""))),
                  resolve(Const.NVL(authenticationUser, "")),
                  authPass);
            } else {
              transport.connect(
                  resolve(Const.NVL(server, "")),
                  resolve(Const.NVL(authenticationUser, "")),
                  authPass);
            }
          } else {
            transport.connect();
          }
        }
        transport.sendMessage(msg, msg.getAllRecipients());
      } finally {
        if (transport != null) {
          transport.close();
        }
      }
    } catch (IOException e) {
      logError("Problem while sending message: " + e.toString());
      result.setNrErrors(1);
    } catch (MessagingException mex) {
      logError("Problem while sending message: " + mex.toString());
      result.setNrErrors(1);

      Exception ex = mex;
      do {
        if (ex instanceof SendFailedException sfex) {
          Address[] invalid = sfex.getInvalidAddresses();
          if (invalid != null) {
            logError("    ** Invalid Addresses");
            for (int i = 0; i < invalid.length; i++) {
              logError(CONST_SPACES_LONG + invalid[i]);
              result.setNrErrors(1);
            }
          }

          Address[] validUnsent = sfex.getValidUnsentAddresses();
          if (validUnsent != null) {
            logError("    ** ValidUnsent Addresses");
            for (int i = 0; i < validUnsent.length; i++) {
              logError(CONST_SPACES_LONG + validUnsent[i]);
              result.setNrErrors(1);
            }
          }

          Address[] validSent = sfex.getValidSentAddresses();
          if (validSent != null) {
            for (int i = 0; i < validSent.length; i++) {
              logError(CONST_SPACES_LONG + validSent[i]);
              result.setNrErrors(1);
            }
          }
        }
        if (ex instanceof MessagingException messagingException) {
          ex = messagingException.getNextException();
        } else {
          ex = null;
        }
      } while (ex != null);
    } finally {
      if (masterZipfile != null && masterZipfile.exists()) {
        masterZipfile.delete();
      }
    }

    if (result.getNrErrors() > 0) {
      result.setResult(false);
    } else {
      result.setResult(true);
    }

    return result;
  }

  private void addBacktracking(WorkflowTracker workflowTracker, StringBuilder messageText) {
    addBacktracking(workflowTracker, messageText, 0);
  }

  private void addBacktracking(
      WorkflowTracker workflowTracker, StringBuilder messageText, int level) {
    int nr = workflowTracker.nrWorkflowTrackers();

    messageText.append(Const.rightPad(" ", level * 2));
    messageText.append(Const.NVL(workflowTracker.getWorkflowName(), "-"));
    ActionResult jer = workflowTracker.getActionResult();
    if (jer != null) {
      messageText.append(" : ");
      if (jer.getActionName() != null) {
        messageText.append(" : ");
        messageText.append(jer.getActionName());
      }
      if (jer.getResult() != null) {
        messageText.append(" : ");
        messageText.append("[" + jer.getResult().toString() + "]");
      }
      if (jer.getReason() != null) {
        messageText.append(" : ");
        messageText.append(jer.getReason());
      }
      if (jer.getComment() != null) {
        messageText.append(" : ");
        messageText.append(jer.getComment());
      }
      if (jer.getLogDate() != null) {
        messageText.append(" (");
        messageText.append(XmlHandler.date2string(jer.getLogDate()));
        messageText.append(')');
      }
    }
    messageText.append(Const.CR);

    for (int i = 0; i < nr; i++) {
      WorkflowTracker jt = workflowTracker.getWorkflowTracker(i);
      addBacktracking(jt, messageText, level + 1);
    }
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return true;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    String realServername = resolve(server);
    ResourceReference reference = new ResourceReference(this);
    reference.getEntries().add(new ResourceEntry(realServername, ResourceType.SERVER));
    references.add(reference);
    return references;
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
            CONST_SERVER,
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "replyAddress",
            remarks,
            AndValidator.putValidators(
                ActionValidatorUtils.notBlankValidator(), ActionValidatorUtils.emailValidator()));

    ActionValidatorUtils.andValidator()
        .validate(
            this,
            CONST_DESTINATION,
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));

    if (usingAuthentication) {
      ActionValidatorUtils.andValidator()
          .validate(
              this,
              "authenticationUser",
              remarks,
              AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
      ActionValidatorUtils.andValidator()
          .validate(
              this,
              "authenticationPassword",
              remarks,
              AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));
    }

    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "port",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.integerValidator()));
  }

  public String getPassword(String authPassword) {
    return Encr.decryptPasswordOptionallyEncrypted(resolve(Const.NVL(authPassword, "")));
  }
}
