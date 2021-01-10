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

package org.apache.hop.pipeline.transforms.mail;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
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

/**
 * Send mail transform. based on Mail action
 *
 * @author Samatar
 * @since 28-07-2008
 */
@Transform(
    id = "Mail",
    image = "mail.svg",
    name = "i18n::BaseTransform.TypeLongDesc.Mail",
    description = "i18n::BaseTransform.TypeTooltipDesc.Mail",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/mail.html")
public class MailMeta extends BaseTransformMeta implements ITransformMeta<Mail, MailData> {
  private static final Class<?> PKG = MailMeta.class; // For Translator

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

  /** Flag : attach file from content defined in a field */
  private boolean attachContentFromField;

  /** file content field name */
  private String attachContentField;

  /** filename content field */
  private String attachContentFileNameField;

  public MailMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      MailData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Mail(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  public void allocate(int value) {
    this.embeddedimages = new String[value];
    this.contentids = new String[value];
  }

  private void readData(Node transformNode) {
    setServer(XmlHandler.getTagValue(transformNode, "server"));
    setPort(XmlHandler.getTagValue(transformNode, "port"));
    setDestination(XmlHandler.getTagValue(transformNode, "destination"));
    setDestinationCc(XmlHandler.getTagValue(transformNode, "destinationCc"));
    setDestinationBCc(XmlHandler.getTagValue(transformNode, "destinationBCc"));
    setReplyToAddresses(XmlHandler.getTagValue(transformNode, "replyToAddresses"));
    setReplyAddress(XmlHandler.getTagValue(transformNode, "replyto"));
    setReplyName(XmlHandler.getTagValue(transformNode, "replytoname"));
    setSubject(XmlHandler.getTagValue(transformNode, "subject"));
    setIncludeDate("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include_date")));
    setIncludeSubFolders(
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include_subfolders")));
    setZipFilenameDynamic(
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "zipFilenameDynamic")));
    setisDynamicFilename(
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "isFilenameDynamic")));
    setDynamicFieldname(XmlHandler.getTagValue(transformNode, "dynamicFieldname"));
    setDynamicWildcard(XmlHandler.getTagValue(transformNode, "dynamicWildcard"));
    setAttachContentFromField(
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "attachContentFromField")));
    setAttachContentField(XmlHandler.getTagValue(transformNode, "attachContentField"));
    setAttachContentFileNameField(
        XmlHandler.getTagValue(transformNode, "attachContentFileNameField"));

    setDynamicZipFilenameField(XmlHandler.getTagValue(transformNode, "dynamicZipFilename"));
    setSourceFileFoldername(XmlHandler.getTagValue(transformNode, "sourcefilefoldername"));
    setSourceWildcard(XmlHandler.getTagValue(transformNode, "sourcewildcard"));
    setContactPerson(XmlHandler.getTagValue(transformNode, "contact_person"));
    setContactPhone(XmlHandler.getTagValue(transformNode, "contact_phone"));
    setComment(XmlHandler.getTagValue(transformNode, "comment"));
    setIncludingFiles("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include_files")));
    setUsingAuthentication("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "use_auth")));
    setUsingSecureAuthentication(
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "use_secure_auth")));
    setAuthenticationUser(XmlHandler.getTagValue(transformNode, "auth_user"));
    setAuthenticationPassword(
        Encr.decryptPasswordOptionallyEncrypted(
            XmlHandler.getTagValue(transformNode, "auth_password")));
    setOnlySendComment("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "only_comment")));
    setUseHTML("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "use_HTML")));
    setUsePriority("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "use_Priority")));
    setEncoding(XmlHandler.getTagValue(transformNode, "encoding"));
    setPriority(XmlHandler.getTagValue(transformNode, "priority"));
    setImportance(XmlHandler.getTagValue(transformNode, "importance"));
    setSensitivity(XmlHandler.getTagValue(transformNode, "sensitivity"));
    setSecureConnectionType(XmlHandler.getTagValue(transformNode, "secureconnectiontype"));
    setZipFiles("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "zip_files")));
    setZipFilename(XmlHandler.getTagValue(transformNode, "zip_name"));
    setZipLimitSize(XmlHandler.getTagValue(transformNode, "zip_limit_size"));

    Node images = XmlHandler.getSubNode(transformNode, "embeddedimages");
    // How many field embedded images ?
    int nrImages = XmlHandler.countNodes(images, "embeddedimage");

    allocate(nrImages);

    // Read them all...
    for (int i = 0; i < nrImages; i++) {
      Node fnode = XmlHandler.getSubNodeByNr(images, "embeddedimage", i);

      embeddedimages[i] = XmlHandler.getTagValue(fnode, "image_name");
      contentids[i] = XmlHandler.getTagValue(fnode, "content_id");
    }
  }

  public void setEmbeddedImage(int i, String value) {
    embeddedimages[i] = value;
  }

  public void setEmbeddedImages(String[] value) {
    this.embeddedimages = value;
  }

  public void setContentIds(int i, String value) {
    contentids[i] = value;
  }

  public void setContentIds(String[] value) {
    this.contentids = value;
  }

  @Override
  public void setDefault() {}

  @Override
  public String getXml() throws HopException {
    StringBuilder retval = new StringBuilder(300);

    retval.append(super.getXml());

    retval.append("      ").append(XmlHandler.addTagValue("server", this.server));
    retval.append("      ").append(XmlHandler.addTagValue("port", this.port));
    retval.append("      ").append(XmlHandler.addTagValue("destination", this.destination));
    retval.append("      ").append(XmlHandler.addTagValue("destinationCc", this.destinationCc));
    retval.append("      ").append(XmlHandler.addTagValue("destinationBCc", this.destinationBCc));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("replyToAddresses", this.replyToAddresses));
    retval.append("      ").append(XmlHandler.addTagValue("replyto", this.replyAddress));
    retval.append("      ").append(XmlHandler.addTagValue("replytoname", this.replyName));
    retval.append("      ").append(XmlHandler.addTagValue("subject", this.subject));
    retval.append("      ").append(XmlHandler.addTagValue("include_date", this.includeDate));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("include_subfolders", this.includeSubFolders));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("zipFilenameDynamic", this.zipFilenameDynamic));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("isFilenameDynamic", this.isFilenameDynamic));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("attachContentFromField", this.attachContentFromField));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("attachContentField", this.attachContentField));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue("attachContentFileNameField", this.attachContentFileNameField));

    retval
        .append("      ")
        .append(XmlHandler.addTagValue("dynamicFieldname", this.dynamicFieldname));
    retval.append("      ").append(XmlHandler.addTagValue("dynamicWildcard", this.dynamicWildcard));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("dynamicZipFilename", this.dynamicZipFilename));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("sourcefilefoldername", this.sourcefilefoldername));
    retval.append("      ").append(XmlHandler.addTagValue("sourcewildcard", this.sourcewildcard));
    retval.append("      ").append(XmlHandler.addTagValue("contact_person", this.contactPerson));
    retval.append("      ").append(XmlHandler.addTagValue("contact_phone", this.contactPhone));
    retval.append("      ").append(XmlHandler.addTagValue("comment", this.comment));
    retval.append("      ").append(XmlHandler.addTagValue("include_files", this.includingFiles));
    retval.append("      ").append(XmlHandler.addTagValue("zip_files", this.zipFiles));
    retval.append("      ").append(XmlHandler.addTagValue("zip_name", this.zipFilename));
    retval.append("      ").append(XmlHandler.addTagValue("zip_limit_size", this.ziplimitsize));
    retval.append("      ").append(XmlHandler.addTagValue("use_auth", this.usingAuthentication));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("use_secure_auth", this.usingSecureAuthentication));
    retval.append("      ").append(XmlHandler.addTagValue("auth_user", this.authenticationUser));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "auth_password",
                Encr.encryptPasswordIfNotUsingVariables(this.authenticationPassword)));
    retval.append("      ").append(XmlHandler.addTagValue("only_comment", this.onlySendComment));
    retval.append("      ").append(XmlHandler.addTagValue("use_HTML", this.useHTML));
    retval.append("      ").append(XmlHandler.addTagValue("use_Priority", this.usePriority));
    retval.append("    " + XmlHandler.addTagValue("encoding", this.encoding));
    retval.append("    " + XmlHandler.addTagValue("priority", this.priority));
    retval.append("    " + XmlHandler.addTagValue("importance", this.importance));
    retval.append("    " + XmlHandler.addTagValue("sensitivity", this.sensitivity));
    retval.append(
        "    " + XmlHandler.addTagValue("secureconnectiontype", this.secureConnectionType));

    retval.append("      <embeddedimages>").append(Const.CR);
    if (embeddedimages != null) {
      for (int i = 0; i < embeddedimages.length; i++) {
        retval.append("        <embeddedimage>").append(Const.CR);
        retval.append("          ").append(XmlHandler.addTagValue("image_name", embeddedimages[i]));
        retval.append("          ").append(XmlHandler.addTagValue("content_id", contentids[i]));
        retval.append("        </embeddedimage>").append(Const.CR);
      }
    }
    retval.append("      </embeddedimages>").append(Const.CR);

    return retval.toString();
  }

  public void setServer(String s) {
    this.server = s;
  }

  public String getServer() {
    return this.server;
  }

  public void setDestination(String dest) {
    this.destination = dest;
  }

  public void setDestinationCc(String destCc) {
    this.destinationCc = destCc;
  }

  public void setDestinationBCc(String destBCc) {
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

  public void setReplyAddress(String reply) {
    this.replyAddress = reply;
  }

  public String getReplyAddress() {
    return this.replyAddress;
  }

  public void setReplyName(String replyname) {
    this.replyName = replyname;
  }

  public String getReplyName() {
    return this.replyName;
  }

  public void setSubject(String subj) {
    this.subject = subj;
  }

  public String getSubject() {
    return this.subject;
  }

  public void setIncludeDate(boolean incl) {
    this.includeDate = incl;
  }

  public void setIncludeSubFolders(boolean incl) {
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

  public void setZipFilenameDynamic(boolean isdynamic) {
    this.zipFilenameDynamic = isdynamic;
  }

  public void setisDynamicFilename(boolean isdynamic) {
    this.isFilenameDynamic = isdynamic;
  }

  public void setAttachContentFromField(boolean attachContentFromField) {
    this.attachContentFromField = attachContentFromField;
  }

  public void setAttachContentField(String attachContentField) {
    this.attachContentField = attachContentField;
  }

  public void setAttachContentFileNameField(String attachContentFileNameField) {
    this.attachContentFileNameField = attachContentFileNameField;
  }

  public void setDynamicWildcard(String dynamicwildcard) {
    this.dynamicWildcard = dynamicwildcard;
  }

  public void setDynamicZipFilenameField(String dynamiczipfilename) {
    this.dynamicZipFilename = dynamiczipfilename;
  }

  public String getDynamicZipFilenameField() {
    return this.dynamicZipFilename;
  }

  public String getDynamicWildcard() {
    return this.dynamicWildcard;
  }

  public void setSourceFileFoldername(String sourcefile) {
    this.sourcefilefoldername = sourcefile;
  }

  public String getSourceFileFoldername() {
    return this.sourcefilefoldername;
  }

  public void setSourceWildcard(String wildcard) {
    this.sourcewildcard = wildcard;
  }

  public String getSourceWildcard() {
    return this.sourcewildcard;
  }

  public void setDynamicFieldname(String dynamicfield) {
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

  public void setContactPerson(String person) {
    this.contactPerson = person;
  }

  public String getContactPerson() {
    return this.contactPerson;
  }

  public void setContactPhone(String phone) {
    this.contactPhone = phone;
  }

  public String getContactPhone() {
    return this.contactPhone;
  }

  public void setComment(String comm) {
    this.comment = comm;
  }

  public String getComment() {
    return this.comment;
  }

  public boolean isIncludingFiles() {
    return this.includingFiles;
  }

  public void setIncludingFiles(boolean includeFiles) {
    this.includingFiles = includeFiles;
  }

  /** @return Returns the zipFilename. */
  public String getZipFilename() {
    return this.zipFilename;
  }

  /** @return Returns the ziplimitsize. */
  public String getZipLimitSize() {
    return this.ziplimitsize;
  }

  /** @param ziplimitsize The ziplimitsize to set. */
  public void setZipLimitSize(String ziplimitsize) {
    this.ziplimitsize = ziplimitsize;
  }

  /** @param zipFilename The zipFilename to set. */
  public void setZipFilename(String zipFilename) {
    this.zipFilename = zipFilename;
  }

  /** @return Returns the zipFiles. */
  public boolean isZipFiles() {
    return zipFiles;
  }

  /** @param zipFiles The zipFiles to set. */
  public void setZipFiles(boolean zipFiles) {
    this.zipFiles = zipFiles;
  }

  /** @return Returns the authenticationPassword. */
  public String getAuthenticationPassword() {
    return this.authenticationPassword;
  }

  /** @param authenticationPassword The authenticationPassword to set. */
  public void setAuthenticationPassword(String authenticationPassword) {
    this.authenticationPassword = authenticationPassword;
  }

  /** @return Returns the authenticationUser. */
  public String getAuthenticationUser() {
    return this.authenticationUser;
  }

  /** @param authenticationUser The authenticationUser to set. */
  public void setAuthenticationUser(String authenticationUser) {
    this.authenticationUser = authenticationUser;
  }

  /** @return Returns the usingAuthentication. */
  public boolean isUsingAuthentication() {
    return this.usingAuthentication;
  }

  /** @param usingAuthentication The usingAuthentication to set. */
  public void setUsingAuthentication(boolean usingAuthentication) {
    this.usingAuthentication = usingAuthentication;
  }

  /** @return the onlySendComment flag */
  public boolean isOnlySendComment() {
    return this.onlySendComment;
  }

  /** @param onlySendComment the onlySendComment flag to set */
  public void setOnlySendComment(boolean onlySendComment) {
    this.onlySendComment = onlySendComment;
  }

  /** @return the useHTML flag */
  public boolean isUseHTML() {
    return this.useHTML;
  }

  /** @param useHTML the useHTML to set */
  public void setUseHTML(boolean useHTML) {
    this.useHTML = useHTML;
  }

  /** @return the encoding */
  public String getEncoding() {
    return this.encoding;
  }

  /** @return the secure connection type */
  public String getSecureConnectionType() {
    return this.secureConnectionType;
  }

  /** @param secureConnectionType the secureconnectiontype to set */
  public void setSecureConnectionType(String secureConnectionType) {
    this.secureConnectionType = secureConnectionType;
  }

  /** @param replyToAddresses the replyToAddresses to set */
  public void setReplyToAddresses(String replyToAddresses) {
    this.replyToAddresses = replyToAddresses;
  }

  /** @return the secure replyToAddresses */
  public String getReplyToAddresses() {
    return this.replyToAddresses;
  }

  /** @param encoding the encoding to set */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /** @return the usingSecureAuthentication */
  public boolean isUsingSecureAuthentication() {
    return this.usingSecureAuthentication;
  }

  /** @param usingSecureAuthentication the usingSecureAuthentication to set */
  public void setUsingSecureAuthentication(boolean usingSecureAuthentication) {
    this.usingSecureAuthentication = usingSecureAuthentication;
  }

  /** @return the port */
  public String getPort() {
    return this.port;
  }

  /** @param port the port to set */
  public void setPort(String port) {
    this.port = port;
  }

  /** @param usePriority the usePriority to set */
  public void setUsePriority(boolean usePriority) {
    this.usePriority = usePriority;
  }

  /** @return the usePriority flag */
  public boolean isUsePriority() {
    return this.usePriority;
  }

  /** @return the priority */
  public String getPriority() {
    return this.priority;
  }

  /** @param importance the importance to set */
  public void setImportance(String importance) {
    this.importance = importance;
  }

  /** @return the importance */
  public String getImportance() {
    return this.importance;
  }

  public String getSensitivity() {
    return sensitivity;
  }

  public void setSensitivity(String sensitivity) {
    this.sensitivity = sensitivity;
  }

  /** @param priority the priority to set */
  public void setPriority(String priority) {
    this.priority = priority;
  }

  @Override
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
    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.NotReceivingFields"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "MailMeta.CheckResult.TransformRecevingData", prev.size() + ""),
              transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "MailMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
    }
    remarks.add(cr);

    // Servername
    if (Utils.isEmpty(server)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.ServerEmpty"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.ServerOk"),
              transformMeta);
      remarks.add(cr);
      // is the field exists?
      if (prev.indexOfValue(variables.resolve(server)) < 0) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_WARNING,
                BaseMessages.getString(PKG, "MailMeta.CheckResult.ServerFieldNotFound", server),
                transformMeta);
      }
      remarks.add(cr);
    }

    // port number
    if (Utils.isEmpty(port)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.PortEmpty"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.PortOk"),
              transformMeta);
    }
    remarks.add(cr);

    // reply address
    if (Utils.isEmpty(replyAddress)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.ReplayAddressEmpty"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.ReplayAddressOk"),
              transformMeta);
    }
    remarks.add(cr);

    // Destination
    if (Utils.isEmpty(destination)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.DestinationEmpty"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.DestinationOk"),
              transformMeta);
    }
    remarks.add(cr);

    // Subject
    if (Utils.isEmpty(subject)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.SubjectEmpty"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.SubjectOk"),
              transformMeta);
    }
    remarks.add(cr);

    // Comment
    if (Utils.isEmpty(comment)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.CommentEmpty"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.CommentEmpty"),
              transformMeta);
    }
    remarks.add(cr);

    if (isFilenameDynamic) {
      // Dynamic Filename field
      if (Utils.isEmpty(dynamicFieldname)) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "MailMeta.CheckResult.DynamicFilenameFieldEmpty"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "MailMeta.CheckResult.DynamicFilenameFieldOk"),
                transformMeta);
      }
      remarks.add(cr);

    } else {
      // static filename
      if (Utils.isEmpty(sourcefilefoldername)) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "MailMeta.CheckResult.SourceFilenameEmpty"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "MailMeta.CheckResult.SourceFilenameOk"),
                transformMeta);
      }
      remarks.add(cr);
    }

    if (isZipFiles()) {
      if (isFilenameDynamic) {
        // dynamic zipfilename
        if (Utils.isEmpty(getDynamicZipFilenameField())) {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "MailMeta.CheckResult.DynamicZipfilenameEmpty"),
                  transformMeta);
        } else {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "MailMeta.CheckResult.DynamicZipfilenameOK"),
                  transformMeta);
        }
        remarks.add(cr);

      } else {
        // static zipfilename
        if (Utils.isEmpty(zipFilename)) {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "MailMeta.CheckResult.ZipfilenameEmpty"),
                  transformMeta);
        } else {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "MailMeta.CheckResult.ZipfilenameOk"),
                  transformMeta);
        }
        remarks.add(cr);
      }
    }
  }

  @Override
  public MailData getTransformData() {
    return new MailData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
