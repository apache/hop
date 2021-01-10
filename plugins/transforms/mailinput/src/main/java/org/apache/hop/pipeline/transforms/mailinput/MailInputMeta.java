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

package org.apache.hop.pipeline.transforms.mailinput;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
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
import org.apache.hop.workflow.actions.getpop.MailConnectionMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "MailInput",
    image = "mailinput.svg",
    name = "i18n::BaseTransform.TypeLongDesc.MailInput",
    description = "i18n::BaseTransform.TypeTooltipDesc.MailInput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/mailinput.html")
public class MailInputMeta extends BaseTransformMeta
    implements ITransformMeta<MailInput, MailInputData> {
  private static final Class<?> PKG = MailInputMeta.class; // For Translator

  public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
  public static int DEFAULT_BATCH_SIZE = 500;

  public int conditionReceivedDate;

  public int valueimaplist;

  private String servername;
  private String username;
  private String password;
  private boolean usessl;
  private String sslport;
  private String firstmails;
  public int retrievemails;
  private boolean delete;
  private String protocol;
  private String imapfirstmails;
  private String imapfolder;
  // search term
  private String senderSearch;
  private boolean notTermSenderSearch;
  private String recipientSearch;
  private String subjectSearch;
  private String receivedDate1;
  private String receivedDate2;
  private boolean notTermSubjectSearch;
  private boolean notTermRecipientSearch;
  private boolean notTermReceivedDateSearch;
  private boolean includesubfolders;
  private boolean useproxy;
  private String proxyusername;
  private String folderfield;
  private boolean usedynamicfolder;
  private String rowlimit;

  /** The fields ... */
  private MailInputField[] inputFields;

  private boolean useBatch;
  private String start;
  private String end;

  private Integer batchSize = DEFAULT_BATCH_SIZE;

  private boolean stopOnError;

  public MailInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int nrFields) {
    inputFields = new MailInputField[nrFields];
  }

  @Override
  public Object clone() {
    MailInputMeta retval = (MailInputMeta) super.clone();
    int nrFields = inputFields.length;
    retval.allocate(nrFields);
    for (int i = 0; i < nrFields; i++) {
      if (inputFields[i] != null) {
        retval.inputFields[i] = (MailInputField) inputFields[i].clone();
      }
    }

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      MailInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new MailInput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  private void readData(Node transformNode) {
    servername = XmlHandler.getTagValue(transformNode, "servername");
    username = XmlHandler.getTagValue(transformNode, "username");
    password =
        Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(transformNode, "password"));
    usessl = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "usessl"));
    sslport = XmlHandler.getTagValue(transformNode, "sslport");
    retrievemails = Const.toInt(XmlHandler.getTagValue(transformNode, "retrievemails"), -1);
    firstmails = XmlHandler.getTagValue(transformNode, "firstmails");
    delete = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "delete"));

    protocol =
        Const.NVL(
            XmlHandler.getTagValue(transformNode, "protocol"),
            MailConnectionMeta.PROTOCOL_STRING_POP3);
    valueimaplist =
        MailConnectionMeta.getValueImapListByCode(
            Const.NVL(XmlHandler.getTagValue(transformNode, "valueimaplist"), ""));
    imapfirstmails = XmlHandler.getTagValue(transformNode, "imapfirstmails");
    imapfolder = XmlHandler.getTagValue(transformNode, "imapfolder");
    // search term
    senderSearch = XmlHandler.getTagValue(transformNode, "sendersearch");
    notTermSenderSearch =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "nottermsendersearch"));
    recipientSearch = XmlHandler.getTagValue(transformNode, "recipientsearch");
    notTermRecipientSearch =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "notTermRecipientSearch"));
    subjectSearch = XmlHandler.getTagValue(transformNode, "subjectsearch");
    notTermSubjectSearch =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "nottermsubjectsearch"));
    conditionReceivedDate =
        MailConnectionMeta.getConditionByCode(
            Const.NVL(XmlHandler.getTagValue(transformNode, "conditionreceiveddate"), ""));
    notTermReceivedDateSearch =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "nottermreceiveddatesearch"));
    receivedDate1 = XmlHandler.getTagValue(transformNode, "receivedDate1");
    receivedDate2 = XmlHandler.getTagValue(transformNode, "receivedDate2");
    includesubfolders =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includesubfolders"));
    usedynamicfolder =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "usedynamicfolder"));
    folderfield = XmlHandler.getTagValue(transformNode, "folderfield");
    proxyusername = XmlHandler.getTagValue(transformNode, "proxyusername");
    useproxy = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "useproxy"));
    useBatch = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, Tags.USE_BATCH));
    try {
      batchSize = Integer.parseInt(XmlHandler.getTagValue(transformNode, Tags.BATCH_SIZE));
    } catch (NumberFormatException e) {
      batchSize = DEFAULT_BATCH_SIZE;
    }
    start = XmlHandler.getTagValue(transformNode, Tags.START_MSG);
    end = XmlHandler.getTagValue(transformNode, Tags.END_MSG);
    stopOnError = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, Tags.STOP_ON_ERROR));

    rowlimit = XmlHandler.getTagValue(transformNode, "rowlimit");
    Node fields = XmlHandler.getSubNode(transformNode, "fields");
    int nrFields = XmlHandler.countNodes(fields, "field");

    allocate(nrFields);
    for (int i = 0; i < nrFields; i++) {
      Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
      inputFields[i] = new MailInputField();
      inputFields[i].setName(XmlHandler.getTagValue(fnode, "name"));
      inputFields[i].setColumn(
          MailInputField.getColumnByCode(XmlHandler.getTagValue(fnode, "column")));
    }
  }

  @Override
  public void setDefault() {
    servername = null;
    username = null;
    password = null;
    usessl = false;
    sslport = null;
    retrievemails = 0;
    firstmails = null;
    delete = false;
    protocol = MailConnectionMeta.PROTOCOL_STRING_POP3;
    imapfirstmails = "0";
    valueimaplist = MailConnectionMeta.VALUE_IMAP_LIST_ALL;
    imapfolder = null;
    // search term
    senderSearch = null;
    notTermSenderSearch = false;
    notTermRecipientSearch = false;
    notTermSubjectSearch = false;
    receivedDate1 = null;
    receivedDate2 = null;
    notTermReceivedDateSearch = false;
    recipientSearch = null;
    subjectSearch = null;
    includesubfolders = false;
    useproxy = false;
    proxyusername = null;
    folderfield = null;
    usedynamicfolder = false;
    rowlimit = "0";

    batchSize = DEFAULT_BATCH_SIZE;
    useBatch = false;
    start = null;
    end = null;
    stopOnError = true;

    int nrFields = 0;
    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      inputFields[i] = new MailInputField("field" + (i + 1));
    }
  }

  private static final class Tags {
    static final String USE_BATCH = "useBatch";
    static final String BATCH_SIZE = "batchSize";
    static final String START_MSG = "startMsg";
    static final String END_MSG = "endMsg";
    static final String STOP_ON_ERROR = "stopOnError";
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();
    String tab = "      ";
    retval.append("      ").append(XmlHandler.addTagValue("servername", servername));
    retval.append("      ").append(XmlHandler.addTagValue("username", username));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue("password", Encr.encryptPasswordIfNotUsingVariables(password)));
    retval.append("      ").append(XmlHandler.addTagValue("usessl", usessl));
    retval.append("      ").append(XmlHandler.addTagValue("sslport", sslport));
    retval.append("      ").append(XmlHandler.addTagValue("retrievemails", retrievemails));
    retval.append("      ").append(XmlHandler.addTagValue("firstmails", firstmails));
    retval.append("      ").append(XmlHandler.addTagValue("delete", delete));
    retval.append("      ").append(XmlHandler.addTagValue("protocol", protocol));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "valueimaplist", MailConnectionMeta.getValueImapListCode(valueimaplist)));
    retval.append("      ").append(XmlHandler.addTagValue("imapfirstmails", imapfirstmails));
    retval.append("      ").append(XmlHandler.addTagValue("imapfolder", imapfolder));
    // search term
    retval.append("      ").append(XmlHandler.addTagValue("sendersearch", senderSearch));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("nottermsendersearch", notTermSenderSearch));

    retval.append("      ").append(XmlHandler.addTagValue("recipientsearch", recipientSearch));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("notTermRecipientSearch", notTermRecipientSearch));
    retval.append("      ").append(XmlHandler.addTagValue("subjectsearch", subjectSearch));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("nottermsubjectsearch", notTermSubjectSearch));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "conditionreceiveddate",
                MailConnectionMeta.getConditionDateCode(conditionReceivedDate)));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("nottermreceiveddatesearch", notTermReceivedDateSearch));
    retval.append("      ").append(XmlHandler.addTagValue("receiveddate1", receivedDate1));
    retval.append("      ").append(XmlHandler.addTagValue("receiveddate2", receivedDate2));
    retval.append("      ").append(XmlHandler.addTagValue("includesubfolders", includesubfolders));
    retval.append("      ").append(XmlHandler.addTagValue("useproxy", useproxy));
    retval.append("      ").append(XmlHandler.addTagValue("proxyusername", proxyusername));
    retval.append("      ").append(XmlHandler.addTagValue("usedynamicfolder", usedynamicfolder));
    retval.append("      ").append(XmlHandler.addTagValue("folderfield", folderfield));
    retval.append("      ").append(XmlHandler.addTagValue("rowlimit", rowlimit));
    retval.append(tab).append(XmlHandler.addTagValue(Tags.USE_BATCH, useBatch));
    retval.append(tab).append(XmlHandler.addTagValue(Tags.BATCH_SIZE, batchSize));
    retval.append(tab).append(XmlHandler.addTagValue(Tags.START_MSG, start));
    retval.append(tab).append(XmlHandler.addTagValue(Tags.END_MSG, end));
    retval.append(tab).append(XmlHandler.addTagValue(Tags.STOP_ON_ERROR, stopOnError));

    /*
     * Describe the fields to read
     */
    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < inputFields.length; i++) {
      retval.append("      <field>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", inputFields[i].getName()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("column", inputFields[i].getColumnCode()));
      retval.append("      </field>").append(Const.CR);
    }
    retval.append("    </fields>").append(Const.CR);
    return retval.toString();
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
    // See if we get input...
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MailInputMeta.CheckResult.NoInputExpected"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailInputMeta.CheckResult.NoInput"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public String getPort() {
    return sslport;
  }

  public void setPort(String sslport) {
    this.sslport = sslport;
  }

  public void setFirstMails(String firstmails) {
    this.firstmails = firstmails;
  }

  public String getFirstMails() {
    return firstmails;
  }

  public boolean isIncludeSubFolders() {
    return includesubfolders;
  }

  public void setIncludeSubFolders(boolean includesubfolders) {
    this.includesubfolders = includesubfolders;
  }

  /** @return Returns the useproxy. */
  public boolean isUseProxy() {
    return this.useproxy;
  }

  public void setUseProxy(boolean useprox) {
    this.useproxy = useprox;
  }

  public void setProxyUsername(String username) {
    this.proxyusername = username;
  }

  public String getProxyUsername() {
    return this.proxyusername;
  }

  /** @return Returns the usedynamicfolder. */
  public boolean isDynamicFolder() {
    return this.usedynamicfolder;
  }

  public void setDynamicFolder(boolean usedynamicfolder) {
    this.usedynamicfolder = usedynamicfolder;
  }

  public void setRowLimit(String rowlimit) {
    this.rowlimit = rowlimit;
  }

  public String getRowLimit() {
    return this.rowlimit;
  }

  public void setFolderField(String folderfield) {
    this.folderfield = folderfield;
  }

  public String getFolderField() {
    return this.folderfield;
  }

  public void setFirstIMAPMails(String firstmails) {
    this.imapfirstmails = firstmails;
  }

  public String getFirstIMAPMails() {
    return imapfirstmails;
  }

  public void setSenderSearchTerm(String senderSearch) {
    this.senderSearch = senderSearch;
  }

  public String getSenderSearchTerm() {
    return this.senderSearch;
  }

  public void setNotTermSenderSearch(boolean notTermSenderSearch) {
    this.notTermSenderSearch = notTermSenderSearch;
  }

  public boolean isNotTermSenderSearch() {
    return this.notTermSenderSearch;
  }

  public void setNotTermSubjectSearch(boolean notTermSubjectSearch) {
    this.notTermSubjectSearch = notTermSubjectSearch;
  }

  public boolean isNotTermSubjectSearch() {
    return this.notTermSubjectSearch;
  }

  public void setNotTermReceivedDateSearch(boolean notTermReceivedDateSearch) {
    this.notTermReceivedDateSearch = notTermReceivedDateSearch;
  }

  public boolean isNotTermReceivedDateSearch() {
    return this.notTermReceivedDateSearch;
  }

  public void setNotTermRecipientSearch(boolean notTermRecipientSearch) {
    this.notTermRecipientSearch = notTermRecipientSearch;
  }

  public boolean isNotTermRecipientSearch() {
    return this.notTermRecipientSearch;
  }

  public void setRecipientSearch(String recipientSearch) {
    this.recipientSearch = recipientSearch;
  }

  public String getRecipientSearch() {
    return this.recipientSearch;
  }

  public void setSubjectSearch(String subjectSearch) {
    this.subjectSearch = subjectSearch;
  }

  public String getSubjectSearch() {
    return this.subjectSearch;
  }

  public String getReceivedDate1() {
    return this.receivedDate1;
  }

  public void setReceivedDate1(String inputDate) {
    this.receivedDate1 = inputDate;
  }

  public String getReceivedDate2() {
    return this.receivedDate2;
  }

  public void setReceivedDate2(String inputDate) {
    this.receivedDate2 = inputDate;
  }

  public void setConditionOnReceivedDate(int conditionReceivedDate) {
    this.conditionReceivedDate = conditionReceivedDate;
  }

  public int getConditionOnReceivedDate() {
    return this.conditionReceivedDate;
  }

  public void setServerName(String servername) {
    this.servername = servername;
  }

  public String getServerName() {
    return servername;
  }

  public void setUserName(String username) {
    this.username = username;
  }

  public String getUserName() {
    return username;
  }

  /**
   *
   * <li>0 = retrieve all
   * <li>2 = retrieve unread
   *
   * @param nr
   * @see {@link #setValueImapList(int)}
   */
  public void setRetrievemails(int nr) {
    retrievemails = nr;
  }

  public int getRetrievemails() {
    return this.retrievemails;
  }

  public int getValueImapList() {
    return valueimaplist;
  }

  public void setValueImapList(int value) {
    this.valueimaplist = value;
  }

  /** @return Returns the input fields. */
  public MailInputField[] getInputFields() {
    return inputFields;
  }

  /** @param inputFields The input fields to set. */
  public void setInputFields(MailInputField[] inputFields) {
    this.inputFields = inputFields;
  }

  /** @return Returns the password. */
  public String getPassword() {
    return password;
  }

  /** @param delete The delete to set. */
  public void setDelete(boolean delete) {
    this.delete = delete;
  }

  /** @return Returns the delete. */
  public boolean getDelete() {
    return delete;
  }

  public String getProtocol() {
    return protocol;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public String getIMAPFolder() {
    return imapfolder;
  }

  public void setIMAPFolder(String folder) {
    this.imapfolder = folder;
  }

  /** @param usessl The usessl to set. */
  public void setUseSSL(boolean usessl) {
    this.usessl = usessl;
  }

  /** @return Returns the usessl. */
  public boolean isUseSSL() {
    return usessl;
  }

  /** @param password The password to set. */
  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public MailInputData getTransformData() {
    return new MailInputData();
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    int i;
    for (i = 0; i < inputFields.length; i++) {
      MailInputField field = inputFields[i];
      IValueMeta v = new ValueMetaString(variables.resolve(field.getName()));
      switch (field.getColumn()) {
        case MailInputField.COLUMN_MESSAGE_NR:
        case MailInputField.COLUMN_SIZE:
        case MailInputField.COLUMN_ATTACHED_FILES_COUNT:
          v = new ValueMetaInteger(variables.resolve(field.getName()));
          v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
          break;
        case MailInputField.COLUMN_RECEIVED_DATE:
        case MailInputField.COLUMN_SENT_DATE:
          v = new ValueMetaDate(variables.resolve(field.getName()));
          break;
        case MailInputField.COLUMN_FLAG_DELETED:
        case MailInputField.COLUMN_FLAG_DRAFT:
        case MailInputField.COLUMN_FLAG_FLAGGED:
        case MailInputField.COLUMN_FLAG_NEW:
        case MailInputField.COLUMN_FLAG_READ:
          v = new ValueMetaBoolean(variables.resolve(field.getName()));
          break;
        default:
          // STRING
          v.setLength(250);
          v.setPrecision(-1);
          break;
      }
      v.setOrigin(name);
      r.addValueMeta(v);
    }
  }

  public boolean useBatch() {
    return useBatch;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  public boolean isStopOnError() {
    return stopOnError;
  }

  public void setStopOnError(boolean breakOnError) {
    this.stopOnError = breakOnError;
  }

  public boolean isUseBatch() {
    return useBatch;
  }

  public void setUseBatch(boolean useBatch) {
    this.useBatch = useBatch;
  }

  public String getStart() {
    return start;
  }

  public void setStart(String start) {
    this.start = start;
  }

  public String getEnd() {
    return end;
  }

  public void setEnd(String end) {
    this.end = end;
  }

  public void setBatchSize(Integer batchSize) {
    this.batchSize = batchSize;
  }
}
