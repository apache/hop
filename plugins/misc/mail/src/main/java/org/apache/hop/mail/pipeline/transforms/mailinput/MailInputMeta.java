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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mail.workflow.actions.getpop.MailConnectionMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "MailInput",
    image = "mailinput.svg",
    name = "i18n::MailInput.Name",
    description = "i18n::MailInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::MailInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/emailinput.html")
public class MailInputMeta extends BaseTransformMeta<MailInput, MailInputData> {
  private static final Class<?> PKG = MailInputMeta.class;
  private static final String CONST_SPACE = "      ";
  private static final String CONST_FIELD = "field";

  public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
  public static final int DEFAULT_BATCH_SIZE = 500;

  private String conditionReceivedDateStr;

  @HopMetadataProperty(key = "conditionreceiveddate")
  public int conditionReceivedDate;

  public Map<String, String> valueImaps =
      new HashMap<String, String>() {
        {
          put("imaplistall", BaseMessages.getString(PKG, "ActionGetPOP.IMAPListGetAll.Label"));
          put("imaplistnew", BaseMessages.getString(PKG, "ActionGetPOP.IMAPListGetNew.Label"));
          put("imaplistold", BaseMessages.getString(PKG, "ActionGetPOP.IMAPListGetOld.Label"));
          put("imaplistread", BaseMessages.getString(PKG, "ActionGetPOP.IMAPListGetRead.Label"));
          put(
              "imaplistunread",
              BaseMessages.getString(PKG, "ActionGetPOP.IMAPListGetUnread.Label"));
          put(
              "imaplistflagged",
              BaseMessages.getString(PKG, "ActionGetPOP.IMAPListGetFlagged.Label"));
          put(
              "imaplistnotflagged",
              BaseMessages.getString(PKG, "ActionGetPOP.IMAPListGetNotFlagged.Label"));
          put("imaplistdraft", BaseMessages.getString(PKG, "ActionGetPOP.IMAPListGetDraft.Label"));
          put(
              "imaplistnotdraft",
              BaseMessages.getString(PKG, "ActionGetPOP.IMAPListGetNotDraft.Label"));
          put(
              "imaplistanswered",
              BaseMessages.getString(PKG, "ActionGetPOP.IMAPListGetAnswered.Label"));
          put(
              "imaplistnotanswered",
              BaseMessages.getString(PKG, "ActionGetPOP.IMAPListGetNotAnswered.Label"));
        }
      };

  private int valueImapList;

  @HopMetadataProperty(key = "valueimaplist")
  private String valueImap;

  @HopMetadataProperty(key = "servername")
  private String serverName;

  @HopMetadataProperty(key = "username")
  private String username;

  @HopMetadataProperty(password = true)
  private String password;

  @HopMetadataProperty(key = "usessl")
  private boolean useSsl;

  @HopMetadataProperty(key = "usexoauth2")
  private boolean useXOAuth2;

  @HopMetadataProperty(key = "sslport")
  private String sslPort;

  @HopMetadataProperty(key = "firstmails")
  private String firstMails;

  @HopMetadataProperty(key = "retrievemails")
  public int retrieveMails;

  @HopMetadataProperty private boolean delete;

  @HopMetadataProperty private String protocol;

  @HopMetadataProperty(key = "imapfirstmails")
  private String imapFirstMails;

  @HopMetadataProperty(key = "imapfolder")
  private String imapFolder;

  // search term
  @HopMetadataProperty private String senderSearch;

  @HopMetadataProperty private boolean notTermSenderSearch;

  @HopMetadataProperty private String recipientSearch;

  @HopMetadataProperty private String subjectSearch;

  @HopMetadataProperty private String receivedDate1;

  @HopMetadataProperty private String receivedDate2;

  @HopMetadataProperty private boolean notTermSubjectSearch;

  @HopMetadataProperty private boolean notTermRecipientSearch;

  @HopMetadataProperty private boolean notTermReceivedDateSearch;

  @HopMetadataProperty(key = "includesubfolders")
  private boolean includeSubFolders;

  @HopMetadataProperty(key = "useproxy")
  private boolean useProxy;

  @HopMetadataProperty(key = "proxyusername")
  private String proxyUsername;

  @HopMetadataProperty(key = "folderfield")
  private String folderField;

  @HopMetadataProperty(key = "usedynamicfolder")
  private boolean useDynamicFolder;

  @HopMetadataProperty(key = "rowlimit")
  private String rowLimit;

  @HopMetadataProperty(key = "connection_name")
  private String connectionName;

  /** The fields ... */
  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<MailInputField> inputFields;

  @HopMetadataProperty(key = "USE_BATCH")
  private boolean useBatch;

  @HopMetadataProperty(key = "startMsg")
  private String start;

  @HopMetadataProperty(key = "endMsg")
  private String end;

  @HopMetadataProperty(key = "BATCH_SIZE")
  private Integer batchSize = DEFAULT_BATCH_SIZE;

  @HopMetadataProperty private boolean stopOnError;

  public MailInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public Object clone() {
    MailInputMeta retval = (MailInputMeta) super.clone();
    int nrFields = inputFields.size();
    //    retval.allocate(nrFields);
    List<MailInputField> retvalFields = new ArrayList<MailInputField>();
    for (int i = 0; i < nrFields; i++) {
      if (inputFields.get(i) != null) {
        retvalFields.add((MailInputField) inputFields.get(i).clone());
        //        retval.inputFields.set(i) = (MailInputField) inputFields.get(i).clone();
      }
    }
    retval.setInputFields(retvalFields);

    return retval;
  }

  @Override
  public void setDefault() {
    serverName = null;
    username = null;
    password = null;
    useSsl = false;
    useXOAuth2 = false;
    sslPort = null;
    retrieveMails = 0;
    firstMails = null;
    delete = false;
    protocol = MailConnectionMeta.PROTOCOL_STRING_POP3;
    imapFirstMails = "0";
    valueImapList = MailConnectionMeta.VALUE_IMAP_LIST_ALL;
    imapFolder = null;
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
    includeSubFolders = false;
    useProxy = false;
    proxyUsername = null;
    folderField = null;
    useDynamicFolder = false;
    rowLimit = "0";

    batchSize = DEFAULT_BATCH_SIZE;
    useBatch = false;
    start = null;
    end = null;
    stopOnError = true;

    int nrFields = 0;

    inputFields = new ArrayList<>();
  }

  private static final class Tags {
    static final String USE_BATCH = "useBatch";
    static final String BATCH_SIZE = "batchSize";
    static final String START_MSG = "startMsg";
    static final String END_MSG = "endMsg";
    static final String STOP_ON_ERROR = "stopOnError";
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
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MailInputMeta.CheckResult.NoInputExpected"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailInputMeta.CheckResult.NoInput"),
              transformMeta);
      remarks.add(cr);
    }
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
    for (i = 0; i < inputFields.size(); i++) {
      MailInputField field = inputFields.get(i);
      IValueMeta v = new ValueMetaString(variables.resolve(field.getName()));
      switch (field.getColumn()) {
        case MailInputField.COLUMN_MESSAGE_NR,
            MailInputField.COLUMN_SIZE,
            MailInputField.COLUMN_ATTACHED_FILES_COUNT:
          v = new ValueMetaInteger(variables.resolve(field.getName()));
          v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
          break;
        case MailInputField.COLUMN_RECEIVED_DATE, MailInputField.COLUMN_SENT_DATE:
          v = new ValueMetaDate(variables.resolve(field.getName()));
          break;
        case MailInputField.COLUMN_FLAG_DELETED,
            MailInputField.COLUMN_FLAG_DRAFT,
            MailInputField.COLUMN_FLAG_FLAGGED,
            MailInputField.COLUMN_FLAG_NEW,
            MailInputField.COLUMN_FLAG_READ:
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

  public void setValueImapList(int valueImapList) {
    this.valueImapList = valueImapList;
    valueImap = MailConnectionMeta.getValueImapListDesc(valueImapList);
  }

  public String getValueImap() {
    return MailConnectionMeta.getValueImapListCode(valueImapList);
  }

  public int getConditionReceivedDate() {
    return MailConnectionMeta.getConditionDateByDesc(conditionReceivedDateStr);
  }
}
