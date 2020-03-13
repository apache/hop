/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.getpop;

import org.apache.hop.job.entry.loadSave.JobEntryLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.IntLoadSaveValidator;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobEntryGetPOPLoadSaveTest extends JobEntryLoadSaveTestSupport<JobEntryGetPOP> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<JobEntryGetPOP> getJobEntryClass() {
    return JobEntryGetPOP.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] { "serverName", "userName", "password", "useSSL", "port", "outputDirectory",
      "filenamePattern", "retrievemails", "firstMails", "delete", "saveMessage", "saveAttachment",
      "differentFolderForAttachment", "protocol", "attachmentFolder", "attachmentWildcard", "valueImapList",
      "firstIMAPMails", "IMAPFolder", "senderSearchTerm", "notTermSenderSearch", "receipientSearch",
      "notTermReceipientSearch", "subjectSearch", "notTermSubjectSearch", "bodySearch", "notTermBodySearch",
      "conditionReceivedDate", "notTermReceivedDateSearch", "receivedDate1", "receivedDate2", "actiontype",
      "moveToIMAPFolder", "createMoveToFolder", "createLocalFolder", "afterGetIMAP", "includeSubFolders",
      "useProxy", "proxyUsername" } );
  }

  @Override
  protected Map<String, FieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, FieldLoadSaveValidator<?>> validators = new HashMap<String, FieldLoadSaveValidator<?>>();
    validators.put( "valueImapList", new IntLoadSaveValidator( MailConnectionMeta.valueIMAPListCode.length ) );
    validators.put( "conditionReceivedDate", new IntLoadSaveValidator( MailConnectionMeta.conditionDateCode.length ) );
    validators.put( "actiontype", new IntLoadSaveValidator( MailConnectionMeta.actionTypeCode.length ) );
    validators.put( "afterGetIMAP", new IntLoadSaveValidator( MailConnectionMeta.afterGetIMAPCode.length ) );

    return validators;
  }

}
