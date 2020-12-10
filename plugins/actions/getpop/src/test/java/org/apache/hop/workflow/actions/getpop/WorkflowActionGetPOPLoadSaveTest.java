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

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkflowActionGetPOPLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionGetPOP> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionGetPOP> getActionClass() {
    return ActionGetPOP.class;
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
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, IFieldLoadSaveValidator<?>> validators = new HashMap<>();
    validators.put( "valueImapList", new IntLoadSaveValidator( MailConnectionMeta.valueIMAPListCode.length ) );
    validators.put( "conditionReceivedDate", new IntLoadSaveValidator( MailConnectionMeta.conditionDateCode.length ) );
    validators.put( "actiontype", new IntLoadSaveValidator( MailConnectionMeta.actionTypeCode.length ) );
    validators.put( "afterGetIMAP", new IntLoadSaveValidator( MailConnectionMeta.afterGetIMAPCode.length ) );

    return validators;
  }

}
