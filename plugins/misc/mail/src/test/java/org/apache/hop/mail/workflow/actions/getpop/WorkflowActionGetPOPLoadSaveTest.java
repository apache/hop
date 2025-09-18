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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.jupiter.api.extension.RegisterExtension;

class WorkflowActionGetPOPLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionGetPOP> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Override
  protected Class<ActionGetPOP> getActionClass() {
    return ActionGetPOP.class;
  }

  @Override
  protected List<String> listAttributes() {
    return Arrays.asList(
        "serverName",
        "userName",
        "password",
        "useSsl",
        "SslPort",
        "outputDirectory",
        "filenamePattern",
        "retrieveMails",
        "firstMails",
        "delete",
        "saveMessage",
        "saveAttachment",
        "useDifferentFolderForAttachment",
        "protocol",
        "attachmentFolder",
        "attachmentWildcard",
        //        "valueIMAPList",
        "firstMails",
        "imapFolder",
        "senderSearch",
        "notTermSenderSearch",
        "recipientSearch",
        "notTermRecipientSearch",
        "subjectSearch",
        "notTermSubjectSearch",
        "bodySearch",
        "notTermBodySearch",
        //        "conditionReceivedDate",
        "notTermReceivedDateSearch",
        "receivedDate1",
        "receivedDate2",
        //        "actionType",
        "moveToIMAPFolder",
        "createMoveToFolder",
        "createLocalFolder",
        //        "afterGetIMAP",
        "includeSubFolders",
        "useProxy",
        "proxyUsername");
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, IFieldLoadSaveValidator<?>> validators = new HashMap<>();
    //    validators.put(
    //        "valueIMAPList", new
    // IntLoadSaveValidator(MailConnectionMeta.valueIMAPListCode.length));
    //    validators.put(
    //        "conditionReceivedDate",
    //        new IntLoadSaveValidator(MailConnectionMeta.conditionDateCode.length));
    //    validators.put(
    //        "actionType", new IntLoadSaveValidator(MailConnectionMeta.actionTypeCode.length));
    //    validators.put(
    //        "afterGetIMAP", new IntLoadSaveValidator(MailConnectionMeta.afterGetIMAPCode.length));

    return validators;
  }
}
