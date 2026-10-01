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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit test for {@link ActionGetPOP} */
class ActionGetPOPTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void defaultsUsePop3AllMessagesAndGetAction() {
    ActionGetPOP action = new ActionGetPOP("mails");
    assertEquals("mails", action.getName());
    assertEquals(MailConnectionMeta.PROTOCOL_STRING_POP3, action.getProtocol());
    assertEquals(MailConnectionMeta.VALUE_IMAP_LIST_ALL, action.getValueIMAPList());
    assertEquals(MailConnectionMeta.ACTION_TYPE_GET, action.getActionType());
    assertEquals(MailConnectionMeta.AFTER_GET_IMAP_NOTHING, action.getAfterGetIMAP());
    assertEquals(0, action.getRetrieveMails());
    assertTrue(action.isSaveMessage());
    assertTrue(action.isSaveAttachment());
    assertTrue(action.isEvaluation());
  }

  @Test
  void realAccessorsResolveVariables() {
    ActionGetPOP action = new ActionGetPOP();
    action.setVariable("MAIL_HOST", "imap.example.com");
    action.setVariable("MAIL_USER", "alice");
    action.setVariable("MAIL_PORT", "993");
    action.setVariable("MAIL_DIR", "/tmp/out");
    action.setVariable("MAIL_ATTACH", "/tmp/att");
    action.setVariable("MAIL_PROXY", "proxy-user");
    action.setVariable("MAIL_PATTERN", "msg_${MAIL_USER}.eml");

    action.setServerName("${MAIL_HOST}");
    action.setUserName("${MAIL_USER}");
    action.setSslPort("${MAIL_PORT}");
    action.setOutputDirectory("${MAIL_DIR}");
    action.setAttachmentFolder("${MAIL_ATTACH}");
    action.setProxyUsername("${MAIL_PROXY}");
    action.setFilenamePattern("${MAIL_PATTERN}");

    assertEquals("imap.example.com", action.getRealServername());
    assertEquals("alice", action.getRealUsername());
    assertEquals("993", action.getRealPort());
    assertEquals("/tmp/out", action.getRealOutputDirectory());
    assertEquals("/tmp/att", action.getRealAttachmentFolder());
    assertEquals("proxy-user", action.getRealProxyUsername());
    assertEquals("msg_alice.eml", action.getRealFilenamePattern());
  }

  @Test
  void checkReportsMissingServer() {
    ActionGetPOP action = new ActionGetPOP("mails");
    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, new WorkflowMeta(), new Variables(), new MemoryMetadataProvider());
    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void createOutputDirectoryRejectsUnknownFolderType() {
    ActionGetPOP action = new ActionGetPOP();
    assertThrows(IllegalArgumentException.class, () -> action.createOutputDirectory(99));
  }

  @Test
  void lombokAccessorsCoverImapRetrieveSettings() {
    ActionGetPOP action = new ActionGetPOP();
    action.setValueIMAPList(MailConnectionMeta.VALUE_IMAP_LIST_UNREAD);
    action.setActionType(MailConnectionMeta.ACTION_TYPE_MOVE);
    action.setAfterGetIMAP(MailConnectionMeta.AFTER_GET_IMAP_MOVE);
    action.setConditionReceivedDate(MailConnectionMeta.CONDITION_DATE_BETWEEN);
    action.setImapFolder("Archive");
    action.setIncludeSubFolders(true);

    assertEquals(MailConnectionMeta.VALUE_IMAP_LIST_UNREAD, action.getValueIMAPList());
    assertEquals(MailConnectionMeta.ACTION_TYPE_MOVE, action.getActionType());
    assertEquals(MailConnectionMeta.AFTER_GET_IMAP_MOVE, action.getAfterGetIMAP());
    assertEquals(MailConnectionMeta.CONDITION_DATE_BETWEEN, action.getConditionReceivedDate());
    assertEquals("Archive", action.getImapFolder());
    assertTrue(action.isIncludeSubFolders());
  }
}
