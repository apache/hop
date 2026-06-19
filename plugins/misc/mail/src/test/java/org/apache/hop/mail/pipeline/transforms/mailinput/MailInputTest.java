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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.mail.workflow.actions.getpop.MailConnectionMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MailInputTest {

  private TransformMockHelper<MailInputMeta, MailInputData> mockHelper;

  @BeforeAll
  static void initEnvironment() throws Exception {
    // Registers JavaMail providers (mstor, etc.) so MailConnection's constructor
    // does not throw a NoSuchProviderException for MBOX.
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    mockHelper = new TransformMockHelper<>("MailInput", MailInputMeta.class, MailInputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void cleanUp() {
    mockHelper.cleanUp();
  }

  /** Build a transform whose data + meta references are the ones we want to assert against. */
  private MailInput buildTransform(MailInputMeta meta, MailInputData data) {
    return new MailInput(
        mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  private MailInputMeta stubBaseMeta(String protocol) {
    MailInputMeta meta = mock(MailInputMeta.class);
    when(meta.isUseDynamicFolder()).thenReturn(false);
    when(meta.getProtocol()).thenReturn(protocol);
    when(meta.getImapFirstMails()).thenReturn("2");
    when(meta.getFirstMails()).thenReturn("3");
    return meta;
  }

  /** IMAP uses imapFirstMails as the row-limit fallback. */
  @Test
  void testInitSetGetFirstForIMAP() {
    MailInputData data = new MailInputData();
    MailInputMeta meta = stubBaseMeta(MailConnectionMeta.PROTOCOL_STRING_IMAP);

    buildTransform(meta, data).init();

    Assertions.assertEquals(2, data.rowlimit, "Row Limit is set up to 2 rows.");
  }

  /** POP3 uses firstMails as the row-limit fallback. */
  @Test
  void testInitSetGetFirstForPOP3() {
    MailInputData data = new MailInputData();
    MailInputMeta meta = stubBaseMeta(MailConnectionMeta.PROTOCOL_STRING_POP3);

    buildTransform(meta, data).init();

    Assertions.assertEquals(3, data.rowlimit, "Row Limit is set up to 3 rows.");
  }

  /** An explicit Limit value overrides the protocol-specific fallback. */
  @Test
  void testInitSetGetFirstLimitOverride() {
    MailInputData data = new MailInputData();
    MailInputMeta meta = stubBaseMeta(MailConnectionMeta.PROTOCOL_STRING_POP3);
    when(meta.getRowLimit()).thenReturn("5");

    buildTransform(meta, data).init();

    Assertions.assertEquals(
        5, data.rowlimit, "Row Limit is set up to 5 rows as the Limit has priority.");
  }

  /** MBOX has no retrieve-first option, so the row limit stays at 0. */
  @Test
  void testInitSetGetFirstForMBOXIgnored() {
    MailInputData data = new MailInputData();
    MailInputMeta meta = stubBaseMeta(MailConnectionMeta.PROTOCOL_STRING_MBOX);

    // For MBOX the legacy MailConnection constructor needs the mstor provider, which is
    // not on this module's test classpath; the failure leaves data.mailConn null and a
    // later getFolders() call NPEs. data.rowlimit is set before that, which is what we
    // care about here.
    try {
      buildTransform(meta, data).init();
    } catch (NullPointerException ignored) {
      // expected — see comment above
    }

    Assertions.assertEquals(0, data.rowlimit, "Row Limit defaults to 0 for MBOX.");
  }
}
