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

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.mail.workflow.actions.getpop.MailConnectionMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class MailInputTest {

  private TransformMockHelper<MailInputMeta, MailInputData> mockHelper;

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

  /** Check that imap retrieve ... first will be applied. */
  @Test
  @Disabled("This test needs to be reviewed")
  void testInitSetGetFirstForIMAP() {
    MailInput transform =
        new MailInput(
            mockHelper.transformMeta,
            new MailInputMeta(),
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    MailInputData data = new MailInputData();
    MailInputMeta meta = mock(MailInputMeta.class);
    when(meta.isUseDynamicFolder()).thenReturn(false);
    when(meta.getProtocol()).thenReturn(MailConnectionMeta.PROTOCOL_STRING_IMAP);
    when(meta.getImapFirstMails()).thenReturn("2");
    when(meta.getFirstMails()).thenReturn("3");

    transform.init();

    Assertions.assertEquals(2, data.rowlimit, "Row Limit is set up to 2 rows.");
  }

  /** Check that pop3 retrieve ... first will be applied. */
  @Test
  @Disabled("This test needs to be reviewed")
  void testInitSetGetFirstForPOP3() {
    MailInput transform =
        new MailInput(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    MailInputData data = new MailInputData();
    MailInputMeta meta = mock(MailInputMeta.class);
    when(meta.isUseDynamicFolder()).thenReturn(false);
    when(meta.getProtocol()).thenReturn(MailConnectionMeta.PROTOCOL_STRING_POP3);
    when(meta.getImapFirstMails()).thenReturn("2");
    when(meta.getFirstMails()).thenReturn("3");

    transform.init();

    Assertions.assertEquals(3, data.rowlimit, "Row Limit is set up to 3 rows.");
  }

  /** Check that Limit value overrides retrieve ... first if any. */
  @Test
  @Disabled("This test needs to be reviewed")
  void testInitSetGetFirstLimitOverride() {
    MailInput transform =
        new MailInput(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    MailInputData data = new MailInputData();
    MailInputMeta meta = mock(MailInputMeta.class);
    when(meta.isUseDynamicFolder()).thenReturn(false);
    when(meta.getProtocol()).thenReturn(MailConnectionMeta.PROTOCOL_STRING_POP3);
    when(meta.getImapFirstMails()).thenReturn("2");
    when(meta.getFirstMails()).thenReturn("3");

    when(meta.getRowLimit()).thenReturn("5");

    transform.init();

    Assertions.assertEquals(
        5, data.rowlimit, "Row Limit is set up to 5 rows as the Limit has priority.");
  }

  /** We do not use any of retrieve ... first if protocol is MBOX */
  @Test
  @Disabled("This test needs to be reviewed")
  void testInitSetGetFirstForMBOXIgnored() {
    MailInput transform =
        new MailInput(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    MailInputData data = new MailInputData();
    MailInputMeta meta = mock(MailInputMeta.class);
    when(meta.isUseDynamicFolder()).thenReturn(false);
    when(meta.getProtocol()).thenReturn(MailConnectionMeta.PROTOCOL_STRING_MBOX);
    when(meta.getImapFirstMails()).thenReturn("2");
    when(meta.getFirstMails()).thenReturn("3");

    transform.init();

    Assertions.assertEquals(
        0, data.rowlimit, "Row Limit is set up to 0 rows as the Limit has priority.");
  }
}
