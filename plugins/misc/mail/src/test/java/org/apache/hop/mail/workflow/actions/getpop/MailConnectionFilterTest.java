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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.mail.Flags;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.search.AndTerm;
import jakarta.mail.search.BodyTerm;
import jakarta.mail.search.FlagTerm;
import jakarta.mail.search.FromStringTerm;
import jakarta.mail.search.NotTerm;
import jakarta.mail.search.ReceivedDateTerm;
import jakarta.mail.search.RecipientStringTerm;
import jakarta.mail.search.SubjectTerm;
import java.util.Date;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MailConnectionFilterTest {

  private MailConnection conn;

  @BeforeAll
  static void initLogStore() {
    // setReceivedDateTermEQ etc. log via LogChannel when protocol is POP3.
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws HopException, MessagingException {
    ILogChannel log = new LogChannel(new Object());
    conn =
        new MailConnection(
            log,
            MailConnectionMeta.PROTOCOL_IMAP,
            "junit",
            0,
            "junit",
            "junit",
            false,
            false,
            false,
            "junit");
  }

  @Test
  void senderTermAdded() {
    conn.setSenderTerm("a@x.com", false);
    assertInstanceOf(FromStringTerm.class, conn.getSearchTerm());
  }

  @Test
  void senderTermNegated() {
    conn.setSenderTerm("a@x.com", true);
    assertInstanceOf(NotTerm.class, conn.getSearchTerm());
  }

  @Test
  void senderTermEmptyIsNoop() {
    conn.setSenderTerm("", false);
    conn.setSenderTerm(null, true);
    assertNull(conn.getSearchTerm());
  }

  @Test
  void recipientTermAdded() {
    conn.setReceipientTerm("b@x.com");
    assertInstanceOf(RecipientStringTerm.class, conn.getSearchTerm());
  }

  @Test
  void subjectTermAdded() {
    conn.setSubjectTerm("invoice", false);
    assertInstanceOf(SubjectTerm.class, conn.getSearchTerm());
  }

  @Test
  void subjectTermNegated() {
    conn.setSubjectTerm("spam", true);
    assertInstanceOf(NotTerm.class, conn.getSearchTerm());
  }

  @Test
  void bodyTermAdded() {
    conn.setBodyTerm("keyword", false);
    assertInstanceOf(BodyTerm.class, conn.getSearchTerm());
  }

  @Test
  void bodyTermNegated() {
    conn.setBodyTerm("keyword", true);
    assertInstanceOf(NotTerm.class, conn.getSearchTerm());
  }

  @Test
  void chainingBuildsAndTerm() {
    conn.setSenderTerm("a@x.com", false);
    conn.setSubjectTerm("hi", false);
    assertInstanceOf(AndTerm.class, conn.getSearchTerm());
  }

  @Test
  void receivedDateTermsAreSkippedForPop3() throws HopException, MessagingException {
    MailConnection pop3 =
        new MailConnection(
            new LogChannel(new Object()),
            MailConnectionMeta.PROTOCOL_POP3,
            "junit",
            0,
            "junit",
            "junit",
            false,
            false,
            false,
            "junit");

    pop3.setReceivedDateTermEQ(new Date());
    pop3.setReceivedDateTermLT(new Date());
    pop3.setReceivedDateTermGT(new Date());
    pop3.setReceivedDateTermBetween(new Date(0), new Date());

    assertNull(pop3.getSearchTerm(), "POP3 doesn't support date searches");
  }

  @Test
  void receivedDateTermsApplyForImap() {
    conn.setReceivedDateTermEQ(new Date());
    assertInstanceOf(ReceivedDateTerm.class, conn.getSearchTerm());
  }

  @Test
  void receivedDateTermBetweenForImap() {
    conn.setReceivedDateTermBetween(new Date(0), new Date());
    assertInstanceOf(AndTerm.class, conn.getSearchTerm());
  }

  @Test
  void flagSettersAccumulate() {
    conn.setFlagTermNew();
    conn.setFlagTermOld();
    conn.setFlagTermRead();
    conn.setFlagTermUnread();
    conn.setFlagTermFlagged();
    conn.setFlagTermNotFlagged();
    conn.setFlagTermDraft();
    conn.setFlagTermNotDraft();

    assertInstanceOf(AndTerm.class, conn.getSearchTerm());
  }

  @Test
  void singleFlagSetterAddsFlagTerm() {
    conn.setFlagTermFlagged();
    assertInstanceOf(FlagTerm.class, conn.getSearchTerm());
  }

  @Test
  void clearFiltersResetsSearchAndCounters() {
    conn.setSenderTerm("a@x.com", false);
    conn.updateSavedMessagesCounter();
    conn.updateSavedAttachedFilesCounter();

    conn.clearFilters();

    assertNull(conn.getSearchTerm());
    assertEquals(0, conn.getSavedMessagesCounter());
    assertEquals(0, conn.getSavedAttachedFilesCounter());
    assertEquals(0, conn.getDeletedMessagesCounter());
    assertEquals(0, conn.getMovedMessagesCounter());
  }

  @Test
  void countersIncrementIndependently() {
    conn.updateSavedMessagesCounter();
    conn.updateSavedMessagesCounter();
    conn.updateSavedAttachedFilesCounter();

    assertEquals(2, conn.getSavedMessagesCounter());
    assertEquals(1, conn.getSavedAttachedFilesCounter());
  }

  @Test
  void isMessageDraftFlaggedNewReadDeletedDelegateToFlags() throws MessagingException {
    Message msg = mock(Message.class);
    when(msg.isSet(Flags.Flag.DRAFT)).thenReturn(true);
    when(msg.isSet(Flags.Flag.FLAGGED)).thenReturn(true);
    when(msg.isSet(Flags.Flag.RECENT)).thenReturn(true);
    when(msg.isSet(Flags.Flag.SEEN)).thenReturn(true);
    when(msg.isSet(Flags.Flag.DELETED)).thenReturn(true);

    assertTrue(conn.isMessageDraft(msg));
    assertTrue(conn.isMessageFlagged(msg));
    assertTrue(conn.isMessageNew(msg));
    assertTrue(conn.isMessageRead(msg));
    assertTrue(conn.isMessageDeleted(msg));
  }

  @Test
  void isMessageFlagSwallowsMessagingException() throws MessagingException {
    Message msg = mock(Message.class);
    when(msg.isSet(Flags.Flag.DRAFT)).thenThrow(new MessagingException());
    when(msg.isSet(Flags.Flag.FLAGGED)).thenThrow(new MessagingException());
    when(msg.isSet(Flags.Flag.RECENT)).thenThrow(new MessagingException());
    when(msg.isSet(Flags.Flag.SEEN)).thenThrow(new MessagingException());
    when(msg.isSet(Flags.Flag.DELETED)).thenThrow(new MessagingException());

    assertFalse(conn.isMessageDraft(msg));
    assertFalse(conn.isMessageFlagged(msg));
    assertFalse(conn.isMessageNew(msg));
    assertFalse(conn.isMessageRead(msg));
    assertFalse(conn.isMessageDeleted(msg));
  }
}
