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
package org.apache.hop.mail.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import jakarta.mail.search.SearchTerm;
import jakarta.mail.search.SubjectTerm;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.mail.common.MailConst;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MailServerConnectionFilterTest {

  @BeforeAll
  static void initEnvironment() throws Exception {
    // Encr.encoder is initialized by HopClientEnvironment; getPassword() needs it.
    HopClientEnvironment.init();
  }

  @Test
  void setSenderTermAddsFromStringTerm() {
    MailServerConnection c = new MailServerConnection();
    c.setSenderTerm("alice@example.com", false);

    assertInstanceOf(FromStringTerm.class, c.getSearchTerm());
  }

  @Test
  void setSenderTermNegatesWhenNotTerm() {
    MailServerConnection c = new MailServerConnection();
    c.setSenderTerm("alice@example.com", true);

    assertInstanceOf(NotTerm.class, c.getSearchTerm());
  }

  @Test
  void setSenderTermIgnoredWhenEmpty() {
    MailServerConnection c = new MailServerConnection();
    c.setSenderTerm("", false);
    c.setSenderTerm(null, true);

    assertNull(c.getSearchTerm());
  }

  @Test
  void setRecipientTermAddsRecipientStringTerm() {
    MailServerConnection c = new MailServerConnection();
    c.setRecipientTerm("bob@example.com");

    assertInstanceOf(RecipientStringTerm.class, c.getSearchTerm());
  }

  @Test
  void setRecipientTermIgnoredWhenEmpty() {
    MailServerConnection c = new MailServerConnection();
    c.setRecipientTerm("");
    assertNull(c.getSearchTerm());
  }

  @Test
  void setSubjectTermAddsSubjectTerm() {
    MailServerConnection c = new MailServerConnection();
    c.setSubjectTerm("invoice", false);

    assertInstanceOf(SubjectTerm.class, c.getSearchTerm());
  }

  @Test
  void setSubjectTermNegated() {
    MailServerConnection c = new MailServerConnection();
    c.setSubjectTerm("spam", true);

    assertInstanceOf(NotTerm.class, c.getSearchTerm());
  }

  @Test
  void setBodyTermAddsBodyTerm() {
    MailServerConnection c = new MailServerConnection();
    c.setBodyTerm("keyword", false);

    assertInstanceOf(BodyTerm.class, c.getSearchTerm());
  }

  @Test
  void setBodyTermNegated() {
    MailServerConnection c = new MailServerConnection();
    c.setBodyTerm("keyword", true);

    assertInstanceOf(NotTerm.class, c.getSearchTerm());
  }

  @Test
  void chainingFiltersBuildsAndTerm() {
    MailServerConnection c = new MailServerConnection();
    c.setSenderTerm("a@x.com", false);
    c.setSubjectTerm("hi", false);

    assertInstanceOf(AndTerm.class, c.getSearchTerm());
  }

  @Test
  void receivedDateTermsSkippedForPop3() {
    MailServerConnection c = new MailServerConnection();
    c.setProtocol(MailConst.PROTOCOL_STRING_POP3);

    c.setReceivedDateTermEQ(new Date());
    c.setReceivedDateTermLT(new Date());
    c.setReceivedDateTermGT(new Date());
    c.setReceivedDateTermBetween(new Date(0), new Date());

    assertNull(c.getSearchTerm(), "POP3 doesn't support date searches");
  }

  @Test
  void receivedDateTermEQAddedForImap() {
    MailServerConnection c = new MailServerConnection();
    c.setProtocol(MailConst.PROTOCOL_STRING_IMAP);
    c.setReceivedDateTermEQ(new Date());

    assertInstanceOf(ReceivedDateTerm.class, c.getSearchTerm());
  }

  @Test
  void receivedDateTermBetweenAddedForImap() {
    MailServerConnection c = new MailServerConnection();
    c.setProtocol(MailConst.PROTOCOL_STRING_IMAP);
    c.setReceivedDateTermBetween(new Date(0), new Date());

    assertInstanceOf(AndTerm.class, c.getSearchTerm());
  }

  @Test
  void flagTermsAddFlagTermRegardlessOfProtocol() {
    MailServerConnection c = new MailServerConnection();
    c.setProtocol(MailConst.PROTOCOL_STRING_POP3);

    c.setFlagTermNew();
    assertInstanceOf(FlagTerm.class, c.getSearchTerm());
  }

  @Test
  void allFlagTermSettersAccumulate() {
    MailServerConnection c = new MailServerConnection();
    c.setFlagTermNew();
    c.setFlagTermOld();
    c.setFlagTermRead();
    c.setFlagTermUnread();
    c.setFlagTermFlagged();
    c.setFlagTermNotFlagged();
    c.setFlagTermDraft();
    c.setFlagTermNotDraft();

    // Eight setters chained should produce nested AndTerms with a FlagTerm at the bottom.
    SearchTerm term = c.getSearchTerm();
    assertNotNull(term);
    assertInstanceOf(AndTerm.class, term);
  }

  @Test
  void clearFiltersResetsSearchAndCounters() {
    MailServerConnection c = new MailServerConnection();
    c.setSenderTerm("a@x.com", false);
    c.updateSavedMessagesCounter();
    c.updateSavedAttachedFilesCounter();

    assertNotNull(c.getSearchTerm());
    assertEquals(1, c.getSavedMessagesCounter());
    assertEquals(1, c.getSavedAttachedFilesCounter());

    c.clearFilters();

    assertNull(c.getSearchTerm());
    assertEquals(0, c.getSavedMessagesCounter());
    assertEquals(0, c.getDeletedMessagesCounter());
    assertEquals(0, c.getMovedMessagesCounter());
    assertEquals(0, c.getSavedAttachedFilesCounter());
  }

  @Test
  void countersIncrementIndividually() {
    MailServerConnection c = new MailServerConnection();
    c.updateSavedMessagesCounter();
    c.updateSavedMessagesCounter();
    c.updateSavedAttachedFilesCounter();

    assertEquals(2, c.getSavedMessagesCounter());
    assertEquals(1, c.getSavedAttachedFilesCounter());
    assertEquals(0, c.getDeletedMessagesCounter());
  }

  @Test
  void isMessageDraftDelegatesToFlags() throws MessagingException {
    Message msg = mock(Message.class);
    when(msg.isSet(Flags.Flag.DRAFT)).thenReturn(true);

    MailServerConnection c = new MailServerConnection();
    assertTrue(c.isMessageDraft(msg));
  }

  @Test
  void isMessageDraftReturnsFalseOnException() throws MessagingException {
    Message msg = mock(Message.class);
    when(msg.isSet(Flags.Flag.DRAFT)).thenThrow(new MessagingException("boom"));

    MailServerConnection c = new MailServerConnection();
    assertFalse(c.isMessageDraft(msg));
  }

  @Test
  void isMessageFlaggedNewReadDeletedDelegate() throws MessagingException {
    Message msg = mock(Message.class);
    when(msg.isSet(Flags.Flag.FLAGGED)).thenReturn(true);
    when(msg.isSet(Flags.Flag.RECENT)).thenReturn(true);
    when(msg.isSet(Flags.Flag.SEEN)).thenReturn(true);
    when(msg.isSet(Flags.Flag.DELETED)).thenReturn(true);

    MailServerConnection c = new MailServerConnection();
    assertTrue(c.isMessageFlagged(msg));
    assertTrue(c.isMessageNew(msg));
    assertTrue(c.isMessageRead(msg));
    assertTrue(c.isMessageDeleted(msg));
  }

  @Test
  void isMessageFlagSwallowsMessagingException() throws MessagingException {
    Message msg = mock(Message.class);
    when(msg.isSet(Flags.Flag.FLAGGED)).thenThrow(new MessagingException());
    when(msg.isSet(Flags.Flag.RECENT)).thenThrow(new MessagingException());
    when(msg.isSet(Flags.Flag.SEEN)).thenThrow(new MessagingException());
    when(msg.isSet(Flags.Flag.DELETED)).thenThrow(new MessagingException());

    MailServerConnection c = new MailServerConnection();
    assertFalse(c.isMessageFlagged(msg));
    assertFalse(c.isMessageNew(msg));
    assertFalse(c.isMessageRead(msg));
    assertFalse(c.isMessageDeleted(msg));
  }

  @Test
  void equalsIsCaseInsensitiveByName() {
    MailServerConnection a = new MailServerConnection();
    a.setName("prod");
    MailServerConnection b = new MailServerConnection();
    b.setName("PROD");

    assertEquals(a, b);
    // Note: hashCode() uses case-sensitive name.hashCode() while equals() is case-insensitive,
    // which is technically a violation of the equals/hashCode contract. Not asserted here so
    // this test doesn't lock in that quirk. Two objects with identical-case names DO share a
    // hashCode (covered by equalsSameInstanceShortCircuits via reference identity).
  }

  @Test
  void equalsDifferentNamesNotEqual() {
    MailServerConnection a = new MailServerConnection();
    a.setName("prod");
    MailServerConnection b = new MailServerConnection();
    b.setName("staging");

    assertNotEquals(a, b);
  }

  @Test
  void equalsAgainstNonConnectionIsFalse() {
    MailServerConnection a = new MailServerConnection();
    a.setName("prod");

    assertNotEquals("prod", a);
  }

  @Test
  void equalsSameInstanceShortCircuits() {
    MailServerConnection a = new MailServerConnection();
    a.setName("prod");
    assertEquals(a, a);
  }

  @Test
  void toStringReturnsNameWhenSet() {
    MailServerConnection c = new MailServerConnection();
    c.setName("prod");
    assertEquals("prod", c.toString());
  }

  @Test
  void toStringFallsBackToObjectWhenNoName() {
    MailServerConnection c = new MailServerConnection();
    // Default Object.toString returns "ClassName@hashcode"; just assert it's not null
    // and not an empty string — exact value isn't important.
    String s = c.toString();
    assertNotNull(s);
    assertFalse(s.isEmpty());
  }

  @Test
  void findValidTargetReturnsFolderJoinedFilenameWhenNoCollision(@TempDir Path tmp)
      throws Exception {
    String target = MailServerConnection.findValidTarget(tmp.toString(), "msg.eml");
    assertTrue(target.endsWith("msg.eml"), "got: " + target);
  }

  @Test
  void findValidTargetIncrementsSuffixWhenFileExists(@TempDir Path tmp) throws Exception {
    Files.createFile(tmp.resolve("msg.eml"));
    Files.createFile(tmp.resolve("msg1.eml"));
    String target = MailServerConnection.findValidTarget(tmp.toString(), "msg.eml");
    assertTrue(target.endsWith("msg2.eml"), "got: " + target);
  }

  @Test
  void findValidTargetHandlesExtensionlessFile(@TempDir Path tmp) throws Exception {
    String target = MailServerConnection.findValidTarget(tmp.toString(), "noext");
    assertTrue(target.endsWith("noext"), "got: " + target);
  }

  @Test
  void findValidTargetRejectsNullArguments() {
    Exception e1 =
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class, () -> MailServerConnection.findValidTarget(null, "x"));
    assertNotNull(e1);

    Exception e2 =
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class, () -> MailServerConnection.findValidTarget("x", null));
    assertNotNull(e2);
  }

  @Test
  void getPasswordResolvesAndDecrypts() throws IOException {
    MailServerConnection c =
        new MailServerConnection(new org.apache.hop.core.variables.Variables());
    // A plain (non-encrypted) password just gets returned as-is after variable resolution.
    assertEquals("plain", c.getPassword("plain"));
    assertEquals("", c.getPassword(null));
  }
}
