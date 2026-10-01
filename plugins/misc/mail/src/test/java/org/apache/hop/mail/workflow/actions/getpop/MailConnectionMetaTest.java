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

import org.apache.hop.core.HopClientEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Unit test for {@link MailConnectionMeta} */
class MailConnectionMetaTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void parseCodeOrIntAcceptsBlankUnknownAndOutOfRange() {
    assertEquals(0, MailConnectionMeta.parseCodeOrInt(null, MailConnectionMeta.conditionDateCode));
    assertEquals(0, MailConnectionMeta.parseCodeOrInt("  ", MailConnectionMeta.conditionDateCode));
    assertEquals(
        0, MailConnectionMeta.parseCodeOrInt("nope", MailConnectionMeta.conditionDateCode));
    assertEquals(0, MailConnectionMeta.parseCodeOrInt("99", MailConnectionMeta.conditionDateCode));
    assertEquals(0, MailConnectionMeta.parseCodeOrInt("-1", MailConnectionMeta.conditionDateCode));
  }

  @Test
  void conditionDateCodesAndIndexesRoundTrip() {
    assertEquals(
        MailConnectionMeta.CONDITION_DATE_IGNORE,
        MailConnectionMeta.getConditionDateByCode("ignore"));
    assertEquals(
        MailConnectionMeta.CONDITION_DATE_EQUAL,
        MailConnectionMeta.getConditionDateByCode("equal"));
    assertEquals(
        MailConnectionMeta.CONDITION_DATE_BETWEEN,
        MailConnectionMeta.getConditionByCode("between"));
    assertEquals(
        MailConnectionMeta.CONDITION_DATE_GREATER, MailConnectionMeta.getConditionDateByCode("3"));
    assertEquals("ignore", MailConnectionMeta.getConditionDateCode(0));
    assertEquals("ignore", MailConnectionMeta.getConditionDateCode(-1));
    assertEquals("ignore", MailConnectionMeta.getConditionDateCode(99));
  }

  @Test
  void imapListCodesAndIndexesRoundTrip() {
    assertEquals(
        MailConnectionMeta.VALUE_IMAP_LIST_UNREAD,
        MailConnectionMeta.getValueImapListByCode("imaplistunread"));
    assertEquals(
        MailConnectionMeta.VALUE_IMAP_LIST_UNREAD, MailConnectionMeta.getValueImapListByCode("4"));
    assertEquals(
        MailConnectionMeta.VALUE_IMAP_LIST_UNREAD,
        MailConnectionMeta.getValueListImapListByCode("imaplistunread"));
    assertEquals("imaplistall", MailConnectionMeta.getValueImapListCode(-5));
    assertEquals(
        "imaplistunread",
        MailConnectionMeta.getValueImapListCode(MailConnectionMeta.VALUE_IMAP_LIST_UNREAD));
  }

  @Test
  void actionTypeAndAfterGetCodesRoundTrip() {
    assertEquals(
        MailConnectionMeta.ACTION_TYPE_MOVE, MailConnectionMeta.getActionTypeByCode("move"));
    assertEquals(
        MailConnectionMeta.ACTION_TYPE_DELETE, MailConnectionMeta.getActionTypeByCode("2"));
    assertEquals(
        MailConnectionMeta.AFTER_GET_IMAP_MOVE, MailConnectionMeta.getAfterGetIMAPByCode("move"));
    assertEquals(
        MailConnectionMeta.AFTER_GET_IMAP_DELETE, MailConnectionMeta.getAfterGetIMAPByCode("1"));
    assertEquals("get", MailConnectionMeta.getActionTypeCode(99));
    assertEquals("nothing", MailConnectionMeta.getAfterGetIMAPCode(-1));
  }

  @Test
  void descLookupsFallBackToCode() {
    assertEquals(
        MailConnectionMeta.VALUE_IMAP_LIST_NEW,
        MailConnectionMeta.getValueImapListByDesc("imaplistnew"));
    assertEquals(
        MailConnectionMeta.CONDITION_DATE_SMALLER,
        MailConnectionMeta.getConditionDateByDesc("smaller"));
    assertEquals(MailConnectionMeta.ACTION_TYPE_GET, MailConnectionMeta.getActionTypeByDesc("get"));
    assertEquals(
        MailConnectionMeta.AFTER_GET_IMAP_NOTHING,
        MailConnectionMeta.getAfterGetIMAPByDesc("nothing"));
    assertEquals(0, MailConnectionMeta.getValueImapListByDesc(null));
    assertEquals(0, MailConnectionMeta.getConditionDateByDesc(" "));
    assertEquals(0, MailConnectionMeta.getActionTypeByDesc(null));
    assertEquals(0, MailConnectionMeta.getAfterGetIMAPByDesc(null));
  }

  @Test
  void descByIndexFallsBackToFirstEntry() {
    assertEquals(
        MailConnectionMeta.getValueImapListDesc(0), MailConnectionMeta.getValueImapListDesc(-1));
    assertEquals(
        MailConnectionMeta.getConditionDateDesc(0), MailConnectionMeta.getConditionDateDesc(50));
    assertEquals(MailConnectionMeta.getActionTypeDesc(0), MailConnectionMeta.getActionTypeDesc(-2));
    assertEquals(
        MailConnectionMeta.getAfterGetIMAPDesc(0), MailConnectionMeta.getAfterGetIMAPDesc(9));
  }

  @Test
  void protocolFromString() {
    assertEquals(
        MailConnectionMeta.PROTOCOL_IMAP,
        MailConnectionMeta.getProtocolFromString("IMAP", MailConnectionMeta.PROTOCOL_POP3));
    assertEquals(
        MailConnectionMeta.PROTOCOL_POP3,
        MailConnectionMeta.getProtocolFromString("pop3", MailConnectionMeta.PROTOCOL_IMAP));
    assertEquals(
        MailConnectionMeta.PROTOCOL_MBOX,
        MailConnectionMeta.getProtocolFromString("MBOX", MailConnectionMeta.PROTOCOL_POP3));
    assertEquals(
        MailConnectionMeta.PROTOCOL_POP3,
        MailConnectionMeta.getProtocolFromString(null, MailConnectionMeta.PROTOCOL_POP3));
    assertEquals(
        MailConnectionMeta.PROTOCOL_IMAP,
        MailConnectionMeta.getProtocolFromString("nntp", MailConnectionMeta.PROTOCOL_IMAP));
  }

  @Test
  void convertersMatchStaticHelpers() {
    MailConnectionMeta.ValueImapListConverter imap =
        new MailConnectionMeta.ValueImapListConverter();
    assertEquals("imaplistunread", imap.getCode(MailConnectionMeta.VALUE_IMAP_LIST_UNREAD));
    assertEquals(MailConnectionMeta.VALUE_IMAP_LIST_UNREAD, imap.getType("imaplistunread"));
    assertEquals(MailConnectionMeta.VALUE_IMAP_LIST_UNREAD, imap.getType("4"));

    MailConnectionMeta.ConditionDateConverter date =
        new MailConnectionMeta.ConditionDateConverter();
    assertEquals("ignore", date.getCode(0));
    assertEquals(MailConnectionMeta.CONDITION_DATE_IGNORE, date.getType("ignore"));

    MailConnectionMeta.ActionTypeConverter action = new MailConnectionMeta.ActionTypeConverter();
    assertEquals("delete", action.getCode(MailConnectionMeta.ACTION_TYPE_DELETE));
    assertEquals(MailConnectionMeta.ACTION_TYPE_MOVE, action.getType("move"));

    MailConnectionMeta.AfterGetImapConverter after = new MailConnectionMeta.AfterGetImapConverter();
    assertEquals("nothing", after.getCode(0));
    assertEquals(MailConnectionMeta.AFTER_GET_IMAP_DELETE, after.getType("delete"));
  }

  @Test
  void descLookupUsesDisplayedLabel() {
    String unreadLabel =
        MailConnectionMeta.getValueImapListDesc(MailConnectionMeta.VALUE_IMAP_LIST_UNREAD);
    assertEquals(
        MailConnectionMeta.VALUE_IMAP_LIST_UNREAD,
        MailConnectionMeta.getValueImapListByDesc(unreadLabel));

    String ignoreLabel =
        MailConnectionMeta.getConditionDateDesc(MailConnectionMeta.CONDITION_DATE_IGNORE);
    assertEquals(
        MailConnectionMeta.CONDITION_DATE_IGNORE,
        MailConnectionMeta.getConditionDateByDesc(ignoreLabel));
  }
}
