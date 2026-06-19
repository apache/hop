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

import jakarta.mail.Address;
import jakarta.mail.Header;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.mail.pipeline.transforms.mailinput.MailInput.MessageParser;
import org.apache.hop.mail.workflow.actions.getpop.MailConnection;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalMatchers;
import org.mockito.Mockito;

class ParseMailInputTest {

  // mock is existed per-class instance loaded by junit loader
  private static TransformMockHelper<MailInputMeta, ITransformData> transformMockHelper;

  // test data
  public static final int MSG_NUMB = 3;
  public static final String MSG_BODY = "msg_body";
  public static final String FLD_NAME = "junit_folder";
  public static final int ATTCH_COUNT = 3;
  public static final String CNTNT_TYPE = "text/html";
  public static final String FROM1 = "localhost_1";
  public static final String FROM2 = "localhost_2";
  public static final String REP1 = "127.0.0.1";
  public static final String REP2 = "127.0.0.2";
  public static final String REC1 = "Vasily";
  public static final String REC2 = "Pupkin";
  public static final String SUBJ = "mocktest";
  public static final String DESC = "desc";
  public static final Date DATE1 = new Date(0);
  public static final Date DATE2 = new Date(60000);
  public static final String CNTNT_TYPE_EMAIL = "application/acad";
  public static final int CNTNT_SIZE = 23;
  public static final String HDR_EX1 = "header_ex1";
  public static final String HDR_EX1V = "header_ex1_value";
  public static final String HDR_EX2 = "header_ex2";
  public static final String HDR_EX2V = "header_ex2_value";

  // re-created for every test method
  private Message message;
  private MailConnection mailConn;

  @BeforeAll
  static void setup() {
    transformMockHelper =
        new TransformMockHelper<>("ABORT TEST", MailInputMeta.class, ITransformData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterAll
  static void tearDown() {
    transformMockHelper.cleanUp();
  }

  @BeforeEach
  void beforeTest() throws MessagingException, IOException, HopException {
    mailConn = mock(MailConnection.class);
    when(mailConn.getMessageBody(any(Message.class))).thenReturn(MSG_BODY);
    when(mailConn.getFolderName()).thenReturn(FLD_NAME);
    when(mailConn.getAttachedFilesCount(any(Message.class), Mockito.nullable(Pattern.class)))
        .thenReturn(ATTCH_COUNT);
    when(mailConn.getMessageBodyContentType(any(Message.class))).thenReturn(CNTNT_TYPE);
    when(mailConn.isMessageDraft(any(Message.class))).thenReturn(true);
    when(mailConn.isMessageFlagged(any(Message.class))).thenReturn(true);
    when(mailConn.isMessageNew(any(Message.class))).thenReturn(true);
    when(mailConn.isMessageRead(any(Message.class))).thenReturn(true);
    when(mailConn.isMessageDeleted(any(Message.class))).thenReturn(true);

    Address addrFrom1 = mock(Address.class);
    when(addrFrom1.toString()).thenReturn(FROM1);
    Address addrFrom2 = mock(Address.class);
    when(addrFrom2.toString()).thenReturn(FROM2);
    Address addrRep1 = mock(Address.class);
    when(addrRep1.toString()).thenReturn(REP1);
    Address addrRep2 = mock(Address.class);
    when(addrRep2.toString()).thenReturn(REP2);
    Address allRec1 = mock(Address.class);
    when(allRec1.toString()).thenReturn(REC1);
    Address allRec2 = mock(Address.class);
    when(allRec2.toString()).thenReturn(REC2);

    Address[] adrFr = {addrFrom1, addrFrom2};
    Address[] adrRep = {addrRep1, addrRep2};
    Address[] adrRecip = {allRec1, allRec2};

    message = Mockito.mock(Message.class);
    when(message.getMessageNumber()).thenReturn(MSG_NUMB);
    when(message.getSubject()).thenReturn(SUBJ);

    when(message.getFrom()).thenReturn(adrFr);
    when(message.getReplyTo()).thenReturn(adrRep);
    when(message.getAllRecipients()).thenReturn(adrRecip);
    when(message.getDescription()).thenReturn(DESC);
    when(message.getReceivedDate()).thenReturn(DATE1);
    when(message.getSentDate()).thenReturn(DATE2);
    when(message.getContentType()).thenReturn(CNTNT_TYPE_EMAIL);
    when(message.getSize()).thenReturn(CNTNT_SIZE);

    Header ex1 = new Header(HDR_EX1, HDR_EX1V);
    Header ex2 = new Header(HDR_EX2, HDR_EX2V);

    when(message.getMatchingHeaders(AdditionalMatchers.aryEq(new String[] {HDR_EX1})))
        .thenReturn(getEnum(new Header[] {ex1}));
    when(message.getMatchingHeaders(AdditionalMatchers.aryEq(new String[] {HDR_EX2})))
        .thenReturn(getEnum(new Header[] {ex2}));
    when(message.getMatchingHeaders(AdditionalMatchers.aryEq(new String[] {HDR_EX1, HDR_EX2})))
        .thenReturn(getEnum(new Header[] {ex1, ex2}));

    when(message.getHeader(HDR_EX1)).thenReturn(new String[] {ex1.getValue()});
    when(message.getHeader(HDR_EX2)).thenReturn(new String[] {ex2.getValue()});
  }

  /** Build a MailInput whose meta/data references match what we'll assert against. */
  private MailInput buildMailInput(List<MailInputField> fields) {
    MailInputMeta meta = mock(MailInputMeta.class);
    when(meta.getInputFields()).thenReturn(fields);

    MailInputData data = new MailInputData();
    data.nrFields = fields.size();
    data.mailConn = mailConn;

    return new MailInput(
        transformMockHelper.transformMeta,
        meta,
        data,
        0,
        transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline);
  }

  /** Run parseToArray against a single-field input and return the value of that field. */
  private Object parseSingleField(MailInputField field) throws Exception {
    MailInput mailInput = buildMailInput(Collections.singletonList(field));
    MessageParser parser = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(1);
    parser.parseToArray(r, message);
    return r[0];
  }

  private MailInputField fieldForColumn(int column) {
    MailInputField field = new MailInputField();
    field.setColumn(column);
    field.setName(MailInputField.getColumnDesc(column));
    return field;
  }

  @Test
  void testHeadersParsedPositive() throws Exception {
    MailInputField field = fieldForColumn(MailInputField.COLUMN_HEADER);
    field.setName(HDR_EX1);
    Assertions.assertEquals(HDR_EX1V, parseSingleField(field), "Header is correct");
  }

  @Test
  void testHeadersParsedNegative() throws Exception {
    MailInputField field = fieldForColumn(MailInputField.COLUMN_HEADER);
    field.setName(HDR_EX1 + "salt");
    Assertions.assertEquals("", parseSingleField(field), "Missing header returns empty string");
  }

  @Test
  void testMessageNumberIsParsed() throws Exception {
    Assertions.assertEquals(
        Long.valueOf(MSG_NUMB),
        parseSingleField(fieldForColumn(MailInputField.COLUMN_MESSAGE_NR)),
        "Message number is correct");
  }

  @Test
  void testMessageSubjectIsParsed() throws Exception {
    Assertions.assertEquals(
        SUBJ,
        parseSingleField(fieldForColumn(MailInputField.COLUMN_SUBJECT)),
        "Message subject is correct");
  }

  @Test
  void testMessageFromIsParsed() throws Exception {
    String expected = StringUtils.join(new String[] {FROM1, FROM2}, ";");
    Assertions.assertEquals(
        expected,
        parseSingleField(fieldForColumn(MailInputField.COLUMN_SENDER)),
        "Message From is correct");
  }

  @Test
  void testMessageReplayToIsParsed() throws Exception {
    String expected = StringUtils.join(new String[] {REP1, REP2}, ";");
    Assertions.assertEquals(
        expected,
        parseSingleField(fieldForColumn(MailInputField.COLUMN_REPLY_TO)),
        "Message ReplayTo is correct");
  }

  @Test
  void testMessageRecipientsIsParsed() throws Exception {
    String expected = StringUtils.join(new String[] {REC1, REC2}, ";");
    Assertions.assertEquals(
        expected,
        parseSingleField(fieldForColumn(MailInputField.COLUMN_RECIPIENTS)),
        "Message Recipients is correct");
  }

  @Test
  void testMessageDescriptionIsParsed() throws Exception {
    Assertions.assertEquals(
        DESC,
        parseSingleField(fieldForColumn(MailInputField.COLUMN_DESCRIPTION)),
        "Message Description is correct");
  }

  @Test
  void testMessageRecivedDateIsParsed() throws Exception {
    Assertions.assertEquals(
        DATE1,
        parseSingleField(fieldForColumn(MailInputField.COLUMN_RECEIVED_DATE)),
        "Message Received date is correct");
  }

  @Test
  void testMessageSentDateIsParsed() throws Exception {
    Assertions.assertEquals(
        DATE2,
        parseSingleField(fieldForColumn(MailInputField.COLUMN_SENT_DATE)),
        "Message Sent date is correct");
  }

  @Test
  void testMessageContentTypeIsParsed() throws Exception {
    Assertions.assertEquals(
        CNTNT_TYPE_EMAIL,
        parseSingleField(fieldForColumn(MailInputField.COLUMN_CONTENT_TYPE)),
        "Message Content type is correct");
  }

  @Test
  void testMessageSizeIsParsed() throws Exception {
    Assertions.assertEquals(
        Long.valueOf(CNTNT_SIZE),
        parseSingleField(fieldForColumn(MailInputField.COLUMN_SIZE)),
        "Message Size is correct");
  }

  @Test
  void testMessageBodyIsParsed() throws Exception {
    Assertions.assertEquals(
        MSG_BODY,
        parseSingleField(fieldForColumn(MailInputField.COLUMN_BODY)),
        "Message Body is correct");
  }

  @Test
  void testMessageFolderNameIsParsed() throws Exception {
    Assertions.assertEquals(
        FLD_NAME,
        parseSingleField(fieldForColumn(MailInputField.COLUMN_FOLDER_NAME)),
        "Message Folder Name is correct");
  }

  @Test
  void testMessageAttachedFilesCountNameIsParsed() throws Exception {
    Assertions.assertEquals(
        Long.valueOf(ATTCH_COUNT),
        parseSingleField(fieldForColumn(MailInputField.COLUMN_ATTACHED_FILES_COUNT)),
        "Message Attached files count is correct");
  }

  @Test
  void testMessageBodyContentTypeIsParsed() throws Exception {
    Assertions.assertEquals(
        CNTNT_TYPE,
        parseSingleField(fieldForColumn(MailInputField.COLUMN_BODY_CONTENT_TYPE)),
        "Message body content type is correct");
  }

  @Test
  void testFlagDraftIsParsed() throws Exception {
    Assertions.assertEquals(
        Boolean.TRUE, parseSingleField(fieldForColumn(MailInputField.COLUMN_FLAG_DRAFT)));
  }

  @Test
  void testFlagFlaggedIsParsed() throws Exception {
    Assertions.assertEquals(
        Boolean.TRUE, parseSingleField(fieldForColumn(MailInputField.COLUMN_FLAG_FLAGGED)));
  }

  @Test
  void testFlagNewIsParsed() throws Exception {
    Assertions.assertEquals(
        Boolean.TRUE, parseSingleField(fieldForColumn(MailInputField.COLUMN_FLAG_NEW)));
  }

  @Test
  void testFlagReadIsParsed() throws Exception {
    Assertions.assertEquals(
        Boolean.TRUE, parseSingleField(fieldForColumn(MailInputField.COLUMN_FLAG_READ)));
  }

  @Test
  void testFlagDeletedIsParsed() throws Exception {
    Assertions.assertEquals(
        Boolean.TRUE, parseSingleField(fieldForColumn(MailInputField.COLUMN_FLAG_DELETED)));
  }

  private Enumeration<Header> getEnum(Header[] headers) {
    return Collections.enumeration(Arrays.asList(headers));
  }
}
