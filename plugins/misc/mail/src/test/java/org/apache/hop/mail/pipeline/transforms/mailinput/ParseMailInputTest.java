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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
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
import org.junit.jupiter.api.Disabled;
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

  // this objects re-created for every test method
  private Message message;
  private MailInputData data;
  private MailInputMeta meta;
  private MailInput mailInput;

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
    message = Mockito.mock(Message.class);

    MailConnection conn = mock(MailConnection.class);
    when(conn.getMessageBody(any(Message.class))).thenReturn(MSG_BODY);
    when(conn.getFolderName()).thenReturn(FLD_NAME);
    when(conn.getAttachedFilesCount(any(Message.class), any(Pattern.class)))
        .thenReturn(ATTCH_COUNT);
    when(conn.getMessageBodyContentType(any(Message.class))).thenReturn(CNTNT_TYPE);
    data = mock(MailInputData.class);
    data.mailConn = conn;

    mailInput =
        new MailInput(
            transformMockHelper.transformMeta,
            meta,
            data,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

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

    // for previous implementation
    when(message.getHeader(HDR_EX1)).thenReturn(new String[] {ex1.getValue()});
    when(message.getHeader(HDR_EX2)).thenReturn(new String[] {ex2.getValue()});
  }

  /**
   * When mail header is found returns his actual value.
   *
   * @throws Exception
   * @throws HopException
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testHeadersParsedPositive() throws Exception {
    // add expected fields:
    int[] fields = {MailInputField.COLUMN_HEADER};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    // points to existed header
    farr.get(0).setName(HDR_EX1);

    this.mockMailInputMeta(farr);

    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    Assertions.assertEquals(HDR_EX1V, String.class.cast(r[0]), "Header is correct");
  }

  /**
   * When mail header is not found returns empty String
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testHeadersParsedNegative() throws Exception {
    int[] fields = {MailInputField.COLUMN_HEADER};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    farr.get(0).setName(HDR_EX1 + "salt");

    this.mockMailInputMeta(farr);

    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    Assertions.assertEquals("", String.class.cast(r[0]), "Header is correct");
  }

  /**
   * Test, message number can be parsed correctly
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageNumberIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_MESSAGE_NR};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);
    Assertions.assertEquals(
        Long.valueOf(MSG_NUMB), Long.class.cast(r[0]), "Message number is correct");
  }

  /**
   * Test message subject can be parsed
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageSubjectIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_SUBJECT};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);
    Assertions.assertEquals(SUBJ, String.class.cast(r[0]), "Message subject is correct");
  }

  /**
   * Test message From can be parsed correctly
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageFromIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_SENDER};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    // expect, that from is concatenated with ';'
    String expected = StringUtils.join(new String[] {FROM1, FROM2}, ";");
    Assertions.assertEquals(expected, String.class.cast(r[0]), "Message From is correct");
  }

  /**
   * Test message ReplayTo can be parsed correctly
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageReplayToIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_REPLY_TO};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    // is concatenated with ';'
    String expected = StringUtils.join(new String[] {REP1, REP2}, ";");
    Assertions.assertEquals(expected, String.class.cast(r[0]), "Message ReplayTo is correct");
  }

  /**
   * Test message recipients can be parsed
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageRecipientsIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_RECIPIENTS};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    // is concatenated with ';'
    String expected = StringUtils.join(new String[] {REC1, REC2}, ";");
    Assertions.assertEquals(expected, String.class.cast(r[0]), "Message Recipients is correct");
  }

  /**
   * Test message description is correct
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageDescriptionIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_DESCRIPTION};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    Assertions.assertEquals(DESC, String.class.cast(r[0]), "Message Description is correct");
  }

  /**
   * Test message received date is correct
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageRecivedDateIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_RECEIVED_DATE};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    Assertions.assertEquals(DATE1, Date.class.cast(r[0]), "Message Recived date is correct");
  }

  /**
   * Test message sent date is correct
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageSentDateIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_SENT_DATE};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    Assertions.assertEquals(DATE2, Date.class.cast(r[0]), "Message Sent date is correct");
  }

  /**
   * Message content type is correct
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageContentTypeIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_CONTENT_TYPE};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    Assertions.assertEquals(
        CNTNT_TYPE_EMAIL, String.class.cast(r[0]), "Message Content type is correct");
  }

  /**
   * Test message size is correct
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageSizeIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_SIZE};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    Assertions.assertEquals(
        Long.valueOf(CNTNT_SIZE), Long.class.cast(r[0]), "Message Size is correct");
  }

  /**
   * Test that message body can be parsed correctly
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageBodyIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_BODY};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    Assertions.assertEquals(MSG_BODY, String.class.cast(r[0]), "Message Body is correct");
  }

  /**
   * Test that message folder name can be parsed correctly
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageFolderNameIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_FOLDER_NAME};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    Assertions.assertEquals(FLD_NAME, String.class.cast(r[0]), "Message Folder Name is correct");
  }

  /**
   * Test that message folder name can be parsed correctly
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageAttachedFilesCountNameIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_ATTACHED_FILES_COUNT};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    Assertions.assertEquals(
        Long.valueOf(ATTCH_COUNT),
        Long.class.cast(r[0]),
        "Message Attached files count is correct");
  }

  /**
   * Test that message body content type can be parsed correctly
   *
   * @throws Exception
   */
  @Test
  @Disabled("This test needs to be reviewed")
  void testMessageBodyContentTypeIsParsed() throws Exception {
    int[] fields = {MailInputField.COLUMN_BODY_CONTENT_TYPE};
    List<MailInputField> farr = this.getDefaultInputFields(fields);
    this.mockMailInputMeta(farr);
    try {
      mailInput.init();
    } catch (Exception e) {
      // don't worry about it
    }
    MessageParser underTest = mailInput.new MessageParser();
    Object[] r = RowDataUtil.allocateRowData(data.nrFields);
    underTest.parseToArray(r, message);

    Assertions.assertEquals(
        CNTNT_TYPE, String.class.cast(r[0]), "Message body content type is correct");
  }

  private void mockMailInputMeta(List<MailInputField> arr) {
    data.nrFields = arr.size();
    meta = mock(MailInputMeta.class);
    when(meta.getInputFields()).thenReturn(arr);
  }

  private List<MailInputField> getDefaultInputFields(int[] arr) {
    List<MailInputField> fields = new ArrayList<MailInputField>();
    for (int i = 0; i < arr.length; i++) {
      MailInputField field = new MailInputField();
      field.setColumn(arr[i]);
      field.setName(MailInputField.getColumnDesc(arr[i]));
      fields.add(field);
    }
    return fields;
  }

  private Enumeration<Header> getEnum(Header[] headers) {
    return Collections.enumeration(Arrays.asList(headers));
  }
}
