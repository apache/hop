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

package org.apache.hop.pipeline.transforms.webservices;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WebServiceTest {

  private static final String LOCATION_HEADER = "Location";

  private static final String TEST_URL = "TEST_URL";

  private static final String NOT_VALID_URL = "NOT VALID URL";

  private TransformMockHelper<WebServiceMeta, WebServiceData> mockHelper;

  private WebService webServiceTransform;

  @BeforeAll
  static void initEnvironment() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUpBefore() {
    mockHelper =
        new TransformMockHelper<>("WebService", WebServiceMeta.class, WebServiceData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);

    webServiceTransform =
        spy(
            new WebService(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                mockHelper.iTransformData,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));
  }

  @AfterEach
  void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  void newHttpMethodWithInvalidUrl() {
    assertThrows(URISyntaxException.class, () -> webServiceTransform.getHttpMethod(NOT_VALID_URL));
  }

  @Test
  void getLocationFrom() {
    HttpPost postMethod = mock(HttpPost.class);
    Header locationHeader = new BasicHeader(LOCATION_HEADER, TEST_URL);
    doReturn(locationHeader).when(postMethod).getFirstHeader(LOCATION_HEADER);

    assertEquals(TEST_URL, WebService.getLocationFrom(postMethod));
  }

  /** A SOAP reply of the sayHello operation. */
  private static final String REPLY =
      """
      <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" \
      xmlns:tns="http://example.com/hello">
        <soapenv:Body>
          <tns:sayHelloResponse>
            <greeting>Hello, Hop</greeting>
          </tns:sayHelloResponse>
        </soapenv:Body>
      </soapenv:Envelope>
      """;

  /** An input row, over-allocated like the rows the transforms before this one send. */
  private static Object[] inputRow() {
    Object[] row = RowDataUtil.allocateRowData(2);
    row[0] = "Hop";
    row[1] = "some other input";
    return row;
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("firstName"));
    rowMeta.addValueMeta(new ValueMetaString("other"));
    return rowMeta;
  }

  private static WebServiceField outputField(String name, String wsName) {
    WebServiceField field = new WebServiceField();
    field.setName(name);
    field.setWsName(wsName);
    field.setXsdType("string");
    return field;
  }

  /**
   * Feed {@link #REPLY} to the reply parsing of a transform with this configuration, the way
   * processRow() sets it up, and return the rows it sends on.
   */
  private List<Object[]> processReply(WebServiceMeta meta) throws Exception {
    meta.setCompatible(false);
    WebServiceData data = new WebServiceData();
    IRowMeta outputRowMeta = inputRowMeta().clone();
    meta.getFields(outputRowMeta, "Web services lookup", null, null, new Variables(), null);
    data.outputRowMeta = outputRowMeta;

    WebService transform =
        spy(
            new WebService(
                mockHelper.transformMeta,
                meta,
                data,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));
    transform.setInputRowMeta(inputRowMeta());
    List<Object[]> rows = new ArrayList<>();
    doAnswer(
            invocation -> {
              rows.add(invocation.getArgument(1));
              return null;
            })
        .when(transform)
        .putRow(any(), any());

    transform.processRows(
        new ByteArrayInputStream(REPLY.getBytes(StandardCharsets.UTF_8)),
        inputRow(),
        inputRowMeta(),
        false,
        StandardCharsets.UTF_8.name());
    return rows;
  }

  @Test
  void replyAsStringWithoutInputDataGoesInTheOutputField() throws Exception {
    WebServiceMeta meta = new WebServiceMeta();
    meta.setReturningReplyAsString(true);
    meta.setPassingInputData(false);
    meta.getFieldsOut().add(outputField("reply", "sayHelloResponse"));

    List<Object[]> rows = processReply(meta);

    assertEquals(1, rows.size());
    assertEquals(REPLY, rows.get(0)[0]);
  }

  @Test
  void replyAsStringWithInputDataGoesAfterTheInputFields() throws Exception {
    WebServiceMeta meta = new WebServiceMeta();
    meta.setReturningReplyAsString(true);
    meta.setPassingInputData(true);
    meta.getFieldsOut().add(outputField("reply", "sayHelloResponse"));

    List<Object[]> rows = processReply(meta);

    assertEquals(1, rows.size());
    assertEquals("Hop", rows.get(0)[0]);
    assertEquals("some other input", rows.get(0)[1]);
    assertEquals(REPLY, rows.get(0)[2]);
  }

  @Test
  void replyAsStringWithoutInputDataLeaksNoInputValues() throws Exception {
    WebServiceMeta meta = new WebServiceMeta();
    meta.setReturningReplyAsString(true);
    meta.setPassingInputData(false);
    meta.getFieldsOut().add(outputField("reply", "sayHelloResponse"));
    meta.getFieldsOut().add(outputField("unused", "unused"));

    List<Object[]> rows = processReply(meta);

    assertEquals(1, rows.size());
    assertEquals(REPLY, rows.get(0)[0]);
    assertNull(rows.get(0)[1]);
  }

  @Test
  void replyAsStringWithoutOutputFieldStillSendsTheRow() throws Exception {
    WebServiceMeta meta = new WebServiceMeta();
    meta.setReturningReplyAsString(true);
    meta.setPassingInputData(true);

    List<Object[]> rows = processReply(meta);

    assertEquals(1, rows.size());
    assertEquals("Hop", rows.get(0)[0]);
    assertEquals("some other input", rows.get(0)[1]);
  }

  @Test
  void responseNodeAsXmlWithoutInputDataGoesInTheOutputField() throws Exception {
    WebServiceMeta meta = new WebServiceMeta();
    meta.setPassingInputData(false);
    meta.getFieldsOut().add(outputField("response", "tns:sayHelloResponse"));

    List<Object[]> rows = processReply(meta);

    assertEquals(1, rows.size());
    String xml = (String) rows.get(0)[0];
    assertTrue(xml.contains("sayHelloResponse") && xml.contains("Hello, Hop"), xml);
  }
}
