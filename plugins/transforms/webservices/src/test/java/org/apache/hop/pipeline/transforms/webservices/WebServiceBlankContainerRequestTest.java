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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.webservices.wsdl.WsdlOpParameterList;
import org.apache.hop.pipeline.transforms.webservices.wsdl.WsdlOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/**
 * A blank "In field container" or "In field argument" must not produce an element in the SOAP
 * request. Up to 2.17 a saved {@code <wsInFieldContainer/>} loaded as null; since 2.18 it loads as
 * an empty string, which must not turn into a malformed {@code <ns:>} element.
 */
class WebServiceBlankContainerRequestTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<WebServiceMeta, WebServiceData> mockHelper;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("WebService", WebServiceMeta.class, WebServiceData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
  }

  @AfterEach
  void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  void blankContainerAndArgumentAddNoElement() throws Exception {
    String request = requestXml("<wsInFieldContainer/><wsInFieldArgument/>");

    assertFalse(request.contains("<ns:>"), request);
    assertFalse(request.contains("</ns:>"), request);
    assertDoesNotThrow(() -> XmlHandler.loadXmlString(request), request);
  }

  @Test
  void specifiedContainerAndArgumentAddTheirElements() throws Exception {
    String request =
        requestXml(
            "<wsInFieldContainer>Books</wsInFieldContainer>"
                + "<wsInFieldArgument>Book</wsInFieldArgument>");

    assertTrue(request.contains("<ns:Books>") && request.contains("</ns:Books>"), request);
    assertTrue(request.contains("<ns:Book>") && request.contains("</ns:Book>"), request);
    assertDoesNotThrow(() -> XmlHandler.loadXmlString(request), request);
  }

  /**
   * Load the transform from XML, the way it is saved in 0013-webservice.hpl, and build a request.
   */
  private String requestXml(String inFieldTags) throws Exception {
    String xml =
        "<transform>"
            + "<wsURL>http://localhost/BookService.wsdl</wsURL>"
            + "<wsOperation>AddBook</wsOperation>"
            + "<wsOperationRequest/>"
            + "<wsOperationNamespace>http://www.cleverbuilder.com/BookService/</wsOperationNamespace>"
            + inFieldTags
            + "<wsOutFieldContainer>Book</wsOutFieldContainer>"
            + "<wsOutFieldArgument>Book</wsOutFieldArgument>"
            + "<callTransform>1</callTransform>"
            + "<passingInputData>Y</passingInputData>"
            + "<compatible>N</compatible>"
            + "<repeating_element/>"
            + "<reply_as_string>Y</reply_as_string>"
            + "<fieldsIn/><fieldsOut/>"
            + "</transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    WebServiceMeta meta = new WebServiceMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());

    WebServiceData data = new WebServiceData();
    data.argumentRows.add(new Object[0]);
    WebService transform =
        new WebService(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    Field indexList = WebService.class.getDeclaredField("indexList");
    indexList.setAccessible(true);
    indexList.set(transform, new ArrayList<Integer>());

    WsdlOpParameterList parameters = mock(WsdlOpParameterList.class);
    when(parameters.iterator()).thenReturn(Collections.emptyIterator());
    when(parameters.getHeaderNames()).thenReturn(new HashSet<>());
    WsdlOperation operation = mock(WsdlOperation.class);
    when(operation.getParameters()).thenReturn(parameters);

    Method getRequestXml =
        WebService.class.getDeclaredMethod("getRequestXML", WsdlOperation.class, boolean.class);
    getRequestXml.setAccessible(true);
    return (String) getRequestXml.invoke(transform, operation, true);
  }
}
