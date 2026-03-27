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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WebServiceMetaTest {

  @BeforeEach
  void beforeEach() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    WebServiceMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/webservice-lookup.xml", WebServiceMeta.class);

    assertEquals("url", meta.getUrl());
    assertEquals("operation", meta.getOperationName());
    assertEquals("operationRequest", meta.getOperationRequestName());
    assertEquals("operationNameSpace", meta.getOperationNamespace());
    assertEquals("wsInFieldContainer", meta.getInFieldContainerName());
    assertEquals("wsInFieldArg", meta.getInFieldArgumentName());
    assertEquals("outFieldContainer", meta.getOutFieldContainerName());
    assertEquals("outFieldArgument", meta.getOutFieldArgumentName());
    assertEquals("proxy-host", meta.getProxyHost());
    assertEquals("proxy-port", meta.getProxyPort());
    assertEquals("http-login", meta.getHttpLogin());
    assertEquals("http-password", meta.getHttpPassword());
    assertEquals(1000, meta.getCallTransform());
    assertTrue(meta.isPassingInputData());
    assertTrue(meta.isCompatible());
    assertEquals("repeating-element", meta.getRepeatingElementName());
    assertTrue(meta.isReturningReplyAsString());

    validateFieldsIn(meta);
    validateFieldsOut(meta);
  }

  private static void validateFieldsOut(WebServiceMeta meta) {
    assertEquals(3, meta.getFieldsOut().size());
    WebServiceField o = meta.getFieldsOut().getFirst();
    assertEquals("out1", o.getName());
    assertEquals("wsOut1", o.getWsName());
    assertEquals("outType1", o.getXsdType());
    o = meta.getFieldsOut().get(1);
    assertEquals("out2", o.getName());
    assertEquals("wsOut2", o.getWsName());
    assertEquals("outType2", o.getXsdType());
    o = meta.getFieldsOut().getLast();
    assertEquals("out3", o.getName());
    assertEquals("wsOut3", o.getWsName());
    assertEquals("outType3", o.getXsdType());
  }

  private static void validateFieldsIn(WebServiceMeta meta) {
    assertEquals(2, meta.getFieldsIn().size());
    WebServiceField i = meta.getFieldsIn().getFirst();
    assertEquals("name1", i.getName());
    assertEquals("wsName1", i.getWsName());
    assertEquals("wsType1", i.getXsdType());
    i = meta.getFieldsIn().getLast();
    assertEquals("name2", i.getName());
    assertEquals("wsName2", i.getWsName());
    assertEquals("wsType2", i.getXsdType());
  }
}
