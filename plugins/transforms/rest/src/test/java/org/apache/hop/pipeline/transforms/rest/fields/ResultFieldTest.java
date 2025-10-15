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

package org.apache.hop.pipeline.transforms.rest.fields;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class ResultFieldTest {

  @Test
  void testDefaultConstructor() {
    ResultField field = new ResultField();
    assertNotNull(field);
    assertNull(field.getFieldName());
    assertNull(field.getCode());
    assertNull(field.getResponseTime());
    assertNull(field.getResponseHeader());
  }

  @Test
  void testParameterizedConstructor() {
    ResultField field = new ResultField("result", "statusCode", "time", "headers");
    assertEquals("result", field.getFieldName());
    assertEquals("statusCode", field.getCode());
    // Note: Constructor doesn't set responseTime and responseHeader
    assertNull(field.getResponseTime());
    assertNull(field.getResponseHeader());
  }

  @Test
  void testSettersAndGetters() {
    ResultField field = new ResultField();
    field.setFieldName("resultField");
    field.setCode("200");
    field.setResponseTime("1000");
    field.setResponseHeader("headerData");

    assertEquals("resultField", field.getFieldName());
    assertEquals("200", field.getCode());
    assertEquals("1000", field.getResponseTime());
    assertEquals("headerData", field.getResponseHeader());
  }

  @Test
  void testSetFieldName() {
    ResultField field = new ResultField();
    field.setFieldName("testField");
    assertEquals("testField", field.getFieldName());
  }

  @Test
  void testSetCode() {
    ResultField field = new ResultField();
    field.setCode("404");
    assertEquals("404", field.getCode());
  }

  @Test
  void testSetResponseTime() {
    ResultField field = new ResultField();
    field.setResponseTime("2500");
    assertEquals("2500", field.getResponseTime());
  }

  @Test
  void testSetResponseHeader() {
    ResultField field = new ResultField();
    field.setResponseHeader("Content-Type: application/json");
    assertEquals("Content-Type: application/json", field.getResponseHeader());
  }
}
