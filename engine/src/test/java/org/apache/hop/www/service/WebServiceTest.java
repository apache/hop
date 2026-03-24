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

package org.apache.hop.www.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class WebServiceTest {

  @Test
  void defaultConstructorAndSetters() {
    WebService ws = new WebService();
    ws.setName("svc1");
    ws.setEnabled(true);
    ws.setFilename("/path/pipeline.hpl");
    ws.setTransformName("t1");
    ws.setFieldName("f1");
    ws.setContentType("application/json");
    ws.setStatusCode("200");
    ws.setListingStatus(true);
    ws.setBodyContentVariable("bodyVar");
    ws.setRunConfigurationName("local");
    ws.setHeaderContentVariable("hdrVar");

    assertEquals("svc1", ws.getName());
    assertTrue(ws.isEnabled());
    assertEquals("/path/pipeline.hpl", ws.getFilename());
    assertEquals("t1", ws.getTransformName());
    assertEquals("f1", ws.getFieldName());
    assertEquals("application/json", ws.getContentType());
    assertEquals("200", ws.getStatusCode());
    assertTrue(ws.isListingStatus());
    assertEquals("bodyVar", ws.getBodyContentVariable());
    assertEquals("local", ws.getRunConfigurationName());
    assertEquals("hdrVar", ws.getHeaderContentVariable());
  }

  @Test
  void fullConstructor() {
    WebService ws =
        new WebService(
            "n", false, "file.hpl", "tr", "fld", "text/plain", "201", false, "b", "run", "h");
    assertEquals("n", ws.getName());
    assertFalse(ws.isEnabled());
    assertEquals("file.hpl", ws.getFilename());
    assertEquals("h", ws.getHeaderContentVariable());
  }
}
