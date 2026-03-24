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

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class HopServerStatusTest {

  @Test
  void defaultConstructorInitializesLists() {
    HopServerStatus status = new HopServerStatus();
    assertNotNull(status.getPipelineStatusList());
    assertNotNull(status.getWorkflowStatusList());
    assertTrue(status.getPipelineStatusList().isEmpty());
  }

  @Test
  void statusDescriptionConstructor() {
    HopServerStatus status = new HopServerStatus("Online");
    assertEquals("Online", status.getStatusDescription());
  }

  @Test
  void getXmlWithEmptyLists() throws HopException {
    HopServerStatus status = new HopServerStatus("Up");
    status.setMemoryFree(1L);
    status.setMemoryTotal(2L);
    status.setCpuCores(4);
    String xml = status.getXml();
    assertTrue(xml.contains(HopServerStatus.XML_TAG));
    assertTrue(xml.contains("Up"));
    assertTrue(xml.contains("pipeline_status_list"));
    assertTrue(xml.contains("workflow_status_list"));
  }
}
