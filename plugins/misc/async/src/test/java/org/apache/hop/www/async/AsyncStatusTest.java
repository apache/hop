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

package org.apache.hop.www.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.www.HopServerPipelineStatus;
import org.junit.jupiter.api.Test;

/** Test class for AsyncStatus */
public class AsyncStatusTest {

  @Test
  public void testDefaultConstructor() {
    AsyncStatus status = new AsyncStatus();

    assertNotNull(status.getLogDate());
    assertNotNull(status.getStatusVariables());
    assertNotNull(status.getPipelineStatuses());

    assertTrue(status.getStatusVariables().isEmpty());
    assertTrue(status.getPipelineStatuses().isEmpty());
  }

  @Test
  public void testSettersAndGetters() {
    AsyncStatus status = new AsyncStatus();

    String service = "test-service";
    status.setService(service);
    assertEquals(service, status.getService());

    String id = "test-id-123";
    status.setId(id);
    assertEquals(id, status.getId());

    Date logDate = new Date();
    Date startDate = new Date(System.currentTimeMillis() - 1000);
    Date endDate = new Date();

    status.setLogDate(logDate);
    status.setStartDate(startDate);
    status.setEndDate(endDate);

    assertEquals(logDate, status.getLogDate());
    assertEquals(startDate, status.getStartDate());
    assertEquals(endDate, status.getEndDate());

    String statusDescription = "Running";
    status.setStatusDescription(statusDescription);
    assertEquals(statusDescription, status.getStatusDescription());
  }

  @Test
  public void testStatusVariables() {
    AsyncStatus status = new AsyncStatus();
    Map<String, String> variables = new HashMap<>();

    variables.put("VAR1", "value1");
    variables.put("VAR2", "value2");
    variables.put("VAR3", "value3");

    status.setStatusVariables(variables);

    assertEquals(variables, status.getStatusVariables());
    assertEquals(3, status.getStatusVariables().size());
    assertEquals("value1", status.getStatusVariables().get("VAR1"));
    assertEquals("value2", status.getStatusVariables().get("VAR2"));
    assertEquals("value3", status.getStatusVariables().get("VAR3"));
  }

  @Test
  public void testPipelineStatuses() {
    AsyncStatus status = new AsyncStatus();

    HopServerPipelineStatus pipeline1 = new HopServerPipelineStatus();
    pipeline1.setPipelineName("pipeline1");
    pipeline1.setStatusDescription("Running");

    HopServerPipelineStatus pipeline2 = new HopServerPipelineStatus();
    pipeline2.setPipelineName("pipeline2");
    pipeline2.setStatusDescription("Finished");

    status.getPipelineStatuses().add(pipeline1);
    status.getPipelineStatuses().add(pipeline2);

    assertEquals(2, status.getPipelineStatuses().size());
    assertTrue(status.getPipelineStatuses().contains(pipeline1));
    assertTrue(status.getPipelineStatuses().contains(pipeline2));
  }
}
