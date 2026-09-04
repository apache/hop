/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www.api.v1.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.Response;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.www.api.HopApiBadRequestException;
import org.apache.hop.www.api.HopApiNotFoundException;
import org.apache.hop.www.api.HopServerApiContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Covers the JSON facade over an execution information location. */
class LocationResourceTest {

  private static final String LOC = "local";

  private LocationResource resource;
  private IExecutionInfoLocation iLocation;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    MultiMetadataProvider metadataProvider = mock(MultiMetadataProvider.class);
    IHopMetadataSerializer<ExecutionInfoLocation> serializer = mock(IHopMetadataSerializer.class);
    ExecutionInfoLocation location = mock(ExecutionInfoLocation.class);
    iLocation = mock(IExecutionInfoLocation.class);

    when(metadataProvider.getSerializer(ExecutionInfoLocation.class)).thenReturn(serializer);
    when(serializer.load(LOC)).thenReturn(location);
    when(location.getExecutionInfoLocation()).thenReturn(iLocation);

    HopServerApiContext context = mock(HopServerApiContext.class);
    when(context.getMetadataProvider()).thenReturn(metadataProvider);
    when(context.getVariables()).thenReturn(new Variables());

    resource = new LocationResource();
    resource.context = context;
  }

  @Test
  void listingIdsPassesTheQueryParametersThrough() throws Exception {
    when(iLocation.getExecutionIds(true, 25)).thenReturn(List.of("a", "b"));

    Response response = resource.getExecutionIds(LOC, true, 25);

    assertEquals(200, response.getStatus());
    assertEquals(List.of("a", "b"), response.getEntity());
    verify(iLocation).getExecutionIds(true, 25);
  }

  @Test
  void everyCallClosesTheLocation() throws Exception {
    when(iLocation.getExecutionIds(false, 100)).thenReturn(List.of());

    resource.getExecutionIds(LOC, false, 100);

    verify(iLocation).close();
  }

  @Test
  void aMissingExecutionIs404NotAnEmptyBody() throws Exception {
    when(iLocation.getExecution("nope")).thenReturn(null);

    assertThrows(HopApiNotFoundException.class, () -> resource.getExecution(LOC, "nope"));
  }

  @Test
  void aMissingStateIs404() throws Exception {
    when(iLocation.getExecutionState("nope")).thenReturn(null);

    assertThrows(HopApiNotFoundException.class, () -> resource.getExecutionState(LOC, "nope"));
  }

  @Test
  void aFoundExecutionIsReturned() throws Exception {
    Execution execution = new Execution();
    execution.setId("abc");
    when(iLocation.getExecution("abc")).thenReturn(execution);

    Response response = resource.getExecution(LOC, "abc");

    assertEquals(200, response.getStatus());
    assertEquals(execution, response.getEntity());
  }

  @Test
  void registeringAnExecutionReachesTheLocation() throws Exception {
    Execution execution = new Execution();
    execution.setId("abc");

    Response response = resource.registerExecution(LOC, execution);

    assertEquals(200, response.getStatus());
    verify(iLocation).registerExecution(execution);
    verify(iLocation).close();
  }

  @Test
  void updatingStateReachesTheLocation() throws Exception {
    ExecutionState state = new ExecutionState();

    resource.updateExecutionState(LOC, "abc", state);

    verify(iLocation).updateExecutionState(state);
  }

  @Test
  void registeringDataReachesTheLocation() throws Exception {
    ExecutionData data = new ExecutionData();

    resource.registerData(LOC, "abc", data);

    verify(iLocation).registerData(data);
  }

  @Test
  void deletingReturnsWhatTheLocationReports() throws Exception {
    when(iLocation.deleteExecution("abc")).thenReturn(true);

    Response response = resource.deleteExecution(LOC, "abc");

    assertEquals(Boolean.TRUE, response.getEntity());
  }

  @Test
  void executionDataDefaultsTheParentToTheExecutionItself() throws Exception {
    ExecutionData data = new ExecutionData();
    when(iLocation.getExecutionData("abc", "abc")).thenReturn(data);

    Response response = resource.getExecutionData(LOC, "abc", null);

    assertEquals(data, response.getEntity());
    verify(iLocation).getExecutionData("abc", "abc");
  }

  @Test
  void executionDataUsesAnExplicitParentWhenGiven() throws Exception {
    ExecutionData data = new ExecutionData();
    when(iLocation.getExecutionData("parent", "abc")).thenReturn(data);

    resource.getExecutionData(LOC, "abc", "parent");

    verify(iLocation).getExecutionData("parent", "abc");
  }

  @Test
  void aBadExecutionTypeIsAClientError() {
    HopApiBadRequestException thrown =
        assertThrows(
            HopApiBadRequestException.class, () -> resource.findChildIds(LOC, "abc", "Bogus"));

    assertTrue(thrown.getMessage().contains("Bogus"));
  }

  @Test
  void aMissingExecutionTypeIsAClientError() {
    assertThrows(HopApiBadRequestException.class, () -> resource.findChildIds(LOC, "abc", ""));
  }

  @Test
  void aValidExecutionTypeIsAccepted() throws Exception {
    when(iLocation.findChildIds(ExecutionType.Pipeline, "abc")).thenReturn(List.of("child"));

    Response response = resource.findChildIds(LOC, "abc", "Pipeline");

    assertEquals(List.of("child"), response.getEntity());
  }

  @Test
  void findLastExecutionRequiresAName() {
    assertThrows(
        HopApiBadRequestException.class, () -> resource.findLastExecution(LOC, "Pipeline", ""));
  }

  @Test
  void findLastExecutionIs404WhenThereIsNone() throws Exception {
    when(iLocation.findLastExecution(any(ExecutionType.class), any(String.class))).thenReturn(null);

    assertThrows(
        HopApiNotFoundException.class, () -> resource.findLastExecution(LOC, "Pipeline", "demo"));
  }

  @Test
  void anUnknownLocationPropagatesAsNotFound() throws Exception {
    HopException thrown =
        assertThrows(HopException.class, () -> resource.getExecutionIds("nosuch", false, 10));

    assertTrue(thrown.getMessage().contains("nosuch"));
  }

  @Test
  void anUnsetLoggingLimitMeansUnlimitedNotEmpty() throws Exception {
    // The servlet defaults to -1 = no limit. A 0 default returns "" on caching locations, which
    // silently looks like an execution with no log at all.
    when(iLocation.getExecutionStateLoggingText("abc", -1)).thenReturn("the log");

    Response response = resource.getExecutionStateLoggingText(LOC, "abc", -1);

    assertEquals("the log", response.getEntity());
    verify(iLocation).getExecutionStateLoggingText("abc", -1);
  }

  @Test
  void anExplicitLoggingLimitIsPassedThrough() throws Exception {
    when(iLocation.getExecutionStateLoggingText("abc", 500)).thenReturn("trimmed");

    resource.getExecutionStateLoggingText(LOC, "abc", 500);

    verify(iLocation).getExecutionStateLoggingText("abc", 500);
  }

  @Test
  void findingChildExecutionsReachesTheLocation() throws Exception {
    when(iLocation.findExecutions("abc")).thenReturn(List.of());

    Response response = resource.findChildExecutions(LOC, "abc");

    assertEquals(200, response.getStatus());
    verify(iLocation).findExecutions("abc");
  }

  @Test
  void findingTheParentReachesTheLocation() throws Exception {
    when(iLocation.findParentId("abc")).thenReturn("parent-1");

    Response response = resource.findParentId(LOC, "abc");

    assertEquals("parent-1", response.getEntity());
  }
}
