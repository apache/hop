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

package org.apache.hop.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The contract that matters here is the lifecycle: initialize() happens inside the try, so a
 * location which throws part way through is still closed, and a failing close() never replaces the
 * error that actually caused the block to be left.
 */
class ExecutionInfoLocationsTest {

  private IVariables variables;
  private IHopMetadataProvider metadataProvider;
  private IHopMetadataSerializer<ExecutionInfoLocation> serializer;
  private IExecutionInfoLocation iLocation;
  private ExecutionInfoLocation location;
  private ILogChannel log;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    variables = new Variables();
    metadataProvider = mock(IHopMetadataProvider.class);
    serializer = mock(IHopMetadataSerializer.class);
    iLocation = mock(IExecutionInfoLocation.class);
    location = mock(ExecutionInfoLocation.class);
    log = mock(ILogChannel.class);

    when(metadataProvider.getSerializer(ExecutionInfoLocation.class)).thenReturn(serializer);
    when(serializer.load("local")).thenReturn(location);
    when(location.getExecutionInfoLocation()).thenReturn(iLocation);
  }

  private <T> T run(ExecutionInfoLocations.ILocationAction<T> action) throws HopException {
    return ExecutionInfoLocations.withLocation("local", variables, metadataProvider, log, action);
  }

  @Test
  void initializesRunsAndClosesInThatOrder() throws Exception {
    String result = run(l -> "done");

    assertEquals("done", result);
    verify(iLocation).initialize(any(IVariables.class), any(IHopMetadataProvider.class));
    verify(iLocation).close();
  }

  @Test
  void handsTheActionTheInitializedLocation() throws Exception {
    IExecutionInfoLocation seen = run(l -> l);
    assertSame(iLocation, seen);
  }

  @Test
  void closesWhenTheActionThrows() throws Exception {
    HopException boom = new HopException("action failed");

    HopException thrown =
        assertThrows(
            HopException.class,
            () ->
                run(
                    l -> {
                      throw boom;
                    }));

    assertSame(boom, thrown);
    verify(iLocation).close();
  }

  @Test
  void closesWhenInitializeThrows() throws Exception {
    // The leak this helper exists to prevent: initialize() is inside the try.
    doThrow(new HopException("cannot connect"))
        .when(iLocation)
        .initialize(any(IVariables.class), any(IHopMetadataProvider.class));

    HopException thrown = assertThrows(HopException.class, () -> run(l -> "unreachable"));

    assertEquals("cannot connect", thrown.getMessage().trim());
    verify(iLocation).close();
  }

  @Test
  void aFailingCloseDoesNotMaskTheRealError() throws Exception {
    doThrow(new RuntimeException("close blew up")).when(iLocation).close();

    HopException thrown =
        assertThrows(
            HopException.class,
            () ->
                run(
                    l -> {
                      throw new HopException("the real error");
                    }));

    assertTrue(thrown.getMessage().contains("the real error"));
    assertFalse(thrown.getMessage().contains("close blew up"));
    verify(log).logError(org.mockito.ArgumentMatchers.contains("local"), any(Exception.class));
  }

  @Test
  void aFailingCloseOnASuccessfulActionIsReportedNotSwallowed() throws Exception {
    // Caching locations persist their writes in close(). Reporting success while the data was
    // never stored would lose execution history silently, so this must surface to the caller.
    doThrow(new HopException("could not persist")).when(iLocation).close();

    HopException thrown = assertThrows(HopException.class, () -> run(l -> "done"));

    assertTrue(thrown.getMessage().contains("could not persist"));
  }

  @Test
  void aNonHopFailureFromTheActionIsWrappedNotLost() {
    HopException thrown =
        assertThrows(
            HopException.class,
            () ->
                run(
                    l -> {
                      throw new IllegalStateException("unexpected");
                    }));

    assertNotNull(thrown);
  }

  @Test
  void aNullLogIsToleratedWhileUnwinding() throws Exception {
    doThrow(new RuntimeException("close blew up")).when(iLocation).close();

    HopException thrown =
        assertThrows(
            HopException.class,
            () ->
                ExecutionInfoLocations.withLocation(
                    "local",
                    variables,
                    metadataProvider,
                    null,
                    l -> {
                      throw new HopException("the real error");
                    }));

    assertTrue(thrown.getMessage().contains("the real error"));
  }

  @Test
  void anEmptyLocationNameIsRejectedBeforeAnythingIsLoaded() throws Exception {
    HopException thrown =
        assertThrows(
            HopException.class,
            () ->
                ExecutionInfoLocations.withLocation(
                    "", variables, metadataProvider, log, l -> "x"));

    assertTrue(thrown.getMessage().contains("Please specify"));
    verify(serializer, never()).load(org.mockito.ArgumentMatchers.anyString());
  }

  @Test
  void anUnknownLocationIsReportedAsNotFound() throws Exception {
    when(serializer.load("nope")).thenReturn(null);

    ExecutionInfoLocations.LocationNotFoundException thrown =
        assertThrows(
            ExecutionInfoLocations.LocationNotFoundException.class,
            () ->
                ExecutionInfoLocations.withLocation(
                    "nope", variables, metadataProvider, log, l -> "x"));

    assertTrue(thrown.getMessage().contains("nope"));
    // Nothing was opened, so nothing may be closed.
    verify(iLocation, never()).close();
  }

  @Test
  void notFoundIsAHopExceptionSoExistingCallersStillCatchIt() {
    assertNotNull(new ExecutionInfoLocations.LocationNotFoundException("x"));
    assertTrue(
        HopException.class.isAssignableFrom(
            ExecutionInfoLocations.LocationNotFoundException.class));
  }
}
