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

package org.apache.hop.core.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class LogChannelTest {
  private MockedStatic<Utils> mockedUtils;
  private MockedStatic<HopLogStore> mockedHopLogStore;
  private MockedStatic<LoggingRegistry> mockedLoggingRegistry;
  private MockedStatic<DefaultLogLevel> mockedDefaultLogLevel;

  private LogChannel logChannel;

  @BeforeEach
  void setUp() {
    mockedUtils = Mockito.mockStatic(Utils.class);
    mockedHopLogStore = Mockito.mockStatic(HopLogStore.class);
    mockedLoggingRegistry = Mockito.mockStatic(LoggingRegistry.class);
    mockedDefaultLogLevel = Mockito.mockStatic(DefaultLogLevel.class);

    LoggingRegistry regInstance = mock(LoggingRegistry.class);
    String logChannelSubject = "pdi";
    String channelId = "1234-5678-abcd-efgh";
    when(regInstance.registerLoggingSource(logChannelSubject)).thenReturn(channelId);
    mockedLoggingRegistry.when(LoggingRegistry::getInstance).thenReturn(regInstance);

    logChannel = new LogChannel(logChannelSubject);
  }

  @AfterEach
  void tearDownStaticMocks() {
    mockedDefaultLogLevel.closeOnDemand();
    mockedLoggingRegistry.closeOnDemand();
    mockedHopLogStore.closeOnDemand();
    mockedUtils.closeOnDemand();
  }

  @Test
  void testPrintlnWithThrowableBumpsLevelPreRendersTraceAndReleasesThrowable() {
    logChannel.setFilter("");

    LogMessage message = new LogMessage("Boom", "1234-5678-abcd-efgh", LogLevel.BASIC);
    IllegalStateException failure = new IllegalStateException("kaboom");

    logChannel.println(message, failure, LogLevel.ERROR);

    // Surfaced at ERROR level so error scanners/counters still detect it.
    assertEquals(LogLevel.ERROR, message.getLevel());
    // Live throwable released so the buffered event does not retain the exception object graph.
    assertNull(message.getThrowable());
    // Pre-rendered trace survives for text/buffer consumers (console, file, GUI log browser).
    assertNotNull(message.getStackTrace());
    assertTrue(message.getStackTrace().contains("IllegalStateException"));
    assertTrue(message.getStackTrace().contains("kaboom"));
  }
}
