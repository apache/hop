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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@Ignore("This test needs to be reviewed")
public class LogChannelTest {

  private MockedStatic<Utils> mockedUtils;

  private MockedStatic<HopLogStore> mockedHopLogStore;

  private MockedStatic<LoggingRegistry> mockedLoggingRegistry;

  private MockedStatic<DefaultLogLevel> mockedDefaultLogLevel;

  private LogChannel logChannel;
  private final String logChannelSubject = "pdi";
  private final String channelId = "1234-5678-abcd-efgh";

  private LogLevel logLevel;
  private ILogMessage logMsgInterface;
  private LogChannelFileWriterBuffer logChFileWriterBuffer;

  @Before
  public void setUp() {
    LogLevel logLevelStatic = Mockito.mock(LogLevel.class);
    mockedDefaultLogLevel.when(DefaultLogLevel::getLogLevel).thenReturn(LogLevel.BASIC);

    logChFileWriterBuffer = mock(LogChannelFileWriterBuffer.class);

    LoggingRegistry regInstance = mock(LoggingRegistry.class);
    Mockito.when(regInstance.registerLoggingSource(logChannelSubject)).thenReturn(channelId);
    Mockito.when(regInstance.getLogChannelFileWriterBuffer(channelId))
        .thenReturn(logChFileWriterBuffer);
    mockedLoggingRegistry.when(LoggingRegistry::getInstance).thenReturn(regInstance);

    logLevel = Mockito.mock(LogLevel.class);

    logMsgInterface = mock(ILogMessage.class);
    Mockito.when(logMsgInterface.getLevel()).thenReturn(logLevel);

    logChannel = new LogChannel(logChannelSubject);
  }

  @Before
  public void setUpStaticMocks() {
    mockedUtils = Mockito.mockStatic(Utils.class);
    mockedHopLogStore = Mockito.mockStatic(HopLogStore.class);
    mockedLoggingRegistry = Mockito.mockStatic(LoggingRegistry.class);
    mockedDefaultLogLevel = Mockito.mockStatic(DefaultLogLevel.class);
  }

  @After
  public void tearDownStaticMocks() {
    mockedDefaultLogLevel.closeOnDemand();
    mockedLoggingRegistry.closeOnDemand();
    mockedHopLogStore.closeOnDemand();
    mockedUtils.closeOnDemand();
  }

  @Test
  public void testPrintlnWithNullLogChannelFileWriterBuffer() {
    when(logLevel.isVisible(any(LogLevel.class))).thenReturn(true);

    LoggingBuffer loggingBuffer = mock(LoggingBuffer.class);
    mockedHopLogStore.when(HopLogStore::getAppender).thenReturn(loggingBuffer);

    logChannel.println(logMsgInterface, LogLevel.BASIC);
    verify(logChFileWriterBuffer, times(1)).addEvent(any(HopLoggingEvent.class));
    verify(loggingBuffer, times(1)).addLogggingEvent(any(HopLoggingEvent.class));
  }

  @Test
  public void testPrintlnLogNotVisible() {
    when(logLevel.isVisible(any(LogLevel.class))).thenReturn(false);
    logChannel.println(logMsgInterface, LogLevel.BASIC);
    verify(logChFileWriterBuffer, times(0)).addEvent(any(HopLoggingEvent.class));
  }

  @Test
  public void testPrintMessageFiltered() {
    LogLevel logLevelFil = Mockito.mock(LogLevel.class);
    when(logLevelFil.isError()).thenReturn(false);

    ILogMessage logMsgInterfaceFil = mock(ILogMessage.class);
    Mockito.when(logMsgInterfaceFil.getLevel()).thenReturn(logLevelFil);
    Mockito.when(logMsgInterfaceFil.toString()).thenReturn("a");
    mockedUtils.when(() -> Utils.isEmpty(anyString())).thenReturn(false);

    when(logLevelFil.isVisible(any(LogLevel.class))).thenReturn(true);
    logChannel.setFilter("b");
    logChannel.println(logMsgInterfaceFil, LogLevel.BASIC);
    verify(logChFileWriterBuffer, times(0)).addEvent(any(HopLoggingEvent.class));
  }
}
