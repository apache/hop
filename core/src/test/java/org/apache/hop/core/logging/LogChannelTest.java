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

import org.apache.hop.core.util.Utils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith( PowerMockRunner.class )
@PrepareForTest( { DefaultLogLevel.class, LoggingRegistry.class, LogLevel.class, HopLogStore.class, Utils.class } )
public class LogChannelTest {

  private LogChannel logChannel;
  private String logChannelSubject = "pdi";
  private String channelId = "1234-5678-abcd-efgh";

  private LogLevel logLevel;
  private ILogMessage logMsgInterface;
  private LogChannelFileWriterBuffer logChFileWriterBuffer;

  @Before
  public void setUp() throws Exception {
    LogLevel logLevelStatic = PowerMockito.mock( LogLevel.class );
    Whitebox.setInternalState( logLevelStatic, "name", "Basic" );
    Whitebox.setInternalState( logLevelStatic, "ordinal", 3 );

    PowerMockito.mockStatic( DefaultLogLevel.class );
    when( DefaultLogLevel.getLogLevel() ).thenReturn( LogLevel.BASIC );

    logChFileWriterBuffer = mock( LogChannelFileWriterBuffer.class );

    LoggingRegistry regInstance = mock( LoggingRegistry.class );
    Mockito.when( regInstance.registerLoggingSource( logChannelSubject ) ).thenReturn( channelId );
    Mockito.when( regInstance.getLogChannelFileWriterBuffer( channelId ) ).thenReturn( logChFileWriterBuffer );

    PowerMockito.mockStatic( LoggingRegistry.class );
    when( LoggingRegistry.getInstance() ).thenReturn( regInstance );

    logLevel = PowerMockito.mock( LogLevel.class );
    Whitebox.setInternalState( logLevel, "name", "Basic" );
    Whitebox.setInternalState( logLevel, "ordinal", 3 );

    logMsgInterface = mock( ILogMessage.class );
    Mockito.when( logMsgInterface.getLevel() ).thenReturn( logLevel );

    logChannel = new LogChannel( logChannelSubject );
  }

  @Test
  public void testPrintlnWithNullLogChannelFileWriterBuffer() {
    when( logLevel.isVisible( any( LogLevel.class ) ) ).thenReturn( true );

    LoggingBuffer loggingBuffer = mock( LoggingBuffer.class );
    PowerMockito.mockStatic( HopLogStore.class );
    when( HopLogStore.getAppender() ).thenReturn( loggingBuffer );

    logChannel.println( logMsgInterface, LogLevel.BASIC );
    verify( logChFileWriterBuffer, times( 1 ) ).addEvent( any( HopLoggingEvent.class ) );
    verify( loggingBuffer, times( 1 ) ).addLogggingEvent( any( HopLoggingEvent.class ) );
  }

  @Test
  public void testPrintlnLogNotVisible() {
    when( logLevel.isVisible( any( LogLevel.class ) ) ).thenReturn( false );
    logChannel.println( logMsgInterface, LogLevel.BASIC );
    verify( logChFileWriterBuffer, times( 0 ) ).addEvent( any( HopLoggingEvent.class ) );
  }

  @Test
  public void testPrintMessageFiltered() {
    LogLevel logLevelFil = PowerMockito.mock( LogLevel.class );
    Whitebox.setInternalState( logLevelFil, "name", "Error" );
    Whitebox.setInternalState( logLevelFil, "ordinal", 1 );
    when( logLevelFil.isError() ).thenReturn( false );

    ILogMessage logMsgInterfaceFil = mock( ILogMessage.class );
    Mockito.when( logMsgInterfaceFil.getLevel() ).thenReturn( logLevelFil );
    Mockito.when( logMsgInterfaceFil.toString() ).thenReturn( "a" );

    PowerMockito.mockStatic( Utils.class );
    when( Utils.isEmpty( anyString() ) ).thenReturn( false );

    when( logLevelFil.isVisible( any( LogLevel.class ) ) ).thenReturn( true );
    logChannel.setFilter( "b" );
    logChannel.println( logMsgInterfaceFil, LogLevel.BASIC );
    verify( logChFileWriterBuffer, times( 0 ) ).addEvent( any( HopLoggingEvent.class ) );
  }
}
