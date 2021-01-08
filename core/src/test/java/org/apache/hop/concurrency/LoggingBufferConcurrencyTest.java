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

package org.apache.hop.concurrency;

import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LogMessage;
import org.apache.hop.core.logging.LoggingBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class LoggingBufferConcurrencyTest {

  private LoggingBuffer buffer;

  @Test
  public void shouldNotFailProcessingEventsUnderHighContention() throws Exception {
    int modifiersAmount = 100;
    int readersAmount = 100;

    buffer = new LoggingBuffer( 5000 );

    AtomicBoolean condition = new AtomicBoolean( true );

    List<StopOnErrorCallable<?>> modifiers = new ArrayList<>();
    for ( int i = 0; i < modifiersAmount; i++ ) {
      modifiers.add( new Appender( condition ) );
    }
    List<StopOnErrorCallable<?>> readers = new ArrayList<>();
    for ( int i = 0; i < readersAmount; i++ ) {
      readers.add( new Reader( condition ) );
    }

    ConcurrencyTestRunner<?, ?> runner =
      new ConcurrencyTestRunner<>( modifiers, readers, condition, 5000 );
    runner.runConcurrentTest();
    runner.checkNoExceptionRaised();
  }

  private class Appender extends StopOnErrorCallable<Void> {
    Appender( AtomicBoolean condition ) {
      super( condition );
    }

    @Override
    Void doCall() {
      for ( int i = 0; i < 5000; i++ ) {
        buffer.addLogggingEvent( new HopLoggingEvent(
          new LogMessage( "subject", LogLevel.DEBUG ), System.currentTimeMillis(), LogLevel.DEBUG ) );
      }
      return null;
    }
  }

  private class Reader extends StopOnErrorCallable<StringBuffer> {
    Reader( AtomicBoolean condition ) {
      super( condition );
    }

    @Override
    StringBuffer doCall() {
      return buffer.getBuffer();
    }
  }
}
