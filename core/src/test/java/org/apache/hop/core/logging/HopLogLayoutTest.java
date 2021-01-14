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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link HopLogLayout}.
 */
public class HopLogLayoutTest {

  @Test
  public void testFormat() throws Exception {
    LogMessage mcg = new LogMessage( "Log message for {0}",
      "Channel 01",
      new String[] { "Test" },
      LogLevel.DEBUG );

    HopLoggingEvent event = new HopLoggingEvent( mcg, 0, LogLevel.BASIC );
    HopLogLayout layout = new HopLogLayout();

    final String formattedMsg = layout.format( event );

    assertEquals( "The log message must be formatted and not contain placeholders.",
      "Log message for Test",
      formattedMsg.substring( formattedMsg.indexOf( '-' ) + 2 ) );
  }
}
