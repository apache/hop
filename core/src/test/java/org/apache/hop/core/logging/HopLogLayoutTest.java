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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Test for {@link HopLogLayout}. */
class HopLogLayoutTest {

  @Test
  void testFormat() {
    LogMessage mcg =
        new LogMessage("Log message for {0}", "Channel 01", new String[] {"Test"}, LogLevel.DEBUG);

    HopLoggingEvent event = new HopLoggingEvent(mcg, 0, LogLevel.BASIC);
    HopLogLayout layout = new HopLogLayout();

    final String formattedMsg = layout.format(event);

    assertEquals(
        "Log message for Test",
        formattedMsg.substring(formattedMsg.indexOf('-') + 2),
        "The log message must be formatted and not contain placeholders.");
  }

  @Test
  void testFormatRendersAttachedThrowable() {
    LogMessage message = new LogMessage("Boom", "Channel 01", LogLevel.ERROR);
    message.setThrowable(new IllegalStateException("kaboom"));

    HopLoggingEvent event = new HopLoggingEvent(message, 0, LogLevel.ERROR);
    final String formatted = new HopLogLayout().format(event);

    assertTrue(formatted.contains("Boom"), "The message text must be present.");
    assertTrue(
        formatted.contains("IllegalStateException"),
        "The attached throwable's stack trace must be rendered for console/file output.");
    assertTrue(formatted.contains("kaboom"), "The throwable message must be rendered.");
  }

  @Test
  void testFormatWithoutThrowableIsUnchanged() {
    LogMessage message = new LogMessage("No error here", "Channel 01", LogLevel.BASIC);

    HopLoggingEvent event = new HopLoggingEvent(message, 0, LogLevel.BASIC);
    final String formatted = new HopLogLayout().format(event);

    assertFalse(
        formatted.contains("\tat "),
        "Messages without a throwable must not contain any stack-trace frames.");
  }
}
