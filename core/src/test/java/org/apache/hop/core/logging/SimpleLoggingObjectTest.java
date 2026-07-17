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
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class SimpleLoggingObjectTest {

  /** Not every logging object logs itself, so it does not have to have a log channel. */
  @Test
  void withoutALogChannelOfItsOwnItReportsNone() {
    SimpleLoggingObject loggingObject =
        new SimpleLoggingObject("a-servlet", LoggingObjectType.HOP_SERVER, null);

    assertNull(loggingObject.getLogChannelId());
  }

  /**
   * A logging object that is the parent of what a server runs needs a log channel of its own: it is
   * what a log file writer is hung on. See issue #4677.
   */
  @Test
  void itReportsTheLogChannelItWasGiven() {
    SimpleLoggingObject loggingObject =
        new SimpleLoggingObject("a-servlet", LoggingObjectType.HOP_SERVER, null);

    loggingObject.setLogChannelId("a-log-channel");

    assertEquals("a-log-channel", loggingObject.getLogChannelId());
  }
}
