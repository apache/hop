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

import org.junit.jupiter.api.Test;

class LogMessageTest {

  /** Same shape as JSON often logged from scripts (e.g. action.logBasic(myJSON)). */
  private static final String JSON_LIKE = "{ \"myNode\" : { \"myValue\" : \"abc\" } }";

  @Test
  void getMessage_jsonLike_withNullArguments_returnsMessageUnchanged() {
    LogMessage msg = new LogMessage(JSON_LIKE, "channel-1", null, LogLevel.BASIC, false);
    assertEquals(JSON_LIKE, msg.getMessage());
  }

  /**
   * Script/interop may resolve a single-argument call to {@code logBasic(String, Object...)} with
   * an empty varargs array; MessageFormat must not treat the string as a pattern in that case.
   */
  @Test
  void getMessage_jsonLike_withEmptyVarargs_returnsMessageUnchanged() {
    LogMessage msg = new LogMessage(JSON_LIKE, "channel-1", new Object[0], LogLevel.BASIC, false);
    assertEquals(JSON_LIKE, msg.getMessage());
  }

  @Test
  void getMessage_withPlaceholders_substitutesArguments() {
    LogMessage msg =
        new LogMessage("Hello {0}", "channel-1", new Object[] {"world"}, LogLevel.BASIC, false);
    assertEquals("Hello world", msg.getMessage());
  }
}
