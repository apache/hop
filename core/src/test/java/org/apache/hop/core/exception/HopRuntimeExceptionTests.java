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
 *
 */

package org.apache.hop.core.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit test for {@link HopRuntimeException} */
class HopRuntimeExceptionTests {

  @Test
  void testNoArgsConstructor() {
    HopRuntimeException ex = new HopRuntimeException();

    assertNull(ex.getMessage());
    assertNull(ex.getCause());
  }

  @Test
  void testMessageConstructor() {
    HopRuntimeException ex = new HopRuntimeException("error");

    assertEquals("error", ex.getMessage());
    assertNull(ex.getCause());
  }

  @Test
  void testCauseConstructor() {
    Throwable cause = new RuntimeException("root cause");
    HopRuntimeException ex = new HopRuntimeException(cause);

    assertEquals(cause, ex.getCause());
    assertTrue(ex.getMessage().contains("root cause"));
  }

  @Test
  void testMessageAndCauseConstructor() {
    Throwable cause = new RuntimeException("root cause");
    HopRuntimeException ex = new HopRuntimeException("error", cause);

    assertEquals("error", ex.getMessage());
    assertEquals(cause, ex.getCause());
  }

  @Test
  void testStringAndExceptionConstructor() {
    Exception cause = new Exception("checked exception");
    HopRuntimeException ex = new HopRuntimeException("error", cause);

    assertEquals("error", ex.getMessage());
    assertEquals(cause, ex.getCause());
  }
}
