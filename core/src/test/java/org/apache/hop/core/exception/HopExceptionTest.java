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

package org.apache.hop.core.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.Const;
import org.junit.jupiter.api.Test;

class HopExceptionTest {

  @Test
  void getSuperMessageReturnsRawMessageWithoutFormatting() {
    HopException ex = new HopException("hello");
    assertEquals("hello", ex.getSuperMessage());
  }

  @Test
  void getMessageAppendsCauseMessage() {
    Throwable cause = new RuntimeException("root");
    HopException ex = new HopException("top", cause);
    String msg = ex.getMessage();
    assertTrue(msg.contains(Const.CR));
    assertTrue(msg.contains("top"));
    assertTrue(msg.contains("root"));
  }

  @Test
  void noArgConstructor() {
    HopException ex = new HopException();
    assertNull(ex.getSuperMessage());
  }

  @Test
  void causeOnlyConstructor() {
    Throwable inner = new IllegalStateException("state");
    HopException ex = new HopException(inner);
    assertEquals(inner, ex.getCause());
    assertTrue(ex.getMessage().contains("state"));
  }
}
