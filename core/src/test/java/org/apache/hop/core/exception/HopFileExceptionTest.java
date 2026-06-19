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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.Const;
import org.junit.jupiter.api.Test;

class HopFileExceptionTest {

  @Test
  void messageAndCauseFormattedLikeHopException() {
    Throwable cause = new RuntimeException("io failed");
    HopFileException ex = new HopFileException("read error", cause);
    String msg = ex.getMessage();
    assertTrue(msg.contains(Const.CR));
    assertTrue(msg.contains("read error"));
    assertTrue(msg.contains("io failed"));
    assertEquals(cause, ex.getCause());
  }

  @Test
  void messageOnly() {
    HopFileException ex = new HopFileException("eof");
    assertTrue(ex.getMessage().contains("eof"));
  }
}
