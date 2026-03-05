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

class HopFileNotFoundExceptionTest {
  private final String expectedNullMessage = Const.CR + "null" + Const.CR;
  private final String errorMessage = "error message";
  private final String causeExceptionMessage = "Cause exception";
  private final Throwable cause = new RuntimeException(causeExceptionMessage);

  @Test
  void testConstructor() {
    try {
      throw new HopFileNotFoundException();
    } catch (HopFileNotFoundException e) {
      assertNull(e.getCause());
      assertTrue(e.getMessage().contains(expectedNullMessage));
      assertNull(e.getFilepath());
    }
  }

  @Test
  void testConstructorMessage() {
    try {
      throw new HopFileNotFoundException(errorMessage);
    } catch (HopFileNotFoundException e) {
      assertNull(e.getCause());
      assertTrue(e.getMessage().contains(errorMessage));
      assertNull(e.getFilepath());
    }
  }

  @Test
  void testConstructorMessageAndFilepath() {
    String filepath = "file.txt";
    try {
      throw new HopFileNotFoundException(errorMessage, filepath);
    } catch (HopFileNotFoundException e) {
      assertNull(e.getCause());
      assertTrue(e.getMessage().contains(errorMessage));
      assertEquals(filepath, e.getFilepath());
    }
  }

  @Test
  void testConstructorThrowable() {
    try {
      throw new HopFileNotFoundException(cause);
    } catch (HopFileNotFoundException e) {
      assertEquals(cause, e.getCause());
      assertTrue(e.getMessage().contains(causeExceptionMessage));
      assertNull(e.getFilepath());
    }
  }

  @Test
  void testConstructorMessageAndThrowable() {
    Throwable throwable = new RuntimeException(causeExceptionMessage);
    try {
      throw new HopFileNotFoundException(errorMessage, throwable);
    } catch (HopFileNotFoundException e) {
      assertTrue(e.getMessage().contains(errorMessage));
      assertTrue(e.getMessage().contains(causeExceptionMessage));
      assertEquals(throwable, e.getCause());
      assertNull(e.getFilepath());
    }
  }
}
