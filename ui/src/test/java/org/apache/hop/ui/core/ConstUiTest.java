/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConstUiTest {

  @Test
  void encodingListStartsWithAnEmptyEntry() {
    String[] encodings = ConstUi.getEncodings();

    assertEquals("", encodings[0], "The first encoding must be empty so it can be cleared again");
    assertEquals(Charset.availableCharsets().size() + 1, encodings.length);
  }

  @Test
  void encodingListContainsTheAvailableCharsets() {
    List<String> encodings = Arrays.asList(ConstUi.getEncodings());

    for (Charset charset : Charset.availableCharsets().values()) {
      assertTrue(
          encodings.contains(charset.displayName()),
          () -> "Encoding " + charset.displayName() + " is missing from the list");
    }
  }
}
