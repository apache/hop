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
package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class UserDefinedJavaClassCodeSnippetsTest {

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  @Test
  void getSnippetsHelper_returnsNonNull() throws Exception {
    assertNotNull(UserDefinedJavaClassCodeSnippets.getSnippetsHelper());
  }

  @Test
  void getSnippetsHelper_singleton_returnsSameInstance() throws Exception {
    UserDefinedJavaClassCodeSnippets first = UserDefinedJavaClassCodeSnippets.getSnippetsHelper();
    UserDefinedJavaClassCodeSnippets second = UserDefinedJavaClassCodeSnippets.getSnippetsHelper();
    assertEquals(first, second);
  }

  @Test
  void getSnippets_isNotEmpty() throws Exception {
    assertFalse(UserDefinedJavaClassCodeSnippets.getSnippetsHelper().getSnippets().isEmpty());
  }

  @Test
  void getDefaultCode_isNotEmpty() throws Exception {
    String code = UserDefinedJavaClassCodeSnippets.getSnippetsHelper().getDefaultCode();
    assertNotNull(code);
    assertFalse(code.isEmpty());
  }

  @Test
  void getCode_knownSnippet_returnsNonEmpty() throws Exception {
    UserDefinedJavaClassCodeSnippets helper = UserDefinedJavaClassCodeSnippets.getSnippetsHelper();
    // "Implement processRow" is the canonical default snippet
    String code = helper.getCode("Implement processRow");
    assertNotNull(code);
    assertFalse(code.isEmpty(), "Expected code for 'Implement processRow' but got empty");
  }

  @Test
  void getCode_unknownSnippet_returnsEmpty() throws Exception {
    String code = UserDefinedJavaClassCodeSnippets.getSnippetsHelper().getCode("NONEXISTENT");
    assertEquals("", code);
  }

  @Test
  void getSample_unknownSnippet_returnsEmpty() throws Exception {
    String sample = UserDefinedJavaClassCodeSnippets.getSnippetsHelper().getSample("NONEXISTENT");
    assertEquals("", sample);
  }

  @Test
  void getSnippets_isUnmodifiable() throws Exception {
    assertThrows(
        UnsupportedOperationException.class,
        () -> UserDefinedJavaClassCodeSnippets.getSnippetsHelper().getSnippets().clear());
  }
}
