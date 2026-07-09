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

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import org.junit.jupiter.api.Test;

class ContentEditorTm4eSupportTest {

  @Test
  void scopeForLanguage_python_returnsSourcePython() {
    assertEquals("source.python", ContentEditorTm4eSupport.scopeForLanguage("python"));
  }

  @Test
  void scopeForLanguage_py_returnsSourcePython() {
    assertEquals("source.python", ContentEditorTm4eSupport.scopeForLanguage("py"));
  }

  @Test
  void pythonGrammarResourceIsOnClasspath() throws Exception {
    try (InputStream in =
        ContentEditorTm4eSupport.class.getResourceAsStream("grammars/python.json")) {
      assertNotNull(in, "grammars/python.json should be on the classpath");
      assertTrue(in.read() >= 0, "python.json should not be empty");
    }
  }
}
