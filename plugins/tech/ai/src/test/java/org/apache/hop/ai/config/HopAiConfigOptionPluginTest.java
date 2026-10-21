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

package org.apache.hop.ai.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class HopAiConfigOptionPluginTest {

  @Test
  void contextFilesFromTheCommandLineAreOnePathPerLine() {
    assertEquals(
        "${PROJECT_HOME}/ai.md\n/etc/hop/notes.md",
        HopAiConfigOptionPlugin.contextFilesFromCommandLine(
            "${PROJECT_HOME}/ai.md, /etc/hop/notes.md"));
    assertEquals("a.md\nb.md", HopAiConfigOptionPlugin.contextFilesFromCommandLine("a.md\nb.md\n"));
    assertEquals("", HopAiConfigOptionPlugin.contextFilesFromCommandLine(""));
  }

  @Test
  void anAddedFileGoesOnALineOfItsOwn() {
    assertEquals("b.md", HopAiConfigOptionPlugin.appendPath("", "b.md"));
    assertEquals("a.md\nb.md", HopAiConfigOptionPlugin.appendPath("a.md\n", "b.md"));
    assertEquals("b.md", HopAiConfigOptionPlugin.appendPath(null, "b.md"));
  }

  @Test
  void aFileInTheProjectIsWrittenWithProjectHome() {
    Variables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "/work/sales");
    assertEquals(
        "${PROJECT_HOME}/notes/ai.md",
        HopAiConfigOptionPlugin.projectRelative("/work/sales/notes/ai.md", variables));
    assertEquals(
        "/work/salesforce/ai.md",
        HopAiConfigOptionPlugin.projectRelative("/work/salesforce/ai.md", variables));
    assertEquals("/tmp/ai.md", HopAiConfigOptionPlugin.projectRelative("/tmp/ai.md", null));
  }
}
