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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopGuiCommandLineTest {

  @TempDir Path folder;

  @Test
  void findOptionSupportsEqualsAndSeparateValue() {
    assertEquals(
        "ttt",
        HopGuiCommandLine.findOption(List.of("-j", "ttt"), HopGuiCommandLine.PROJECT_OPTION_NAMES));
    assertEquals(
        "ttt",
        HopGuiCommandLine.findOption(
            List.of("--project=ttt"), HopGuiCommandLine.PROJECT_OPTION_NAMES));
    assertEquals(
        "ttt",
        HopGuiCommandLine.findOption(
            List.of("-project=ttt"), HopGuiCommandLine.PROJECT_OPTION_NAMES));
    assertEquals(
        "prod",
        HopGuiCommandLine.findOption(
            List.of("-e", "prod"), HopGuiCommandLine.ENVIRONMENT_OPTION_NAMES));
  }

  @Test
  void findOptionDoesNotMatchProjectLocationsAsProject() {
    assertNull(
        HopGuiCommandLine.findOption(
            List.of("--project-locations=ttt=/tmp/p"), HopGuiCommandLine.PROJECT_OPTION_NAMES));
  }

  @Test
  void takeOptionRemovesFileFlag() {
    List<String> args = new ArrayList<>(List.of("-j", "ttt", "-file=/tmp/a.hpl", "-x"));
    String file = HopGuiCommandLine.takeOption(args, HopGuiCommandLine.FILE_OPTION_NAMES);
    assertEquals("/tmp/a.hpl", file);
    assertEquals(List.of("-j", "ttt", "-x"), args);
  }

  @Test
  void resolveFileUsesProjectHomeWhenRelative() throws Exception {
    Path projectHome = folder.resolve("proj");
    Files.createDirectories(projectHome);
    Path pipeline = projectHome.resolve("long-running-test.hpl");
    Files.writeString(pipeline, "<pipeline/>");

    Variables variables = new Variables();
    variables.setVariable("PROJECT_HOME", projectHome.toAbsolutePath().toString());

    String resolved = HopGuiCommandLine.resolveFile(variables, "long-running-test.hpl");
    assertTrue(resolved.replace('\\', '/').endsWith("long-running-test.hpl"));
    assertTrue(resolved.contains("proj"));
  }
}
