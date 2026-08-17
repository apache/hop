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

package org.apache.hop.setup.persist;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WindowsUserEnvironmentWriterTest {

  @Test
  void buildsSetAndClearStatements() throws Exception {
    Map<String, String> vars = new LinkedHashMap<>();
    vars.put("HOP_CONFIG_FOLDER", "C:\\Users\\alice\\.hop\\config");
    vars.put("HOP_JAVA_HOME", "");
    WindowsUserEnvironmentWriter writer = new WindowsUserEnvironmentWriter(command -> 0);
    String ps = writer.renderCommand(vars);
    assertTrue(
        ps.contains(
            "[Environment]::SetEnvironmentVariable('HOP_CONFIG_FOLDER','C:\\Users\\alice\\.hop\\config','User')"));
    assertTrue(ps.contains("[Environment]::SetEnvironmentVariable('HOP_JAVA_HOME',$null,'User')"));
  }

  @Test
  void applyInvokesPowerShellWithoutSetx() throws Exception {
    List<List<String>> captured = new ArrayList<>();
    WindowsUserEnvironmentWriter writer =
        new WindowsUserEnvironmentWriter(
            command -> {
              captured.add(command);
              return 0;
            });
    writer.apply(Map.of("HOP_CONFIG_FOLDER", "C:\\hop\\config"));
    assertEquals(1, captured.size());
    assertEquals("powershell.exe", captured.get(0).get(0));
    assertTrue(captured.get(0).contains("-NoProfile"));
    assertTrue(captured.get(0).stream().noneMatch(part -> part.contains("setx")));
  }
}
