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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ShellRcWriterTest {

  @Test
  void insertsBlockAtEnd() throws Exception {
    Map<String, String> vars = Map.of("HOP_CONFIG_FOLDER", "/home/alice/.local/share/hop");
    String next = ShellRcWriter.upsert("export PATH=/usr/bin\n", vars);
    assertTrue(next.startsWith("export PATH=/usr/bin\n"));
    assertTrue(next.contains(ShellRcWriter.BEGIN));
    assertTrue(next.contains("export HOP_CONFIG_FOLDER='/home/alice/.local/share/hop'"));
    assertTrue(next.contains(ShellRcWriter.END));
  }

  @Test
  void replacesExistingBlock() throws Exception {
    String existing =
        "before\n"
            + ShellRcWriter.BEGIN
            + "\nexport HOP_CONFIG_FOLDER='/old'\n"
            + ShellRcWriter.END
            + "\nafter\n";
    Map<String, String> vars = new LinkedHashMap<>();
    vars.put("HOP_CONFIG_FOLDER", "/new");
    vars.put("HOP_AUDIT_FOLDER", "/audit");
    String next = ShellRcWriter.upsert(existing, vars);
    assertTrue(next.startsWith("before\n"));
    assertTrue(next.endsWith("after\n"));
    assertTrue(next.contains("export HOP_CONFIG_FOLDER='/new'"));
    assertTrue(next.contains("export HOP_AUDIT_FOLDER='/audit'"));
    assertFalse(next.contains("/old"));
    assertEquals(1, next.split(ShellRcWriter.BEGIN, -1).length - 1);
  }

  @Test
  void omitsEmptyValues() throws Exception {
    Map<String, String> vars = new LinkedHashMap<>();
    vars.put("HOP_CONFIG_FOLDER", "/cfg");
    vars.put("HOP_JAVA_HOME", "");
    String block = ShellRcWriter.renderBlock(vars);
    assertTrue(block.contains("HOP_CONFIG_FOLDER"));
    assertFalse(block.contains("HOP_JAVA_HOME"));
  }
}
