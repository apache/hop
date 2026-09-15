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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EnvScriptWriterTest {

  @Test
  void unixScriptSetsOnlyWhenUnset() throws Exception {
    Map<String, String> vars = Map.of("HOP_CONFIG_FOLDER", "/home/alice/.local/share/hop");
    String script = EnvScriptWriter.unixScript(vars);
    assertTrue(
        script.contains(
            "if [ -z \"${HOP_CONFIG_FOLDER}\" ]; then export HOP_CONFIG_FOLDER='/home/alice/.local/share/hop'; fi"));
  }

  @Test
  void windowsScriptSetsOnlyWhenUndefined() throws Exception {
    Map<String, String> vars = Map.of("HOP_CONFIG_FOLDER", "C:\\Users\\alice\\.hop\\config");
    String script = EnvScriptWriter.windowsScript(vars);
    assertTrue(script.contains("@echo off"));
    assertTrue(
        script.contains(
            "if not defined HOP_CONFIG_FOLDER set \"HOP_CONFIG_FOLDER=C:\\Users\\alice\\.hop\\config\""));
  }

  @Test
  void windowsScriptWritesOptionsContainingDoubleQuotes() throws Exception {
    Map<String, String> vars =
        Map.of("HOP_OPTIONS", "-Xmx2048m -DHOP_SHARED_JDBC_FOLDERS=\"C:\\java\\hop\\jdbc-shared\"");
    String script = EnvScriptWriter.windowsScript(vars);
    assertTrue(
        script.contains(
            "if not defined HOP_OPTIONS set HOP_OPTIONS=-Xmx2048m"
                + " -DHOP_SHARED_JDBC_FOLDERS=\"C:\\java\\hop\\jdbc-shared\""));
  }

  @Test
  void emptyValuesAreOmitted() throws Exception {
    Map<String, String> vars = new LinkedHashMap<>();
    vars.put("HOP_CONFIG_FOLDER", "/cfg");
    vars.put("HOP_OPTIONS", "");
    assertFalse(EnvScriptWriter.unixScript(vars).contains("HOP_OPTIONS"));
    assertFalse(EnvScriptWriter.windowsScript(vars).contains("HOP_OPTIONS"));
  }
}
