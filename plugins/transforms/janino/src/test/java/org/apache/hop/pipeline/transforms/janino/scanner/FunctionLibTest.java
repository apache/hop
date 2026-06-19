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
package org.apache.hop.pipeline.transforms.janino.scanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.pipeline.transforms.janino.function.FunctionDescription;
import org.apache.hop.pipeline.transforms.janino.function.FunctionLib;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class FunctionLibTest {
  // note: using mockito since current IoC design is not very friendly to tests
  //       with the static singleton pattern
  @Test
  void scansAndPopulatesFunctionsFromClasspath(@TempDir Path tempDir) throws Exception {
    Path classesDir = tempDir.resolve("classes");
    FunctionGenerator.writeToDirectory(classesDir);

    try (URLClassLoader scanCl =
        new URLClassLoader(
            new URL[] {classesDir.toUri().toURL()}, FunctionLibTest.class.getClassLoader())) {
      IPlugin plugin = mock(IPlugin.class);
      PluginRegistry registry = mock(PluginRegistry.class);
      when(registry.getPlugin(TransformPluginType.class, "Janino")).thenReturn(plugin);
      when(registry.getClassLoader(plugin)).thenReturn(scanCl);

      try (MockedStatic<PluginRegistry> mocked = mockStatic(PluginRegistry.class)) {
        mocked.when(PluginRegistry::getInstance).thenReturn(registry);

        FunctionLib lib = new FunctionLib();
        List<FunctionDescription> functions = lib.getFunctions();

        assertFalse(functions.isEmpty(), "Should find at least one @JaninoFunction method");
        assertTrue(
            functions.stream().anyMatch(f -> "nvl".equals(f.getName())),
            "Should include the nvl method");

        FunctionDescription nvl =
            functions.stream().filter(f -> "nvl".equals(f.getName())).findFirst().get();
        assertEquals("General", nvl.getCategory());
        assertEquals("String", nvl.getReturns());
        assertNotNull(nvl.getFunctionExamples());
        assertFalse(nvl.getFunctionExamples().isEmpty());
      }
    }
  }
}
