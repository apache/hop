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

package org.apache.hop.fatjar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.config.GenerateFatJarConfigPlugin;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class FatJarEngineTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void looksUpEnginesByCodeIgnoringCase() {
    assertEquals(FatJarEngine.SPARK, FatJarEngine.lookup("spark"));
    assertEquals(FatJarEngine.SPARK, FatJarEngine.lookup(" SPARK "));
    assertEquals(FatJarEngine.BEAM, FatJarEngine.lookup("Beam"));
    assertNull(FatJarEngine.lookup("flink"));
    assertNull(FatJarEngine.lookup(null));
    assertEquals("SPARK", FatJarEngine.SPARK.getCode());
    assertEquals("SPARK", FatJarEngine.SPARK.toString());
  }

  @Test
  void anUnknownEngineIsRefused() {
    GenerateFatJarConfigPlugin plugin = new GenerateFatJarConfigPlugin();
    plugin.setFatJarFilename("/tmp/never-written.jar");
    plugin.setFatJarEngine("flink");
    HopException e =
        assertThrows(
            HopException.class,
            () -> plugin.handleOption(LogChannel.GENERAL, null, new Variables()));
    assertTrue(e.getMessage().contains("'spark' or 'beam'"), e.getMessage());
  }

  @Test
  void anEngineWithoutItsPluginIsRefused() {
    // No engine plugins are registered in this test JVM
    assertTrue(FatJarGenerator.availableEngines().isEmpty());
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                FatJarGenerator.generate(
                    LogChannel.GENERAL,
                    new Variables(),
                    FatJarEngine.SPARK,
                    "/tmp/never-written.jar",
                    null,
                    false));
    assertTrue(e.getMessage().contains("SparkPipelineEngine"), e.getMessage());
  }

  @Test
  void withoutAFilenameNothingHappens() throws Exception {
    assertFalse(
        new GenerateFatJarConfigPlugin().handleOption(LogChannel.GENERAL, null, new Variables()));
  }
}
