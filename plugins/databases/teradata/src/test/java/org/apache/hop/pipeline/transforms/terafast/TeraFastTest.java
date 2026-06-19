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

package org.apache.hop.pipeline.transforms.terafast;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TeraFastTest {

  @BeforeEach
  void setUp() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testCreateCommandLineThrowsWhenFastloadPathBlank() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("test", new TeraFastMeta());
    pipelineMeta.addTransform(transformMeta);

    TeraFastMeta meta = new TeraFastMeta();
    meta.setFastloadPath("");
    TeraFastData data = new TeraFastData();
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);

    TeraFast transform = new TeraFast(transformMeta, meta, data, 0, pipelineMeta, pipeline);

    HopException e = assertThrows(HopException.class, transform::createCommandLine);
    assertTrue(
        e.getMessage().contains("Fastload path not set") || e.getMessage().contains("fastload"));
  }

  @Test
  void testCreateCommandLineWithValidPath() throws Exception {
    String path = new File(System.getProperty("java.io.tmpdir")).getAbsolutePath();

    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("test", new TeraFastMeta());
    pipelineMeta.addTransform(transformMeta);

    TeraFastMeta meta = new TeraFastMeta();
    meta.setFastloadPath(path);
    TeraFastData data = new TeraFastData();
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);

    TeraFast transform = new TeraFast(transformMeta, meta, data, 0, pipelineMeta, pipeline);

    String cmd = transform.createCommandLine();

    assertNotNull(cmd);
    assertTrue(cmd.contains(path) || cmd.replace('\\', '/').contains(path.replace('\\', '/')));
  }

  @Test
  void testCreateCommandLineWithLogFile() throws Exception {
    String fastloadPath = new File(System.getProperty("java.io.tmpdir")).getAbsolutePath();
    String logPath =
        new File(System.getProperty("java.io.tmpdir"), "terafast.log").getAbsolutePath();

    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("test", new TeraFastMeta());
    pipelineMeta.addTransform(transformMeta);

    TeraFastMeta meta = new TeraFastMeta();
    meta.setFastloadPath(fastloadPath);
    meta.setLogFile(logPath);
    TeraFastData data = new TeraFastData();
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);

    TeraFast transform = new TeraFast(transformMeta, meta, data, 0, pipelineMeta, pipeline);

    String cmd = transform.createCommandLine();

    assertNotNull(cmd);
    assertTrue(cmd.contains("-e"));
    assertTrue(
        cmd.contains(logPath) || cmd.replace('\\', '/').contains(logPath.replace('\\', '/')));
  }
}
