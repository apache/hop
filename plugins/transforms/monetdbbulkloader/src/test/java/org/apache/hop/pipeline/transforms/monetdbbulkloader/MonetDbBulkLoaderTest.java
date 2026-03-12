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

package org.apache.hop.pipeline.transforms.monetdbbulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test for MonetDbBulkLoader (excluding dialog). */
class MonetDbBulkLoaderTest {

  @BeforeEach
  void setUp() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testInitFailsWhenNoConnection() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("test", new MonetDbBulkLoaderMeta());
    pipelineMeta.addTransform(transformMeta);

    MonetDbBulkLoaderMeta meta = new MonetDbBulkLoaderMeta();
    MonetDbBulkLoaderData data = new MonetDbBulkLoaderData();
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);

    MonetDbBulkLoader transform =
        new MonetDbBulkLoader(transformMeta, meta, data, 0, pipelineMeta, pipeline);

    assertFalse(transform.init());
  }

  @Test
  void testEscapeOsPathSpacesOnUnix() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("test", new MonetDbBulkLoaderMeta());
    pipelineMeta.addTransform(transformMeta);

    MonetDbBulkLoaderMeta meta = new MonetDbBulkLoaderMeta();
    MonetDbBulkLoaderData data = new MonetDbBulkLoaderData();
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);

    MonetDbBulkLoader transform =
        new MonetDbBulkLoader(transformMeta, meta, data, 0, pipelineMeta, pipeline);

    String result = transform.escapeOsPath("/path with spaces/file", false);
    assertEquals("/path\\ with\\ spaces/file", result);
  }

  @Test
  void testEscapeOsPathSpacesOnWindows() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("test", new MonetDbBulkLoaderMeta());
    pipelineMeta.addTransform(transformMeta);

    MonetDbBulkLoaderMeta meta = new MonetDbBulkLoaderMeta();
    MonetDbBulkLoaderData data = new MonetDbBulkLoaderData();
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);

    MonetDbBulkLoader transform =
        new MonetDbBulkLoader(transformMeta, meta, data, 0, pipelineMeta, pipeline);

    String result = transform.escapeOsPath("C:\\path with spaces\\file", true);
    assertEquals("C:\\path^ with^ spaces\\file", result);
  }

  @Test
  void testEscapeOsPathNoSpaces() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("test", new MonetDbBulkLoaderMeta());
    pipelineMeta.addTransform(transformMeta);

    MonetDbBulkLoaderMeta meta = new MonetDbBulkLoaderMeta();
    MonetDbBulkLoaderData data = new MonetDbBulkLoaderData();
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);

    MonetDbBulkLoader transform =
        new MonetDbBulkLoader(transformMeta, meta, data, 0, pipelineMeta, pipeline);

    String result = transform.escapeOsPath("/path/nospaces", false);
    assertEquals("/path/nospaces", result);
  }
}
