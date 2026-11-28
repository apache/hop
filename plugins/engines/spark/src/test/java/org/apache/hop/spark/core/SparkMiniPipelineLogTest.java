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

package org.apache.hop.spark.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** What the transforms of a mini-pipeline log lands in the Spark pipeline's log, per partition. */
class SparkMiniPipelineLogTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
  }

  @Test
  void aTransformOnPartitionThreeLogsIntoTheSparkPipelineLogAsCopyThree() throws Exception {
    LogChannel sparkPipelineLog = new LogChannel(new LoggingObject("spark-pipeline"));
    ILoggingObject parent =
        HopMapPartitionsFn.miniPipelineParent(sparkPipelineLog.getLogChannelId());
    assertEquals(sparkPipelineLog.getLogChannelId(), parent.getLogChannelId());

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("mini");
    TransformMeta transformMeta = new TransformMeta("partitioned", new DummyMeta());
    transformMeta.setTransformPluginId("Dummy");
    pipelineMeta.addTransform(transformMeta);
    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipelineMeta, new Variables(), parent);
    pipeline.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    pipeline.prepareExecution();

    HopMapPartitionsFn.logAsPartition(pipeline, 3);
    ITransform transform = pipeline.getTransforms().get(0).transform;
    transform.getLogChannel().logBasic("id = 7");

    String logText =
        HopLogStore.getAppender().getBuffer(sparkPipelineLog.getLogChannelId(), true).toString();
    assertTrue(logText.contains("partitioned.3 - id = 7"), logText);
  }

  @Test
  void anExecutorWithoutTheSparkPipelineGetsItsOwnParent() {
    ILoggingObject parent = HopMapPartitionsFn.miniPipelineParent("not-in-this-jvm");

    assertEquals("apache-spark-transform", parent.getObjectName());
  }
}
