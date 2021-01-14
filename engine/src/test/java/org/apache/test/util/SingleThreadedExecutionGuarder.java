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

package org.apache.test.util;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * This is a base class for creating guard tests, that check a transform cannot be executed in the single-threaded mode
 *
 * @author Andrey Khayrutdinov
 */
public abstract class SingleThreadedExecutionGuarder<Meta extends ITransformMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUp() throws Exception {
    HopEnvironment.init();
  }

  protected abstract Meta createMeta();

  @Test( expected = HopException.class )
  public void failsWhenGivenNonSingleThreadTransforms() throws Exception {
    Meta metaInterface = createMeta();

    PluginRegistry plugReg = PluginRegistry.getInstance();
    String id = plugReg.getPluginId( TransformPluginType.class, metaInterface );
    assertNotNull( "pluginId", id );

    TransformMeta transformMeta = new TransformMeta( id, "transformMetrics", metaInterface );

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "failsWhenGivenNonSingleThreadTransforms" );
    pipelineMeta.addTransform( transformMeta );

    Pipeline pipeline = new LocalPipelineEngine( pipelineMeta );
    pipeline.prepareExecution();

    SingleThreadedPipelineExecutor executor = new SingleThreadedPipelineExecutor( pipeline );
    executor.init();
  }
}
