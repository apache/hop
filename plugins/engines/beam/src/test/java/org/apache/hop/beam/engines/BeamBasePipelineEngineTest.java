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

package org.apache.hop.beam.engines;

import org.apache.hop.beam.transform.PipelineTestBase;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BeamBasePipelineEngineTest extends PipelineTestBase {

  protected IPipelineEngine<PipelineMeta> createAndExecutePipeline( String runConfigurationName, IHopMetadataProvider metadataProvider, PipelineMeta pipelineMeta ) throws HopException {
    IPipelineEngine<PipelineMeta> engine = PipelineEngineFactory.createPipelineEngine( variables, runConfigurationName, metadataProvider, pipelineMeta );
    engine.prepareExecution();
    engine.startThreads();
    engine.waitUntilFinished();
    return engine;
  }

  protected void validateInputOutputEngineMetrics( IPipelineEngine<PipelineMeta> engine ) throws Exception {
    assertEquals("No errors expected", 0, engine.getErrors());
    EngineMetrics engineMetrics = engine.getEngineMetrics();
    assertNotNull("Engine metrics can't be null", engineMetrics);
    List<IEngineComponent> components = engineMetrics.getComponents();
    assertNotNull("Engine metrics needs to have a list of components", components);

    assertEquals(3, components.size());
    IEngineComponent inputComponent = engine.findComponent( "INPUT", 0 );
    assertNotNull(inputComponent);
    assertEquals(100, inputComponent.getLinesInput());

    IEngineComponent dummyComponent = engine.findComponent( "Dummy", 0 );
    assertNotNull(dummyComponent);
    assertEquals(100, dummyComponent.getLinesRead());
    assertEquals(100, dummyComponent.getLinesWritten());

    IEngineComponent outputComponent = engine.findComponent( "OUTPUT", 0 );
    assertNotNull(outputComponent);
    assertEquals(100, outputComponent.getLinesOutput());
  }

}
