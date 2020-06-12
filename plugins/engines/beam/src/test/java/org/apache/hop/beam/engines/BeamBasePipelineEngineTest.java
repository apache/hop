package org.apache.hop.beam.engines;

import org.apache.hop.beam.transform.PipelineTestBase;
import org.apache.hop.core.exception.HopException;
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
    IPipelineEngine<PipelineMeta> engine = PipelineEngineFactory.createPipelineEngine( runConfigurationName, metadataProvider, pipelineMeta );
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