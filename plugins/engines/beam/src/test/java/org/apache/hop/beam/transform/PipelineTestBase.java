package org.apache.hop.beam.transform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.apache.hop.beam.core.BeamHop;
import BeamJobConfig;
import RunnerType;
import org.apache.hop.beam.pipeline.PipelineMetaPipelineConverter;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;

import java.io.File;
import java.util.ArrayList;

public class PipelineTestBase {

  protected IMetaStore metaStore;

  @Before
  public void setUp() throws Exception {
    BeamHop.init( new ArrayList<>(), new ArrayList<>() );

    metaStore = new MemoryMetaStore();

    File inputFolder = new File( "/tmp/customers/io" );
    inputFolder.mkdirs();
    File outputFolder = new File( "/tmp/customers/output" );
    outputFolder.mkdirs();
    File tmpFolder = new File( "/tmp/customers/tmp" );
    tmpFolder.mkdirs();

    FileUtils.copyFile( new File( "src/test/resources/customers/customers-100.txt" ), new File( "/tmp/customers/io/customers-100.txt" ) );
    FileUtils.copyFile( new File( "src/test/resources/customers/state-data.txt" ), new File( "/tmp/customers/io/state-data.txt" ) );
  }


  @Ignore
  public void createRunPipeline( PipelineMeta pipelineMeta ) throws Exception {

    /*
    FileOutputStream fos = new FileOutputStream( "/tmp/"+pipelineMeta.getName()+".ktr" );
    fos.write( pipelineMeta.getXML().getBytes() );
    fos.close();
    */

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

    pipelineOptions.setJobName( pipelineMeta.getName() );
    pipelineOptions.setUserAgent( BeamConst.STRING_KETTLE_BEAM );

    BeamJobConfig jobConfig = new BeamJobConfig();
    jobConfig.setName("Direct runner test");
    jobConfig.setRunnerTypeName( RunnerType.Direct.name() );

    // No extra plugins to load : null option
    PipelineMetaPipelineConverter converter = new PipelineMetaPipelineConverter( pipelineMeta, metaStore, (String) null, jobConfig );
    Pipeline pipeline = converter.createPipeline( pipelineOptions );

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();

    MetricResults metricResults = pipelineResult.metrics();

    MetricQueryResults allResults = metricResults.queryMetrics( MetricsFilter.builder().build() );
    for ( MetricResult<Long> result : allResults.getCounters() ) {
      System.out.println( "Name: " + result.getName() + " Attempted: " + result.getAttempted() );
    }
  }
}