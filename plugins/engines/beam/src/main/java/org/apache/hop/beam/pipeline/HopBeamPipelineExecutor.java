package org.apache.hop.beam.pipeline;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hop.beam.core.metastore.SerializableMetaStore;
import org.apache.hop.beam.metastore.BeamJobConfig;
import org.apache.hop.beam.metastore.JobParameter;
import org.apache.hop.beam.metastore.RunnerType;
import org.apache.hop.beam.pipeline.fatjar.FatJarBuilder;
import org.apache.hop.beam.pipeline.spark.MainSpark;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.metastore.api.IMetaStore;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter.findAnnotatedClasses;

public class HopBeamPipelineExecutor {

  private ILogChannel logChannel;
  private PipelineMeta pipelineMeta;
  private BeamJobConfig jobConfig;
  private IMetaStore metaStore;
  private ClassLoader classLoader;
  private List<BeamMetricsUpdatedListener> updatedListeners;
  private boolean loggingMetrics;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;
  private JavaSparkContext sparkContext;

  private HopBeamPipelineExecutor() {
    this.updatedListeners = new ArrayList<>();
  }

  public HopBeamPipelineExecutor( ILogChannel log, PipelineMeta pipelineMeta, BeamJobConfig jobConfig, IMetaStore metaStore, ClassLoader classLoader ) {
    this(log, pipelineMeta, jobConfig, metaStore, classLoader, null, null);
  }

  public HopBeamPipelineExecutor( ILogChannel log, PipelineMeta pipelineMeta, BeamJobConfig jobConfig, IMetaStore metaStore, ClassLoader classLoader, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this();
    this.logChannel = log;
    this.pipelineMeta = pipelineMeta;
    this.jobConfig = jobConfig;
    this.metaStore = metaStore;
    this.classLoader = classLoader;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
    this.loggingMetrics = true;
  }

  public PipelineResult execute() throws HopException {
    return execute( false );
  }

  public PipelineResult execute( boolean server ) throws HopException {

    if (StringUtils.isEmpty(jobConfig.getRunnerTypeName())) {
      throw new HopException( "Please specify a runner type" );
    }
    RunnerType runnerType = RunnerType.getRunnerTypeByName( pipelineMeta.environmentSubstitute( jobConfig.getRunnerTypeName() ) );
    if (runnerType==null) {
      throw new HopException( "Runner type '"+jobConfig.getRunnerTypeName()+"' is not recognized");
    }
    return executePipeline();
  }

  private PipelineResult executeSpark() throws HopException {
    try {

      // Write our artifacts to the deploy folder of the spark environment
      //
      String deployFolder = pipelineMeta.environmentSubstitute( jobConfig.getSparkDeployFolder() );
      if ( !deployFolder.endsWith( File.separator ) ) {
        deployFolder += File.separator;
      }

      // The transformation
      //
      String shortTransformationFilename = "transformation.ktr";
      String transformationFilename = deployFolder + shortTransformationFilename;
      FileUtils.copyFile( new File( pipelineMeta.getFilename() ), new File( transformationFilename ) );

      // Serialize PipelineMeta and MetaStore, set as variables...
      //
      SerializableMetaStore serializableMetaStore = new SerializableMetaStore( metaStore );
      String shortMetastoreJsonFilename = "metastore.json"; ;
      String metastoreJsonFilename = deployFolder + shortMetastoreJsonFilename;
      FileUtils.writeStringToFile( new File( metastoreJsonFilename ), serializableMetaStore.toJson(), "UTF-8" );

      // Create a fat jar...
      //
      // Find the list of jar files to stage...
      //
      List<String> libraryFilesToStage;
      if (StringUtils.isNotEmpty(jobConfig.getFatJar())) {
        libraryFilesToStage = new ArrayList<>(  );
        libraryFilesToStage.add(jobConfig.getFatJar());
      } else {
        libraryFilesToStage = BeamConst.findLibraryFilesToStage( null, pipelineMeta.environmentSubstitute( jobConfig.getPluginsToStage() ), true, true );
      }

      String shortFatJarFilename = "kettle-beam-fat.jar";
      String fatJarFilename = deployFolder + shortFatJarFilename;

      FatJarBuilder fatJarBuilder = new FatJarBuilder( fatJarFilename, libraryFilesToStage );
      // if (!new File(fatJarBuilder.getTargetJarFile()).exists()) {
      fatJarBuilder.buildTargetJar();
      // }

      String master = pipelineMeta.environmentSubstitute( jobConfig.getSparkMaster() );

      // Figure out the list of transform and XP plugin classes...
      //
      StringBuilder stepPluginClasses = new StringBuilder();
      StringBuilder xpPluginClasses = new StringBuilder();

      String pluginsToStage = jobConfig.getPluginsToStage();
      if (!pluginsToStage.contains( "kettle-beam" )) {
        if ( StringUtils.isEmpty( pluginsToStage ) ) {
          pluginsToStage = "kettle-beam";
        } else {
          pluginsToStage = "kettle-beam,"+pluginsToStage;
        }
      }

      if ( StringUtils.isNotEmpty( pluginsToStage ) ) {
        String[] pluginFolders = pluginsToStage.split( "," );
        for ( String pluginFolder : pluginFolders ) {
          List<String> stepClasses = findAnnotatedClasses( pluginFolder, Transform.class.getName() );
          for (String stepClass : stepClasses) {
            if (stepPluginClasses.length()>0) {
              stepPluginClasses.append(",");
            }
            stepPluginClasses.append(stepClass);
          }
          List<String> xpClasses = findAnnotatedClasses( pluginFolder, ExtensionPoint.class.getName() );
          for (String xpClass : xpClasses) {
            if (xpPluginClasses.length()>0) {
              xpPluginClasses.append(",");
            }
            xpPluginClasses.append(xpClass);
          }
        }
      }

      // Write the spark-submit command
      //
      StringBuilder command = new StringBuilder();
      command.append( "spark-submit" ).append( " \\\n" );
      command.append( " --class " ).append( MainSpark.class.getName() ).append( " \\\n" );
      command.append( " --master " ).append( master ).append( " \\\n" );
      command.append( " --deploy-mode cluster" ).append( " \\\n" );
      command.append( " --files " ).append( shortTransformationFilename ).append( "," ).append( shortMetastoreJsonFilename ).append( " \\\n" );
      command.append( " " ).append( shortFatJarFilename ).append( " \\\n" );
      command.append( " " ).append( shortTransformationFilename ).append( " \\\n" );
      command.append( " " ).append( shortMetastoreJsonFilename ).append( " \\\n" );
      command.append( " '" ).append( jobConfig.getName() ).append( "'" ).append( " \\\n" );
      command.append( " '" ).append( master ).append( "'" ).append( " \\\n" );
      command.append( " '" ).append( pipelineMeta.getName() ).append( "'" ).append(" \\\n");
      command.append( " " ).append( stepPluginClasses.toString() ).append(" \\\n");
      command.append( " " ).append( xpPluginClasses.toString() ).append(" \\\n");
      command.append( "\n" ).append( "\n" );

      // Write this command to file
      //
      String commandFilename = deployFolder + "submit-command.sh";
      FileUtils.writeStringToFile( new File( commandFilename ), command.toString() );

      // TODO, unify it all...
      //
      return null;
    } catch ( Exception e ) {
      throw new HopException( "Error executing transformation on Spark", e );
    }
  }

  private PipelineResult executePipeline() throws HopException {
    ClassLoader oldContextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Explain to various classes in the Beam API (@see org.apache.beam.sdk.io.FileSystems)
      // what the context classloader is.
      // Set it back when we're done here.
      //
      Thread.currentThread().setContextClassLoader( classLoader );

      final Pipeline pipeline = getPipeline( pipelineMeta, jobConfig );

      logChannel.logBasic( "Creation of Apache Beam pipeline is complete. Starting execution..." );

      // This next command can block on certain runners...
      //
      PipelineResult pipelineResult = asyncExecutePipeline(pipeline);

      Timer timer = new Timer();
      TimerTask timerTask = new TimerTask() {
        @Override public void run() {

          // Log the metrics...
          //
          if ( isLoggingMetrics() ) {
            logMetrics( pipelineResult );
          }

          // Update the listeners.
          //
          updateListeners( pipelineResult );
        }
      };
      // Every 5 seconds
      //
      timer.schedule( timerTask, 5000, 5000 );

      // Wait until we're done
      //
      pipelineResult.waitUntilFinish();

      timer.cancel();
      timer.purge();

      // Log the metrics at the end.
      logMetrics( pipelineResult );

      // Update a last time
      //
      updateListeners( pipelineResult );

      logChannel.logBasic( "  ----------------- End of Beam job " + pipeline.getOptions().getJobName() + " -----------------------" );

      return pipelineResult;
    } catch(Exception e) {
      throw new HopException( "Error building/executing pipeline", e );
    } finally {
      Thread.currentThread().setContextClassLoader( oldContextClassLoader );
    }

  }

  private PipelineResult asyncExecutePipeline( Pipeline pipeline ) throws HopException {

    RunnerType runnerType = RunnerType.getRunnerTypeByName( pipelineMeta.environmentSubstitute( jobConfig.getRunnerTypeName() ) );
    if (runnerType==null) {
      throw new HopException( "Runner type '"+jobConfig.getRunnerTypeName()+"' is not recognized");
    }
    switch ( runnerType ) {
      case Direct: return DirectRunner.fromOptions( pipeline.getOptions() ).run( pipeline );
      case Flink: return FlinkRunner.fromOptions(pipeline.getOptions()).run( pipeline );
      case DataFlow: return DataflowRunner.fromOptions( pipeline.getOptions() ).run( pipeline );
      case Spark: return SparkRunner.fromOptions( pipeline.getOptions() ).run( pipeline );
      default:
        throw new HopException( "Execution on runner '" + runnerType.name() + "' is not supported yet, sorry." );
    }
  }


  private void logMetrics( PipelineResult pipelineResult ) {
    MetricResults metricResults = pipelineResult.metrics();

    logChannel.logBasic( "  ----------------- Metrics refresh @ " + new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss" ).format( new Date() ) + " -----------------------" );

    MetricQueryResults allResults = metricResults.queryMetrics( MetricsFilter.builder().build() );
    for ( MetricResult<Long> result : allResults.getCounters() ) {
      logChannel.logBasic( "Name: " + result.getName() + " Attempted: " + result.getAttempted() );
    }
  }

  public void updateListeners( PipelineResult pipelineResult ) {
    for ( BeamMetricsUpdatedListener listener : updatedListeners ) {
      listener.beamMetricsUpdated( pipelineResult );
    }
  }

  public Pipeline getPipeline( PipelineMeta pipelineMeta, BeamJobConfig config ) throws HopException {

    try {

      if ( StringUtils.isEmpty( config.getRunnerTypeName() ) ) {
        throw new HopException( "You need to specify a runner type, one of : " + RunnerType.values().toString() );
      }
      PipelineOptions pipelineOptions = null;
      IVariables variables = pipelineMeta;

      RunnerType runnerType = RunnerType.getRunnerTypeByName( pipelineMeta.environmentSubstitute( config.getRunnerTypeName() ) );
      switch ( runnerType ) {
        case Direct:
          pipelineOptions = PipelineOptionsFactory.create();
          break;
        case DataFlow:
          DataflowPipelineOptions dfOptions = PipelineOptionsFactory.as( DataflowPipelineOptions.class );
          configureDataFlowOptions( config, dfOptions, variables );
          pipelineOptions = dfOptions;
          break;
        case Spark:
          SparkPipelineOptions sparkOptions;
          if (sparkContext!=null) {
            SparkContextOptions sparkContextOptions = PipelineOptionsFactory.as( SparkContextOptions.class );
            sparkContextOptions.setProvidedSparkContext( sparkContext );
            sparkOptions = sparkContextOptions;
          } else {
            sparkOptions = PipelineOptionsFactory.as( SparkPipelineOptions.class );
          }
          configureSparkOptions( config, sparkOptions, variables, pipelineMeta.getName() );
          pipelineOptions = sparkOptions;
          break;
        case Flink:
          FlinkPipelineOptions flinkOptions = PipelineOptionsFactory.as( FlinkPipelineOptions.class );
          configureFlinkOptions( config, flinkOptions, variables );
          pipelineOptions = flinkOptions;
          break;
        default:
          throw new HopException( "Sorry, this isn't implemented yet" );
      }

      configureStandardOptions( config, pipelineMeta.getName(), pipelineOptions, variables );

      setVariablesInTransformation( config, pipelineMeta );

      HopPipelineMetaToBeamPipelineConverter converter;
      if ( transformPluginClasses !=null && xpPluginClasses!=null) {
        converter = new HopPipelineMetaToBeamPipelineConverter( pipelineMeta, metaStore, transformPluginClasses, xpPluginClasses, jobConfig );
      } else {
        converter = new HopPipelineMetaToBeamPipelineConverter( pipelineMeta, metaStore, config.getPluginsToStage(), jobConfig );
      }
      Pipeline pipeline = converter.createPipeline( pipelineOptions );

      // Also set the pipeline options...
      //
      FileSystems.setDefaultPipelineOptions(pipelineOptions);

      return pipeline;
    } catch ( Exception e ) {
      throw new HopException( "Error configuring local Beam Engine", e );
    }

  }

  private void configureStandardOptions( BeamJobConfig config, String transformationName, PipelineOptions pipelineOptions, IVariables variables ) {
    if ( StringUtils.isNotEmpty( transformationName ) ) {
      String sanitizedName = transformationName.replaceAll( "[^-A-Za-z0-9]", "" )
        ;
      pipelineOptions.setJobName( sanitizedName );
    }
    if ( StringUtils.isNotEmpty( config.getUserAgent() ) ) {
      String userAgent = variables.environmentSubstitute( config.getUserAgent() );
      pipelineOptions.setUserAgent( userAgent );
    }
    if ( StringUtils.isNotEmpty( config.getTempLocation() ) ) {
      String tempLocation = variables.environmentSubstitute( config.getTempLocation() );
      pipelineOptions.setTempLocation( tempLocation );
    }
  }

  private void setVariablesInTransformation( BeamJobConfig config, PipelineMeta pipelineMeta ) {
    String[] parameters = pipelineMeta.listParameters();
    for ( JobParameter parameter : config.getParameters() ) {
      if ( StringUtils.isNotEmpty( parameter.getVariable() ) ) {
        if ( Const.indexOfString( parameter.getVariable(), parameters ) >= 0 ) {
          try {
            pipelineMeta.setParameterValue( parameter.getVariable(), parameter.getValue() );
          } catch ( UnknownParamException e ) {
            pipelineMeta.setVariable( parameter.getVariable(), parameter.getValue() );
          }
        } else {
          pipelineMeta.setVariable( parameter.getVariable(), parameter.getValue() );
        }
      }
    }
    pipelineMeta.activateParameters();
  }

  private void configureDataFlowOptions( BeamJobConfig config, DataflowPipelineOptions options, IVariables variables ) throws IOException {

    List<String> files;
    if (StringUtils.isNotEmpty(config.getFatJar())) {
      files = new ArrayList<>(  );
      files.add(config.getFatJar());
    } else {
      files = BeamConst.findLibraryFilesToStage( null, variables.environmentSubstitute( config.getPluginsToStage() ), true, true );
      files.removeIf( s-> s.contains( "commons-logging" ) || s.contains( "log4j" ) );
    }

    options.setFilesToStage( files );
    options.setProject( variables.environmentSubstitute( config.getGcpProjectId() ) );
    options.setAppName( variables.environmentSubstitute( config.getGcpAppName() ) );
    options.setStagingLocation( variables.environmentSubstitute( config.getGcpStagingLocation() ) );
    if ( StringUtils.isNotEmpty( config.getGcpInitialNumberOfWorkers() ) ) {
      int numWorkers = Const.toInt( variables.environmentSubstitute( config.getGcpInitialNumberOfWorkers() ), -1 );
      if ( numWorkers >= 0 ) {
        options.setNumWorkers( numWorkers );
      }
    }
    if ( StringUtils.isNotEmpty( config.getGcpMaximumNumberOfWokers() ) ) {
      int numWorkers = Const.toInt( variables.environmentSubstitute( config.getGcpMaximumNumberOfWokers() ), -1 );
      if ( numWorkers >= 0 ) {
        options.setMaxNumWorkers( numWorkers );
      }
    }
    if ( StringUtils.isNotEmpty( config.getGcpWorkerMachineType() ) ) {
      String machineType = variables.environmentSubstitute( config.getGcpWorkerMachineType() );
      options.setWorkerMachineType( machineType );
    }
    if ( StringUtils.isNotEmpty( config.getGcpWorkerDiskType() ) ) {
      String diskType = variables.environmentSubstitute( config.getGcpWorkerDiskType() );
      options.setWorkerDiskType( diskType );
    }
    if ( StringUtils.isNotEmpty( config.getGcpDiskSizeGb() ) ) {
      int diskSize = Const.toInt( variables.environmentSubstitute( config.getGcpDiskSizeGb() ), -1 );
      if ( diskSize >= 0 ) {
        options.setDiskSizeGb( diskSize );
      }
    }
    if ( StringUtils.isNotEmpty( config.getGcpZone() ) ) {
      String zone = variables.environmentSubstitute( config.getGcpZone() );
      options.setZone( zone );
    }
    if ( StringUtils.isNotEmpty( config.getGcpRegion() ) ) {
      String region = variables.environmentSubstitute( config.getGcpRegion() );
      options.setRegion( region );
    }
    if ( StringUtils.isNotEmpty( config.getGcpAutoScalingAlgorithm() ) ) {
      String algorithmCode = variables.environmentSubstitute( config.getGcpAutoScalingAlgorithm() );
      try {

        DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType algorithm = DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.valueOf( algorithmCode );
        options.setAutoscalingAlgorithm( algorithm );
      } catch ( Exception e ) {
        logChannel.logError( "Unknown autoscaling algorithm for GCP Dataflow: " + algorithmCode, e );
      }
    }
    options.setStreaming( config.isGcpStreaming() );

  }

  private void configureSparkOptions( BeamJobConfig config, SparkPipelineOptions options, IVariables variables, String transformationName ) throws IOException {

    // options.setFilesToStage( BeamConst.findLibraryFilesToStage( null, config.getPluginsToStage(), true, true ) );

    if ( StringUtils.isNotEmpty( config.getSparkMaster() ) ) {
      options.setSparkMaster( variables.environmentSubstitute( config.getSparkMaster() ) );
    }
    if ( StringUtils.isNotEmpty( config.getSparkBatchIntervalMillis() ) ) {
      long interval = Const.toLong( variables.environmentSubstitute( config.getSparkBatchIntervalMillis() ), -1L );
      if ( interval >= 0 ) {
        options.setBatchIntervalMillis( interval );
      }
    }
    if ( StringUtils.isNotEmpty( config.getSparkCheckpointDir() ) ) {
      options.setCheckpointDir( variables.environmentSubstitute( config.getSparkCheckpointDir() ) );
    }
    if ( StringUtils.isNotEmpty( config.getSparkCheckpointDurationMillis() ) ) {
      long duration = Const.toLong( variables.environmentSubstitute( config.getSparkCheckpointDurationMillis() ), -1L );
      if ( duration >= 0 ) {
        options.setCheckpointDurationMillis( duration );
      }
    }
    if ( StringUtils.isNotEmpty( config.getSparkMaxRecordsPerBatch() ) ) {
      long records = Const.toLong( variables.environmentSubstitute( config.getSparkMaxRecordsPerBatch() ), -1L );
      if ( records >= 0 ) {
        options.setMaxRecordsPerBatch( records );
      }
    }
    if ( StringUtils.isNotEmpty( config.getSparkMinReadTimeMillis() ) ) {
      long readTime = Const.toLong( variables.environmentSubstitute( config.getSparkMinReadTimeMillis() ), -1L );
      if ( readTime >= 0 ) {
        options.setMinReadTimeMillis( readTime );
      }
    }
    if ( StringUtils.isNotEmpty( config.getSparkReadTimePercentage() ) ) {
      double percentage = Const.toDouble( variables.environmentSubstitute( config.getSparkReadTimePercentage() ), -1.0 );
      if ( percentage >= 0 ) {
        options.setReadTimePercentage( percentage / 100 );
      }
    }
    if ( StringUtils.isNotEmpty( config.getSparkBundleSize() ) ) {
      long bundleSize = Const.toLong( variables.environmentSubstitute( config.getSparkBundleSize() ), -1L );
      if ( bundleSize >= 0 ) {
        options.setBundleSize( bundleSize );
      }
    }
    if ( StringUtils.isNotEmpty( config.getSparkStorageLevel() ) ) {
      options.setStorageLevel( variables.environmentSubstitute( config.getSparkStorageLevel() ) );
    }
    String appName = transformationName.replace( " ", "_" );
    options.setAppName( appName );
  }

  private void configureFlinkOptions( BeamJobConfig config, FlinkPipelineOptions options, IVariables variables ) throws IOException {

    options.setFilesToStage( BeamConst.findLibraryFilesToStage( null, config.getPluginsToStage(), true, true ) );

    // Address of the Flink Master where the Pipeline should be executed. Can either be of the form \"host:port\" or one of the special values [local], [collection] or [auto].")
    if (StringUtils.isNotEmpty( config.getFlinkMaster() )) {
      options.setFlinkMaster(variables.environmentSubstitute( options.getFlinkMaster() ) );
    }

    // The degree of parallelism to be used when distributing operations onto workers. If the parallelism is not set, the configured Flink default is used, or 1 if none can be found.")
    if (StringUtils.isNotEmpty( config.getFlinkParallelism() )) {
      int value = Const.toInt( variables.environmentSubstitute(config.getFlinkParallelism()), -1 );
      if (value>0) {
        options.setParallelism(value);
      }
    }

    // The interval in milliseconds at which to trigger checkpoints of the running pipeline. Default: No checkpointing.")
    if (StringUtils.isNotEmpty( config.getFlinkCheckpointingInterval() )) {
      long value = Const.toLong( variables.environmentSubstitute( config.getFlinkCheckpointingInterval() ), -1L );
      if (value>0) {
        options.setCheckpointingInterval( value );
      }
    }

    // The checkpointing mode that defines consistency guarantee.")
    if (StringUtils.isNotEmpty( config.getFlinkCheckpointingMode() )) {
      String modeString = variables.environmentSubstitute( config.getFlinkCheckpointingMode() );
      try {
        CheckpointingMode mode = CheckpointingMode.valueOf( modeString);
        if ( mode != null ) {
          options.setCheckpointingMode( modeString );
        }
      } catch(Exception e) {
        throw new IOException( "Unable to parse flink check pointing mode '"+modeString+"'", e );
      }
    }

    // The maximum time in milliseconds that a checkpoint may take before being discarded.")
    if (StringUtils.isNotEmpty( config.getFlinkCheckpointTimeoutMillis() )) {
      long value = Const.toLong( variables.environmentSubstitute( config.getFlinkCheckpointTimeoutMillis() ), -1L );
      if (value>0) {
        options.setCheckpointTimeoutMillis( value );
      }
    }

    // The minimal pause in milliseconds before the next checkpoint is triggered.")
    if (StringUtils.isNotEmpty( config.getFlinkMinPauseBetweenCheckpoints() )) {
      long value = Const.toLong( variables.environmentSubstitute( config.getFlinkMinPauseBetweenCheckpoints() ), -1L );
      if (value>0) {
        options.setMinPauseBetweenCheckpoints( value );
      }
    }

    // Sets the number of times that failed tasks are re-executed. A value of zero effectively disables fault tolerance. A value of -1 indicates that the system default value (as defined in the configuration) should be used.")
    if (StringUtils.isNotEmpty( config.getFlinkNumberOfExecutionRetries() )) {
      int value = Const.toInt( variables.environmentSubstitute(config.getFlinkNumberOfExecutionRetries()), -1 );
      if (value>=0) {
        options.setNumberOfExecutionRetries( value );
      }
    }

    // Sets the delay in milliseconds between executions. A value of {@code -1} indicates that the default value should be used.")
    if (StringUtils.isNotEmpty( config.getFlinkExecutionRetryDelay() )) {
      long value = Const.toLong( variables.environmentSubstitute( config.getFlinkExecutionRetryDelay() ), -1L );
      if (value>0) {
        options.setExecutionRetryDelay( value );
      }
    }

    // Sets the behavior of reusing objects.")
    if (StringUtils.isNotEmpty( config.getFlinkObjectReuse() )) {
      String str = variables.environmentSubstitute( config.getFlinkObjectReuse() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setObjectReuse( value );
    }

    // Sets the state backend to use in streaming mode. Otherwise the default is read from the Flink config.")
    /** TODO
    if (StringUtils.isNotEmpty( config.getFlinkStateBackend() )) {
      String str = variables.environmentSubstitute( config.getFlinkStateBackend() );
      try {

        options.setStateBackend(StateBackEnd);
      } catch(Exception e) {
        throw new IOException( "Unable to parse flink state back-end '"+modeString+"'", e );
      }
    }
    */

    // Enable/disable Beam metrics in Flink Runner")
    if (StringUtils.isNotEmpty( config.getFlinkDisableMetrics() )) {
      String str = variables.environmentSubstitute( config.getFlinkDisableMetrics() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setDisableMetrics( !value );
    }

    // Enables or disables externalized checkpoints. Works in conjunction with CheckpointingInterval")
    if (StringUtils.isNotEmpty( config.getFlinkExternalizedCheckpointsEnabled() )) {
      String str = variables.environmentSubstitute( config.getFlinkExternalizedCheckpointsEnabled() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setExternalizedCheckpointsEnabled( value );
    }

    // Sets the behavior of externalized checkpoints on cancellation.")
    if (StringUtils.isNotEmpty( config.getFlinkRetainExternalizedCheckpointsOnCancellation() )) {
      String str = variables.environmentSubstitute( config.getFlinkRetainExternalizedCheckpointsOnCancellation() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setRetainExternalizedCheckpointsOnCancellation( value );
    }

    // The maximum number of elements in a bundle.")
    if (StringUtils.isNotEmpty( config.getFlinkMaxBundleSize() )) {
      long value = Const.toLong( variables.environmentSubstitute( config.getFlinkMaxBundleSize() ), -1L );
      if (value>0) {
        options.setMaxBundleSize( value );
      }
    }

    // The maximum time to wait before finalising a bundle (in milliseconds).")
    if (StringUtils.isNotEmpty( config.getFlinkMaxBundleTimeMills() )) {
      long value = Const.toLong( variables.environmentSubstitute( config.getFlinkMaxBundleTimeMills() ), -1L );
      if ( value > 0 ) {
        options.setMaxBundleSize( value );
      }
    }

    // If set, shutdown sources when their watermark reaches +Inf.")
    if (StringUtils.isNotEmpty( config.getFlinkShutdownSourcesOnFinalWatermark() )) {
      String str = variables.environmentSubstitute( config.getFlinkShutdownSourcesOnFinalWatermark() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setShutdownSourcesOnFinalWatermark( value );
    }

    // Interval in milliseconds for sending latency tracking marks from the sources to the sinks. Interval value <= 0 disables the feature.")
    if (StringUtils.isNotEmpty( config.getFlinkLatencyTrackingInterval() )) {
      long value = Const.toLong( variables.environmentSubstitute( config.getFlinkLatencyTrackingInterval() ), -1L );
      if (value>0) {
        options.setLatencyTrackingInterval( value );
      }
    }

    // The interval in milliseconds for automatic watermark emission.")
    if (StringUtils.isNotEmpty( config.getFlinkAutoWatermarkInterval() )) {
      long value = Const.toLong( variables.environmentSubstitute( config.getFlinkAutoWatermarkInterval() ), -1L );
      if (value>0) {
        options.setAutoWatermarkInterval( value );
      }
    }

    // Flink mode for data exchange of batch pipelines. Reference {@link org.apache.flink.api.common.ExecutionMode}.
    // Set this to BATCH_FORCED if pipelines get blocked, see https://issues.apache.org/jira/browse/FLINK-10672")
    if (StringUtils.isNotEmpty( config.getFlinkExecutionModeForBatch() )) {
      String modeString = variables.environmentSubstitute( config.getFlinkExecutionModeForBatch() );
      ExecutionMode mode = ExecutionMode.valueOf( modeString );
      try {
        options.setExecutionModeForBatch( modeString );
      } catch(Exception e) {
        throw new IOException( "Unable to parse flink execution mode for batch '"+modeString+"'", e );
      }
    }
  }


  /**
   * Gets updatedListeners
   *
   * @return value of updatedListeners
   */
  public List<BeamMetricsUpdatedListener> getUpdatedListeners() {
    return updatedListeners;
  }

  /**
   * @param updatedListeners The updatedListeners to set
   */
  public void setUpdatedListeners( List<BeamMetricsUpdatedListener> updatedListeners ) {
    this.updatedListeners = updatedListeners;
  }

  /**
   * Gets loggingMetrics
   *
   * @return value of loggingMetrics
   */
  public boolean isLoggingMetrics() {
    return loggingMetrics;
  }

  /**
   * @param loggingMetrics The loggingMetrics to set
   */
  public void setLoggingMetrics( boolean loggingMetrics ) {
    this.loggingMetrics = loggingMetrics;
  }

  /**
   * Gets logChannel
   *
   * @return value of logChannel
   */
  public ILogChannel getLogChannel() {
    return logChannel;
  }

  /**
   * @param logChannel The logChannel to set
   */
  public void setLogChannel( ILogChannel logChannel ) {
    this.logChannel = logChannel;
  }

  /**
   * Gets sparkContext
   *
   * @return value of sparkContext
   */
  public JavaSparkContext getSparkContext() {
    return sparkContext;
  }

  /**
   * @param sparkContext The sparkContext to set
   */
  public void setSparkContext( JavaSparkContext sparkContext ) {
    this.sparkContext = sparkContext;
  }
}
