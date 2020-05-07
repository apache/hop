package org.apache.hop.beam.metastore;

import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;

import java.util.ArrayList;
import java.util.List;

@MetaStoreElementType(
  name = "Kettle Beam Job Config",
  description = "Describes a Kettle Beam Job configuration"
)
public class BeamJobConfig {

  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  private String runnerTypeName;

  //
  // Generic options
  //

  @MetaStoreAttribute
  private String userAgent;

  @MetaStoreAttribute
  private String tempLocation;

  @MetaStoreAttribute
  private String pluginsToStage;

  @MetaStoreAttribute
  private String stepPluginClasses;

  @MetaStoreAttribute
  private String xpPluginClasses;

  @MetaStoreAttribute
  private List<JobParameter> parameters;

  @MetaStoreAttribute
  private String streamingKettleStepsFlushInterval;

  @MetaStoreAttribute
  private String fatJar;


  //
  // Dataflow specific options
  //

  @MetaStoreAttribute
  private String gcpProjectId;

  @MetaStoreAttribute
  private String gcpAppName;

  @MetaStoreAttribute
  private String gcpStagingLocation;

  @MetaStoreAttribute
  private String gcpInitialNumberOfWorkers;

  @MetaStoreAttribute
  private String gcpMaximumNumberOfWokers;

  @MetaStoreAttribute
  private String gcpAutoScalingAlgorithm;

  @MetaStoreAttribute
  private String gcpWorkerMachineType;

  @MetaStoreAttribute
  private String gcpWorkerDiskType;

  @MetaStoreAttribute
  private String gcpDiskSizeGb;

  @MetaStoreAttribute
  private String gcpRegion;

  @MetaStoreAttribute
  private String gcpZone;

  @MetaStoreAttribute
  private boolean gcpStreaming;


  //
  // Spark specific options
  //
  @MetaStoreAttribute
  private boolean sparkLocal;

  @MetaStoreAttribute
  private String sparkMaster;

  @MetaStoreAttribute
  private String sparkDeployFolder;

  @MetaStoreAttribute
  private String sparkBatchIntervalMillis;

  @MetaStoreAttribute
  private String sparkCheckpointDir;

  @MetaStoreAttribute
  private String sparkCheckpointDurationMillis;

  @MetaStoreAttribute
  private boolean sparkEnableSparkMetricSinks;

  @MetaStoreAttribute
  private String sparkMaxRecordsPerBatch;

  @MetaStoreAttribute
  private String sparkMinReadTimeMillis;

  @MetaStoreAttribute
  private String sparkReadTimePercentage;

  @MetaStoreAttribute
  private String sparkBundleSize;

  @MetaStoreAttribute
  private String sparkStorageLevel;

  //
  // Flink options
  //
  @MetaStoreAttribute
  private boolean flinkLocal;

  @MetaStoreAttribute
  private String flinkMaster;

  @MetaStoreAttribute
  private String flinkParallelism;

  @MetaStoreAttribute
  private String flinkCheckpointingInterval;

  @MetaStoreAttribute
  private String flinkCheckpointingMode;

  @MetaStoreAttribute
  private String flinkCheckpointTimeoutMillis;

  @MetaStoreAttribute
  private String flinkMinPauseBetweenCheckpoints;

  @MetaStoreAttribute
  private String flinkNumberOfExecutionRetries;

  @MetaStoreAttribute
  private String flinkExecutionRetryDelay;

  @MetaStoreAttribute
  private String flinkObjectReuse;

  @MetaStoreAttribute
  private String flinkStateBackend;

  @MetaStoreAttribute
  private String flinkDisableMetrics;

  @MetaStoreAttribute
  private String flinkExternalizedCheckpointsEnabled;

  @MetaStoreAttribute
  private String flinkRetainExternalizedCheckpointsOnCancellation;

  @MetaStoreAttribute
  private String flinkMaxBundleSize;

  @MetaStoreAttribute
  private String flinkMaxBundleTimeMills;

  @MetaStoreAttribute
  private String flinkShutdownSourcesOnFinalWatermark;

  @MetaStoreAttribute
  private String flinkLatencyTrackingInterval;

  @MetaStoreAttribute
  private String flinkAutoWatermarkInterval;

  @MetaStoreAttribute
  private String flinkExecutionModeForBatch;


  public BeamJobConfig() {
    parameters = new ArrayList<>();
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets runnerTypeName
   *
   * @return value of runnerTypeName
   */
  public String getRunnerTypeName() {
    return runnerTypeName;
  }

  /**
   * @param runnerTypeName The runnerTypeName to set
   */
  public void setRunnerTypeName( String runnerTypeName ) {
    this.runnerTypeName = runnerTypeName;
  }

  /**
   * Gets userAgent
   *
   * @return value of userAgent
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * @param userAgent The userAgent to set
   */
  public void setUserAgent( String userAgent ) {
    this.userAgent = userAgent;
  }

  /**
   * Gets tempLocation
   *
   * @return value of tempLocation
   */
  public String getTempLocation() {
    return tempLocation;
  }

  /**
   * @param tempLocation The tempLocation to set
   */
  public void setTempLocation( String tempLocation ) {
    this.tempLocation = tempLocation;
  }

  /**
   * Gets gcpProjectId
   *
   * @return value of gcpProjectId
   */
  public String getGcpProjectId() {
    return gcpProjectId;
  }

  /**
   * @param gcpProjectId The gcpProjectId to set
   */
  public void setGcpProjectId( String gcpProjectId ) {
    this.gcpProjectId = gcpProjectId;
  }

  /**
   * Gets gcpAppName
   *
   * @return value of gcpAppName
   */
  public String getGcpAppName() {
    return gcpAppName;
  }

  /**
   * @param gcpAppName The gcpAppName to set
   */
  public void setGcpAppName( String gcpAppName ) {
    this.gcpAppName = gcpAppName;
  }

  /**
   * Gets gcpStagingLocation
   *
   * @return value of gcpStagingLocation
   */
  public String getGcpStagingLocation() {
    return gcpStagingLocation;
  }

  /**
   * @param gcpStagingLocation The gcpStagingLocation to set
   */
  public void setGcpStagingLocation( String gcpStagingLocation ) {
    this.gcpStagingLocation = gcpStagingLocation;
  }

  /**
   * Gets parameters
   *
   * @return value of parameters
   */
  public List<JobParameter> getParameters() {
    return parameters;
  }

  /**
   * @param parameters The parameters to set
   */
  public void setParameters( List<JobParameter> parameters ) {
    this.parameters = parameters;
  }

  /**
   * Gets pluginsToStage
   *
   * @return value of pluginsToStage
   */
  public String getPluginsToStage() {
    return pluginsToStage;
  }

  /**
   * @param pluginsToStage The pluginsToStage to set
   */
  public void setPluginsToStage( String pluginsToStage ) {
    this.pluginsToStage = pluginsToStage;
  }

  /**
   * Gets stepPluginClasses
   *
   * @return value of stepPluginClasses
   */
  public String getStepPluginClasses() {
    return stepPluginClasses;
  }

  /**
   * @param stepPluginClasses The stepPluginClasses to set
   */
  public void setStepPluginClasses( String stepPluginClasses ) {
    this.stepPluginClasses = stepPluginClasses;
  }

  /**
   * Gets xpPluginClasses
   *
   * @return value of xpPluginClasses
   */
  public String getXpPluginClasses() {
    return xpPluginClasses;
  }

  /**
   * @param xpPluginClasses The xpPluginClasses to set
   */
  public void setXpPluginClasses( String xpPluginClasses ) {
    this.xpPluginClasses = xpPluginClasses;
  }

  /**
   * Gets streamingKettleStepsFlushInterval
   *
   * @return value of streamingKettleStepsFlushInterval
   */
  public String getStreamingKettleStepsFlushInterval() {
    return streamingKettleStepsFlushInterval;
  }

  /**
   * @param streamingKettleStepsFlushInterval The streamingKettleStepsFlushInterval to set
   */
  public void setStreamingKettleStepsFlushInterval( String streamingKettleStepsFlushInterval ) {
    this.streamingKettleStepsFlushInterval = streamingKettleStepsFlushInterval;
  }

  /**
   * Gets fatJar
   *
   * @return value of fatJar
   */
  public String getFatJar() {
    return fatJar;
  }

  /**
   * @param fatJar The fatJar to set
   */
  public void setFatJar( String fatJar ) {
    this.fatJar = fatJar;
  }

  /**
   * Gets gcpInitialNumberOfWorkers
   *
   * @return value of gcpInitialNumberOfWorkers
   */
  public String getGcpInitialNumberOfWorkers() {
    return gcpInitialNumberOfWorkers;
  }

  /**
   * @param gcpInitialNumberOfWorkers The gcpInitialNumberOfWorkers to set
   */
  public void setGcpInitialNumberOfWorkers( String gcpInitialNumberOfWorkers ) {
    this.gcpInitialNumberOfWorkers = gcpInitialNumberOfWorkers;
  }

  /**
   * Gets gcpMaximumNumberOfWokers
   *
   * @return value of gcpMaximumNumberOfWokers
   */
  public String getGcpMaximumNumberOfWokers() {
    return gcpMaximumNumberOfWokers;
  }

  /**
   * @param gcpMaximumNumberOfWokers The gcpMaximumNumberOfWokers to set
   */
  public void setGcpMaximumNumberOfWokers( String gcpMaximumNumberOfWokers ) {
    this.gcpMaximumNumberOfWokers = gcpMaximumNumberOfWokers;
  }

  /**
   * Gets gcpStreaming
   *
   * @return value of gcpStreaming
   */
  public boolean isGcpStreaming() {
    return gcpStreaming;
  }

  /**
   * @param gcpStreaming The gcpStreaming to set
   */
  public void setGcpStreaming( boolean gcpStreaming ) {
    this.gcpStreaming = gcpStreaming;
  }

  /**
   * Gets gcpAutoScalingAlgorithm
   *
   * @return value of gcpAutoScalingAlgorithm
   */
  public String getGcpAutoScalingAlgorithm() {
    return gcpAutoScalingAlgorithm;
  }

  /**
   * @param gcpAutoScalingAlgorithm The gcpAutoScalingAlgorithm to set
   */
  public void setGcpAutoScalingAlgorithm( String gcpAutoScalingAlgorithm ) {
    this.gcpAutoScalingAlgorithm = gcpAutoScalingAlgorithm;
  }

  /**
   * Gets gcpWorkerMachineType
   *
   * @return value of gcpWorkerMachineType
   */
  public String getGcpWorkerMachineType() {
    return gcpWorkerMachineType;
  }

  /**
   * @param gcpWorkerMachineType The gcpWorkerMachineType to set
   */
  public void setGcpWorkerMachineType( String gcpWorkerMachineType ) {
    this.gcpWorkerMachineType = gcpWorkerMachineType;
  }

  /**
   * Gets gcpWorkerDiskType
   *
   * @return value of gcpWorkerDiskType
   */
  public String getGcpWorkerDiskType() {
    return gcpWorkerDiskType;
  }

  /**
   * @param gcpWorkerDiskType The gcpWorkerDiskType to set
   */
  public void setGcpWorkerDiskType( String gcpWorkerDiskType ) {
    this.gcpWorkerDiskType = gcpWorkerDiskType;
  }

  /**
   * Gets gcpDiskSizeGb
   *
   * @return value of gcpDiskSizeGb
   */
  public String getGcpDiskSizeGb() {
    return gcpDiskSizeGb;
  }

  /**
   * @param gcpDiskSizeGb The gcpDiskSizeGb to set
   */
  public void setGcpDiskSizeGb( String gcpDiskSizeGb ) {
    this.gcpDiskSizeGb = gcpDiskSizeGb;
  }

  /**
   * Gets gcpRegion
   *
   * @return value of gcpRegion
   */
  public String getGcpRegion() {
    return gcpRegion;
  }

  /**
   * @param gcpRegion The gcpRegion to set
   */
  public void setGcpRegion( String gcpRegion ) {
    this.gcpRegion = gcpRegion;
  }

  /**
   * Gets gcpZone
   *
   * @return value of gcpZone
   */
  public String getGcpZone() {
    return gcpZone;
  }

  /**
   * @param gcpZone The gcpZone to set
   */
  public void setGcpZone( String gcpZone ) {
    this.gcpZone = gcpZone;
  }

  /**
   * Gets sparkLocal
   *
   * @return value of sparkLocal
   */
  public boolean isSparkLocal() {
    return sparkLocal;
  }

  /**
   * @param sparkLocal The sparkLocal to set
   */
  public void setSparkLocal( boolean sparkLocal ) {
    this.sparkLocal = sparkLocal;
  }

  /**
   * Gets sparkBatchIntervalMillis
   *
   * @return value of sparkBatchIntervalMillis
   */
  public String getSparkBatchIntervalMillis() {
    return sparkBatchIntervalMillis;
  }

  /**
   * @param sparkBatchIntervalMillis The sparkBatchIntervalMillis to set
   */
  public void setSparkBatchIntervalMillis( String sparkBatchIntervalMillis ) {
    this.sparkBatchIntervalMillis = sparkBatchIntervalMillis;
  }

  /**
   * Gets sparkCheckpointDir
   *
   * @return value of sparkCheckpointDir
   */
  public String getSparkCheckpointDir() {
    return sparkCheckpointDir;
  }

  /**
   * @param sparkCheckpointDir The sparkCheckpointDir to set
   */
  public void setSparkCheckpointDir( String sparkCheckpointDir ) {
    this.sparkCheckpointDir = sparkCheckpointDir;
  }

  /**
   * Gets sparkCheckpointDurationMillis
   *
   * @return value of sparkCheckpointDurationMillis
   */
  public String getSparkCheckpointDurationMillis() {
    return sparkCheckpointDurationMillis;
  }

  /**
   * @param sparkCheckpointDurationMillis The sparkCheckpointDurationMillis to set
   */
  public void setSparkCheckpointDurationMillis( String sparkCheckpointDurationMillis ) {
    this.sparkCheckpointDurationMillis = sparkCheckpointDurationMillis;
  }

  /**
   * Gets sparkEnableSparkMetricSinks
   *
   * @return value of sparkEnableSparkMetricSinks
   */
  public boolean isSparkEnableSparkMetricSinks() {
    return sparkEnableSparkMetricSinks;
  }

  /**
   * @param sparkEnableSparkMetricSinks The sparkEnableSparkMetricSinks to set
   */
  public void setSparkEnableSparkMetricSinks( boolean sparkEnableSparkMetricSinks ) {
    this.sparkEnableSparkMetricSinks = sparkEnableSparkMetricSinks;
  }

  /**
   * Gets sparkMaxRecordsPerBatch
   *
   * @return value of sparkMaxRecordsPerBatch
   */
  public String getSparkMaxRecordsPerBatch() {
    return sparkMaxRecordsPerBatch;
  }

  /**
   * @param sparkMaxRecordsPerBatch The sparkMaxRecordsPerBatch to set
   */
  public void setSparkMaxRecordsPerBatch( String sparkMaxRecordsPerBatch ) {
    this.sparkMaxRecordsPerBatch = sparkMaxRecordsPerBatch;
  }

  /**
   * Gets sparkMinReadTimeMillis
   *
   * @return value of sparkMinReadTimeMillis
   */
  public String getSparkMinReadTimeMillis() {
    return sparkMinReadTimeMillis;
  }

  /**
   * @param sparkMinReadTimeMillis The sparkMinReadTimeMillis to set
   */
  public void setSparkMinReadTimeMillis( String sparkMinReadTimeMillis ) {
    this.sparkMinReadTimeMillis = sparkMinReadTimeMillis;
  }

  /**
   * Gets sparkReadTimePercentage
   *
   * @return value of sparkReadTimePercentage
   */
  public String getSparkReadTimePercentage() {
    return sparkReadTimePercentage;
  }

  /**
   * @param sparkReadTimePercentage The sparkReadTimePercentage to set
   */
  public void setSparkReadTimePercentage( String sparkReadTimePercentage ) {
    this.sparkReadTimePercentage = sparkReadTimePercentage;
  }

  /**
   * Gets sparkBundleSize
   *
   * @return value of sparkBundleSize
   */
  public String getSparkBundleSize() {
    return sparkBundleSize;
  }

  /**
   * @param sparkBundleSize The sparkBundleSize to set
   */
  public void setSparkBundleSize( String sparkBundleSize ) {
    this.sparkBundleSize = sparkBundleSize;
  }

  /**
   * Gets sparkMaster
   *
   * @return value of sparkMaster
   */
  public String getSparkMaster() {
    return sparkMaster;
  }

  /**
   * @param sparkMaster The sparkMaster to set
   */
  public void setSparkMaster( String sparkMaster ) {
    this.sparkMaster = sparkMaster;
  }

  /**
   * Gets sparkStorageLevel
   *
   * @return value of sparkStorageLevel
   */
  public String getSparkStorageLevel() {
    return sparkStorageLevel;
  }

  /**
   * @param sparkStorageLevel The sparkStorageLevel to set
   */
  public void setSparkStorageLevel( String sparkStorageLevel ) {
    this.sparkStorageLevel = sparkStorageLevel;
  }

  /**
   * Gets sparkDeployFolder
   *
   * @return value of sparkDeployFolder
   */
  public String getSparkDeployFolder() {
    return sparkDeployFolder;
  }

  /**
   * @param sparkDeployFolder The sparkDeployFolder to set
   */
  public void setSparkDeployFolder( String sparkDeployFolder ) {
    this.sparkDeployFolder = sparkDeployFolder;
  }

  /**
   * Gets flinkLocal
   *
   * @return value of flinkLocal
   */
  public boolean isFlinkLocal() {
    return flinkLocal;
  }

  /**
   * @param flinkLocal The flinkLocal to set
   */
  public void setFlinkLocal( boolean flinkLocal ) {
    this.flinkLocal = flinkLocal;
  }

  /**
   * Gets flinkMaster
   *
   * @return value of flinkMaster
   */
  public String getFlinkMaster() {
    return flinkMaster;
  }

  /**
   * @param flinkMaster The flinkMaster to set
   */
  public void setFlinkMaster( String flinkMaster ) {
    this.flinkMaster = flinkMaster;
  }

  /**
   * Gets flinkParallelism
   *
   * @return value of flinkParallelism
   */
  public String getFlinkParallelism() {
    return flinkParallelism;
  }

  /**
   * @param flinkParallelism The flinkParallelism to set
   */
  public void setFlinkParallelism( String flinkParallelism ) {
    this.flinkParallelism = flinkParallelism;
  }

  /**
   * Gets flinkCheckpointingInterval
   *
   * @return value of flinkCheckpointingInterval
   */
  public String getFlinkCheckpointingInterval() {
    return flinkCheckpointingInterval;
  }

  /**
   * @param flinkCheckpointingInterval The flinkCheckpointingInterval to set
   */
  public void setFlinkCheckpointingInterval( String flinkCheckpointingInterval ) {
    this.flinkCheckpointingInterval = flinkCheckpointingInterval;
  }

  /**
   * Gets flinkCheckpointingMode
   *
   * @return value of flinkCheckpointingMode
   */
  public String getFlinkCheckpointingMode() {
    return flinkCheckpointingMode;
  }

  /**
   * @param flinkCheckpointingMode The flinkCheckpointingMode to set
   */
  public void setFlinkCheckpointingMode( String flinkCheckpointingMode ) {
    this.flinkCheckpointingMode = flinkCheckpointingMode;
  }

  /**
   * Gets flinkCheckpointTimeoutMillis
   *
   * @return value of flinkCheckpointTimeoutMillis
   */
  public String getFlinkCheckpointTimeoutMillis() {
    return flinkCheckpointTimeoutMillis;
  }

  /**
   * @param flinkCheckpointTimeoutMillis The flinkCheckpointTimeoutMillis to set
   */
  public void setFlinkCheckpointTimeoutMillis( String flinkCheckpointTimeoutMillis ) {
    this.flinkCheckpointTimeoutMillis = flinkCheckpointTimeoutMillis;
  }

  /**
   * Gets flinkMinPauseBetweenCheckpoints
   *
   * @return value of flinkMinPauseBetweenCheckpoints
   */
  public String getFlinkMinPauseBetweenCheckpoints() {
    return flinkMinPauseBetweenCheckpoints;
  }

  /**
   * @param flinkMinPauseBetweenCheckpoints The flinkMinPauseBetweenCheckpoints to set
   */
  public void setFlinkMinPauseBetweenCheckpoints( String flinkMinPauseBetweenCheckpoints ) {
    this.flinkMinPauseBetweenCheckpoints = flinkMinPauseBetweenCheckpoints;
  }

  /**
   * Gets flinkNumberOfExecutionRetries
   *
   * @return value of flinkNumberOfExecutionRetries
   */
  public String getFlinkNumberOfExecutionRetries() {
    return flinkNumberOfExecutionRetries;
  }

  /**
   * @param flinkNumberOfExecutionRetries The flinkNumberOfExecutionRetries to set
   */
  public void setFlinkNumberOfExecutionRetries( String flinkNumberOfExecutionRetries ) {
    this.flinkNumberOfExecutionRetries = flinkNumberOfExecutionRetries;
  }

  /**
   * Gets flinkExecutionRetryDelay
   *
   * @return value of flinkExecutionRetryDelay
   */
  public String getFlinkExecutionRetryDelay() {
    return flinkExecutionRetryDelay;
  }

  /**
   * @param flinkExecutionRetryDelay The flinkExecutionRetryDelay to set
   */
  public void setFlinkExecutionRetryDelay( String flinkExecutionRetryDelay ) {
    this.flinkExecutionRetryDelay = flinkExecutionRetryDelay;
  }

  /**
   * Gets flinkObjectReuse
   *
   * @return value of flinkObjectReuse
   */
  public String getFlinkObjectReuse() {
    return flinkObjectReuse;
  }

  /**
   * @param flinkObjectReuse The flinkObjectReuse to set
   */
  public void setFlinkObjectReuse( String flinkObjectReuse ) {
    this.flinkObjectReuse = flinkObjectReuse;
  }

  /**
   * Gets flinkStateBackend
   *
   * @return value of flinkStateBackend
   */
  public String getFlinkStateBackend() {
    return flinkStateBackend;
  }

  /**
   * @param flinkStateBackend The flinkStateBackend to set
   */
  public void setFlinkStateBackend( String flinkStateBackend ) {
    this.flinkStateBackend = flinkStateBackend;
  }

  /**
   * Gets flinkEnableMetrics
   *
   * @return value of flinkEnableMetrics
   */
  public String getFlinkDisableMetrics() {
    return flinkDisableMetrics;
  }

  /**
   * @param flinkDisableMetrics The flinkEnableMetrics to set
   */
  public void setFlinkDisableMetrics( String flinkDisableMetrics ) {
    this.flinkDisableMetrics = flinkDisableMetrics;
  }

  /**
   * Gets flinkExternalizedCheckpointsEnabled
   *
   * @return value of flinkExternalizedCheckpointsEnabled
   */
  public String getFlinkExternalizedCheckpointsEnabled() {
    return flinkExternalizedCheckpointsEnabled;
  }

  /**
   * @param flinkExternalizedCheckpointsEnabled The flinkExternalizedCheckpointsEnabled to set
   */
  public void setFlinkExternalizedCheckpointsEnabled( String flinkExternalizedCheckpointsEnabled ) {
    this.flinkExternalizedCheckpointsEnabled = flinkExternalizedCheckpointsEnabled;
  }

  /**
   * Gets flinkRetainExternalizedCheckpointsOnCancellation
   *
   * @return value of flinkRetainExternalizedCheckpointsOnCancellation
   */
  public String getFlinkRetainExternalizedCheckpointsOnCancellation() {
    return flinkRetainExternalizedCheckpointsOnCancellation;
  }

  /**
   * @param flinkRetainExternalizedCheckpointsOnCancellation The flinkRetainExternalizedCheckpointsOnCancellation to set
   */
  public void setFlinkRetainExternalizedCheckpointsOnCancellation( String flinkRetainExternalizedCheckpointsOnCancellation ) {
    this.flinkRetainExternalizedCheckpointsOnCancellation = flinkRetainExternalizedCheckpointsOnCancellation;
  }

  /**
   * Gets flinkMaxBundleSize
   *
   * @return value of flinkMaxBundleSize
   */
  public String getFlinkMaxBundleSize() {
    return flinkMaxBundleSize;
  }

  /**
   * @param flinkMaxBundleSize The flinkMaxBundleSize to set
   */
  public void setFlinkMaxBundleSize( String flinkMaxBundleSize ) {
    this.flinkMaxBundleSize = flinkMaxBundleSize;
  }

  /**
   * Gets flinkMaxBundleTimeMills
   *
   * @return value of flinkMaxBundleTimeMills
   */
  public String getFlinkMaxBundleTimeMills() {
    return flinkMaxBundleTimeMills;
  }

  /**
   * @param flinkMaxBundleTimeMills The flinkMaxBundleTimeMills to set
   */
  public void setFlinkMaxBundleTimeMills( String flinkMaxBundleTimeMills ) {
    this.flinkMaxBundleTimeMills = flinkMaxBundleTimeMills;
  }

  /**
   * Gets flinkShutdownSourcesOnFinalWatermark
   *
   * @return value of flinkShutdownSourcesOnFinalWatermark
   */
  public String getFlinkShutdownSourcesOnFinalWatermark() {
    return flinkShutdownSourcesOnFinalWatermark;
  }

  /**
   * @param flinkShutdownSourcesOnFinalWatermark The flinkShutdownSourcesOnFinalWatermark to set
   */
  public void setFlinkShutdownSourcesOnFinalWatermark( String flinkShutdownSourcesOnFinalWatermark ) {
    this.flinkShutdownSourcesOnFinalWatermark = flinkShutdownSourcesOnFinalWatermark;
  }

  /**
   * Gets flinkLatencyTrackingInterval
   *
   * @return value of flinkLatencyTrackingInterval
   */
  public String getFlinkLatencyTrackingInterval() {
    return flinkLatencyTrackingInterval;
  }

  /**
   * @param flinkLatencyTrackingInterval The flinkLatencyTrackingInterval to set
   */
  public void setFlinkLatencyTrackingInterval( String flinkLatencyTrackingInterval ) {
    this.flinkLatencyTrackingInterval = flinkLatencyTrackingInterval;
  }

  /**
   * Gets flinkAutoWatermarkInterval
   *
   * @return value of flinkAutoWatermarkInterval
   */
  public String getFlinkAutoWatermarkInterval() {
    return flinkAutoWatermarkInterval;
  }

  /**
   * @param flinkAutoWatermarkInterval The flinkAutoWatermarkInterval to set
   */
  public void setFlinkAutoWatermarkInterval( String flinkAutoWatermarkInterval ) {
    this.flinkAutoWatermarkInterval = flinkAutoWatermarkInterval;
  }

  /**
   * Gets flinkExecutionModeForBatch
   *
   * @return value of flinkExecutionModeForBatch
   */
  public String getFlinkExecutionModeForBatch() {
    return flinkExecutionModeForBatch;
  }

  /**
   * @param flinkExecutionModeForBatch The flinkExecutionModeForBatch to set
   */
  public void setFlinkExecutionModeForBatch( String flinkExecutionModeForBatch ) {
    this.flinkExecutionModeForBatch = flinkExecutionModeForBatch;
  }
}
