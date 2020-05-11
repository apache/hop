/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.beam.engines.flink;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.hop.beam.engines.BeamPipelineRunConfiguration;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.metastore.RunnerType;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;

@GuiPlugin
public class BeamFlinkPipelineRunConfiguration extends BeamPipelineRunConfiguration implements IBeamPipelineEngineRunConfiguration, IVariables, Cloneable {

  @GuiWidgetElement(
    order = "20010-flink-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "The Flink master",
    toolTip = "Address of the Flink Master where the Pipeline should be executed. Can"
      + " either be of the form \"host:port\" or one of the special values [local], "
      + "[collection] or [auto]."
  )
  @MetaStoreAttribute
  private String flinkMaster;

  @GuiWidgetElement(
    order = "20020-flink-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Parallelism",
    toolTip = "The degree of parallelism to be used when distributing operations onto workers. "
      + "If the parallelism is not set, the configured Flink default is used, or 1 if none can be found."
  )
  @MetaStoreAttribute
  private String flinkParallelism;

  @GuiWidgetElement(
    order = "20030-flink-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Checkpointing interval",
    toolTip = "The interval in milliseconds at which to trigger checkpoints of the running pipeline. "
      + "Default: No checkpointing."
  )
  @MetaStoreAttribute
  private String flinkCheckpointingInterval;

  @GuiWidgetElement(
    order = "20030-flink-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Checkpointing interval",
    toolTip = "The checkpointing mode that defines consistency guarantee."
  )
  @MetaStoreAttribute
  private String flinkCheckpointingMode;

  @GuiWidgetElement(
    order = "20030-flink-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Checkpointing timeout (ms)",
    toolTip = "The maximum time in milliseconds that a checkpoint may take before being discarded."
  )
  @MetaStoreAttribute
  private String flinkCheckpointTimeoutMillis;

  @GuiWidgetElement(
    order = "20030-flink-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Minimum pause between checkpoints",
    toolTip = "The minimal pause in milliseconds before the next checkpoint is triggered."
  )
  @MetaStoreAttribute
  private String flinkMinPauseBetweenCheckpoints;

  @GuiWidgetElement(
    order = "20000-flink-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    label = "Fail on checkpointing errors?",
    toolTip = "Sets the expected behaviour for tasks in case that they encounter an error in their "
      + "checkpointing procedure. If this is set to true, the task will fail on checkpointing error. "
      + "If this is set to false, the task will only decline a the checkpoint and continue running. "
  )
  @MetaStoreAttribute
  private boolean flinkFailingOnCheckpointingErrors;

  @GuiWidgetElement(
    order = "20030-flink-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Number of execution retries",
    toolTip = "Sets the number of times that failed tasks are re-executed. "
      + "A value of zero effectively disables fault tolerance. A value of -1 indicates "
      + "that the system default value (as defined in the configuration) should be used."
  )
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

  public BeamFlinkPipelineRunConfiguration() {
    super();
    this.tempLocation = "file://"+System.getProperty( "java.io.tmpdir" );
  }

  public BeamFlinkPipelineRunConfiguration( String flinkMaster, String flinkParallelism ) {
    this();
    this.flinkMaster = flinkMaster;
    this.flinkParallelism = flinkParallelism;
  }

  public BeamFlinkPipelineRunConfiguration( BeamFlinkPipelineRunConfiguration config ) {
    super( config );
    this.flinkMaster = config.flinkMaster;
    this.flinkParallelism = config.flinkParallelism;
    this.flinkCheckpointingInterval = config.flinkCheckpointingInterval;
    this.flinkCheckpointingMode = config.flinkCheckpointingMode;
    this.flinkCheckpointTimeoutMillis = config.flinkCheckpointTimeoutMillis;
    this.flinkMinPauseBetweenCheckpoints = config.flinkMinPauseBetweenCheckpoints;
    this.flinkFailingOnCheckpointingErrors = config.flinkFailingOnCheckpointingErrors;
    this.flinkNumberOfExecutionRetries = config.flinkNumberOfExecutionRetries;
    this.flinkExecutionRetryDelay = config.flinkExecutionRetryDelay;
    this.flinkObjectReuse = config.flinkObjectReuse;
    this.flinkStateBackend = config.flinkStateBackend;
    this.flinkDisableMetrics = config.flinkDisableMetrics;
    this.flinkExternalizedCheckpointsEnabled = config.flinkExternalizedCheckpointsEnabled;
    this.flinkRetainExternalizedCheckpointsOnCancellation = config.flinkRetainExternalizedCheckpointsOnCancellation;
    this.flinkMaxBundleSize = config.flinkMaxBundleSize;
    this.flinkMaxBundleTimeMills = config.flinkMaxBundleTimeMills;
    this.flinkShutdownSourcesOnFinalWatermark = config.flinkShutdownSourcesOnFinalWatermark;
    this.flinkLatencyTrackingInterval = config.flinkLatencyTrackingInterval;
    this.flinkAutoWatermarkInterval = config.flinkAutoWatermarkInterval;
    this.flinkExecutionModeForBatch = config.flinkExecutionModeForBatch;
  }

  public BeamFlinkPipelineRunConfiguration clone() {
    return new BeamFlinkPipelineRunConfiguration( this );
  }

  @Override public RunnerType getRunnerType() {
    return RunnerType.Flink;
  }

  @Override public PipelineOptions getPipelineOptions() throws HopException {
    FlinkPipelineOptions options = PipelineOptionsFactory.as( FlinkPipelineOptions.class );

    options.setFilesToStage( BeamConst.findLibraryFilesToStage( null, getPluginsToStage(), true, true ) );

    // Address of the Flink Master where the Pipeline should be executed. Can either be of the form \"host:port\" or one of the special values [local], [collection] or [auto].")
    if ( StringUtils.isNotEmpty( getFlinkMaster() ) ) {
      options.setFlinkMaster( environmentSubstitute( options.getFlinkMaster() ) );
    }

    // The degree of parallelism to be used when distributing operations onto workers. If the parallelism is not set, the configured Flink default is used, or 1 if none can be found.")
    if ( StringUtils.isNotEmpty( getFlinkParallelism() ) ) {
      int value = Const.toInt( environmentSubstitute( getFlinkParallelism() ), -1 );
      if ( value > 0 ) {
        options.setParallelism( value );
      }
    }

    // The interval in milliseconds at which to trigger checkpoints of the running pipeline. Default: No checkpointing.")
    if ( StringUtils.isNotEmpty( getFlinkCheckpointingInterval() ) ) {
      long value = Const.toLong( environmentSubstitute( getFlinkCheckpointingInterval() ), -1L );
      if ( value > 0 ) {
        options.setCheckpointingInterval( value );
      }
    }

    // The checkpointing mode that defines consistency guarantee.")
    if ( StringUtils.isNotEmpty( getFlinkCheckpointingMode() ) ) {
      String modeString = environmentSubstitute( getFlinkCheckpointingMode() );
      try {
        CheckpointingMode mode = CheckpointingMode.valueOf( modeString );
        if ( mode != null ) {
          options.setCheckpointingMode( modeString );
        }
      } catch ( Exception e ) {
        throw new HopException( "Unable to parse flink check pointing mode '" + modeString + "'", e );
      }
    }

    // The maximum time in milliseconds that a checkpoint may take before being discarded.")
    if ( StringUtils.isNotEmpty( getFlinkCheckpointTimeoutMillis() ) ) {
      long value = Const.toLong( environmentSubstitute( getFlinkCheckpointTimeoutMillis() ), -1L );
      if ( value > 0 ) {
        options.setCheckpointTimeoutMillis( value );
      }
    }

    // The minimal pause in milliseconds before the next checkpoint is triggered.")
    if ( StringUtils.isNotEmpty( getFlinkMinPauseBetweenCheckpoints() ) ) {
      long value = Const.toLong( environmentSubstitute( getFlinkMinPauseBetweenCheckpoints() ), -1L );
      if ( value > 0 ) {
        options.setMinPauseBetweenCheckpoints( value );
      }
    }

    // Sets the number of times that failed tasks are re-executed. A value of zero effectively disables fault tolerance. A value of -1 indicates that the system default value (as defined in the
    // configuration) should be used.")
    if ( StringUtils.isNotEmpty( getFlinkNumberOfExecutionRetries() ) ) {
      int value = Const.toInt( environmentSubstitute( getFlinkNumberOfExecutionRetries() ), -1 );
      if ( value >= 0 ) {
        options.setNumberOfExecutionRetries( value );
      }
    }

    // Sets the delay in milliseconds between executions. A value of {@code -1} indicates that the default value should be used.")
    if ( StringUtils.isNotEmpty( getFlinkExecutionRetryDelay() ) ) {
      long value = Const.toLong( environmentSubstitute( getFlinkExecutionRetryDelay() ), -1L );
      if ( value > 0 ) {
        options.setExecutionRetryDelay( value );
      }
    }

    // Sets the behavior of reusing objects.")
    if ( StringUtils.isNotEmpty( getFlinkObjectReuse() ) ) {
      String str = environmentSubstitute( getFlinkObjectReuse() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setObjectReuse( value );
    }

    // Sets the state backend to use in streaming mode. Otherwise the default is read from the Flink ")
    /** TODO
     if (StringUtils.isNotEmpty( getFlinkStateBackend() )) {
     String str = environmentSubstitute( getFlinkStateBackend() );
     try {

     options.setStateBackend(StateBackEnd);
     } catch(Exception e) {
     throw new IOException( "Unable to parse flink state back-end '"+modeString+"'", e );
     }
     }
     */

    // Enable/disable Beam metrics in Flink Runner")
    if ( StringUtils.isNotEmpty( getFlinkDisableMetrics() ) ) {
      String str = environmentSubstitute( getFlinkDisableMetrics() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setDisableMetrics( !value );
    }

    // Enables or disables externalized checkpoints. Works in conjunction with CheckpointingInterval")
    if ( StringUtils.isNotEmpty( getFlinkExternalizedCheckpointsEnabled() ) ) {
      String str = environmentSubstitute( getFlinkExternalizedCheckpointsEnabled() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setExternalizedCheckpointsEnabled( value );
    }

    // Sets the behavior of externalized checkpoints on cancellation.")
    if ( StringUtils.isNotEmpty( getFlinkRetainExternalizedCheckpointsOnCancellation() ) ) {
      String str = environmentSubstitute( getFlinkRetainExternalizedCheckpointsOnCancellation() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setRetainExternalizedCheckpointsOnCancellation( value );
    }

    // The maximum number of elements in a bundle.")
    if ( StringUtils.isNotEmpty( getFlinkMaxBundleSize() ) ) {
      long value = Const.toLong( environmentSubstitute( getFlinkMaxBundleSize() ), -1L );
      if ( value > 0 ) {
        options.setMaxBundleSize( value );
      }
    }

    // The maximum time to wait before finalising a bundle (in milliseconds).")
    if ( StringUtils.isNotEmpty( getFlinkMaxBundleTimeMills() ) ) {
      long value = Const.toLong( environmentSubstitute( getFlinkMaxBundleTimeMills() ), -1L );
      if ( value > 0 ) {
        options.setMaxBundleSize( value );
      }
    }

    // If set, shutdown sources when their watermark reaches +Inf.")
    if ( StringUtils.isNotEmpty( getFlinkShutdownSourcesOnFinalWatermark() ) ) {
      String str = environmentSubstitute( getFlinkShutdownSourcesOnFinalWatermark() );
      boolean value = "Y".equalsIgnoreCase( str ) || "TRUE".equalsIgnoreCase( str );
      options.setShutdownSourcesOnFinalWatermark( value );
    }

    // Interval in milliseconds for sending latency tracking marks from the sources to the sinks. Interval value <= 0 disables the feature.")
    if ( StringUtils.isNotEmpty( getFlinkLatencyTrackingInterval() ) ) {
      long value = Const.toLong( environmentSubstitute( getFlinkLatencyTrackingInterval() ), -1L );
      if ( value > 0 ) {
        options.setLatencyTrackingInterval( value );
      }
    }

    // The interval in milliseconds for automatic watermark emission.")
    if ( StringUtils.isNotEmpty( getFlinkAutoWatermarkInterval() ) ) {
      long value = Const.toLong( environmentSubstitute( getFlinkAutoWatermarkInterval() ), -1L );
      if ( value > 0 ) {
        options.setAutoWatermarkInterval( value );
      }
    }

    // Flink mode for data exchange of batch pipelines. Reference {@link org.apache.flink.api.common.ExecutionMode}.
    // Set this to BATCH_FORCED if pipelines get blocked, see https://issues.apache.org/jira/browse/FLINK-10672")
    if ( StringUtils.isNotEmpty( getFlinkExecutionModeForBatch() ) ) {
      String modeString = environmentSubstitute( getFlinkExecutionModeForBatch() );
      ExecutionMode mode = ExecutionMode.valueOf( modeString );
      try {
        options.setExecutionModeForBatch( modeString );
      } catch ( Exception e ) {
        throw new HopException( "Unable to parse flink execution mode for batch '" + modeString + "'", e );
      }
    }


    return options;
  }

  @Override public boolean isRunningAsynchronous() {
    return true;
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
   * Gets flinkFailingOnCheckpointingErrors
   *
   * @return value of flinkFailingOnCheckpointingErrors
   */
  public boolean isFlinkFailingOnCheckpointingErrors() {
    return flinkFailingOnCheckpointingErrors;
  }

  /**
   * @param flinkFailingOnCheckpointingErrors The flinkFailingOnCheckpointingErrors to set
   */
  public void setFlinkFailingOnCheckpointingErrors( boolean flinkFailingOnCheckpointingErrors ) {
    this.flinkFailingOnCheckpointingErrors = flinkFailingOnCheckpointingErrors;
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
   * Gets flinkDisableMetrics
   *
   * @return value of flinkDisableMetrics
   */
  public String getFlinkDisableMetrics() {
    return flinkDisableMetrics;
  }

  /**
   * @param flinkDisableMetrics The flinkDisableMetrics to set
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
