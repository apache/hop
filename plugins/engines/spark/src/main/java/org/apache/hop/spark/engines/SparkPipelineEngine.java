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

package org.apache.hop.spark.engines;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.INamedParameterDefinitions;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.parameters.NamedParameters;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.plugins.EngineCompatibility;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionBuilder;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionDataSetMeta;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionStateBuilder;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.IExecutionStartedListener;
import org.apache.hop.pipeline.IExecutionStoppedListener;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IEngineMetric;
import org.apache.hop.pipeline.engine.IPipelineComponentRowsReceived;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineCapabilities;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.core.SparkExecutionDataAccumulator;
import org.apache.hop.spark.core.SparkTransformMetricSlice;
import org.apache.hop.spark.core.SparkTransformMetricsAccumulator;
import org.apache.hop.spark.execution.SparkTransformExecutionSampling;
import org.apache.hop.spark.pipeline.HopPipelineMetaToSparkConverter;
import org.apache.hop.spark.pkg.SparkProjectPackage;
import org.apache.hop.spark.table.LakeSessionPlan;
import org.apache.hop.spark.util.SparkConst;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Option;

/**
 * Native Apache Spark 4.x pipeline engine. Converts Hop pipelines to Spark Dataset graphs and
 * executes Hop transforms via mapPartitions mini-pipelines.
 *
 * @see <a href="https://github.com/apache/hop/issues/7486">issue #7486</a>
 */
@PipelineEnginePlugin(
    id = SparkConst.PLUGIN_ID,
    name = SparkConst.PLUGIN_NAME,
    description =
        "Executes batch Hop pipelines natively on Apache Spark 4.x (mapPartitions + Dataset API)")
@Getter
@Setter
public class SparkPipelineEngine extends Variables implements IPipelineEngine<PipelineMeta> {

  private final PipelineEngineCapabilities engineCapabilities =
      new SparkPipelineEngineCapabilities();

  private PipelineMeta pipelineMeta;
  private String pluginId = SparkConst.PLUGIN_ID;
  private PipelineRunConfiguration pipelineRunConfiguration;
  private boolean preparing;
  private boolean readyToStart;
  private boolean running;
  private boolean finished;
  private boolean stopped;
  private boolean paused;
  private boolean hasHaltedComponents;
  private boolean preview;
  private int errors;
  private IHopMetadataProvider metadataProvider;
  private ILogChannel logChannel = LogChannel.GENERAL;
  private String containerId;
  private EngineMetrics engineMetrics = new EngineMetrics();
  private Result previousResult;

  private ILoggingObject parent;
  private IPipelineEngine<PipelineMeta> parentPipeline;
  private IWorkflowEngine<WorkflowMeta> parentWorkflow;
  private LogLevel logLevel;

  private Date executionStartDate;
  private Date executionEndDate;

  private final List<IExecutionStartedListener<IPipelineEngine<PipelineMeta>>>
      executionStartedListeners = Collections.synchronizedList(new ArrayList<>());
  private final List<IExecutionFinishedListener<IPipelineEngine<PipelineMeta>>>
      executionFinishedListeners = Collections.synchronizedList(new ArrayList<>());
  private final List<IExecutionStoppedListener<IPipelineEngine<PipelineMeta>>>
      executionStoppedListeners = Collections.synchronizedList(new ArrayList<>());

  private final Map<String, IPipelineEngine> activeSubPipelines = new HashMap<>();
  private final Map<String, IWorkflowEngine<WorkflowMeta>> activeSubWorkflows = new HashMap<>();
  private final Map<String, Object> extensionDataMap = Collections.synchronizedMap(new HashMap<>());

  private final INamedParameters namedParams = new NamedParameters();

  private String statusDescription = Pipeline.STRING_WAITING;
  private ComponentExecutionStatus status = ComponentExecutionStatus.STATUS_EMPTY;

  private HopPipelineMetaToSparkConverter converter;
  private ISparkPipelineEngineRunConfiguration sparkEngineRunConfiguration;
  private SparkSession sparkSession;

  /**
   * True when this engine created the {@link SparkSession} (and may stop it). False when reusing an
   * active/default session or nesting under a parent Spark engine — never stop/cancel the shared
   * context in that case.
   */
  private boolean sessionOwnedByThisEngine;

  private Dataset<Row> resultDataset;
  private Thread sparkThread;
  private SparkTransformMetricsAccumulator metricsAccumulator;
  private SparkExecutionDataAccumulator sampleDataAccumulator;
  private Timer metricsRefreshTimer;

  /** Execution information location from the run configuration (optional). */
  private ExecutionInfoLocation executionInfoLocation;

  private Timer executionInfoTimer;
  private volatile boolean executionInfoClosed;

  /** Stable log channel IDs per transform name + copy for execution info / GUI. */
  private final Map<String, String> componentLogChannelIds = new ConcurrentHashMap<>();

  private final List<IExecutionDataSampler<? extends IExecutionDataSamplerStore>> dataSamplers =
      Collections.synchronizedList(new ArrayList<>());

  public SparkPipelineEngine() {
    super();
  }

  public SparkPipelineEngine(
      PipelineMeta pipelineMeta, ILoggingObject parent, IVariables variables) {
    this();
    this.pipelineMeta = pipelineMeta;
    setParent(parent);
    initializeFrom(variables);
    copyParametersFromDefinitions(pipelineMeta);
    activateParameters(this);
  }

  @Override
  public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    return new SparkPipelineRunConfiguration();
  }

  public void validatePipelineRunConfigurationClass(
      IPipelineEngineRunConfiguration engineRunConfiguration) throws HopException {
    if (!(engineRunConfiguration instanceof SparkPipelineRunConfiguration)) {
      throw new HopException(
          "A native Spark pipeline engine needs a Spark run configuration, not of class "
              + engineRunConfiguration.getClass().getName());
    }
  }

  @Override
  public void prepareExecution() throws HopException {
    try {
      executionStartDate = new Date();
      status = ComponentExecutionStatus.STATUS_INIT;
      statusDescription = Pipeline.STRING_INITIALIZING;
      setPreparing(true);

      IPipelineEngineRunConfiguration engineRunConfiguration =
          pipelineRunConfiguration.getEngineRunConfiguration();
      validatePipelineRunConfigurationClass(engineRunConfiguration);
      sparkEngineRunConfiguration = (ISparkPipelineEngineRunConfiguration) engineRunConfiguration;

      if (metadataProvider == null) {
        throw new HopException("The Spark pipeline engine did not receive a metadata provider");
      }

      logChannel = new LogChannel(this, parent);
      if (logLevel != null) {
        logChannel.setLogLevel(logLevel);
      }

      // Execution info location (optional): load + initialize for later register/update/close
      lookupExecutionInformationLocation();

      converter =
          new HopPipelineMetaToSparkConverter(
              this,
              pipelineMeta,
              metadataProvider,
              pipelineRunConfiguration.getName(),
              sparkEngineRunConfiguration);
      converter.validatePipeline();

      logChannel.logBasic(
          "Prepared native Spark pipeline engine with run configuration '"
              + pipelineRunConfiguration.getName()
              + "' (master="
              + resolve(sparkEngineRunConfiguration.getSparkMaster())
              + ")");

      setRunning(false);
      setReadyToStart(true);
    } catch (Exception e) {
      setRunning(false);
      setReadyToStart(false);
      setStopped(true);
      setErrors(getErrors() + 1);
      setPaused(false);
      setPreparing(false);
      throw new HopException("Error preparing Spark pipeline", e);
    } finally {
      setPreparing(false);
    }
  }

  @Override
  public void startThreads() throws HopException {
    ClassLoader pluginCl = getClass().getClassLoader();
    ClassLoader previousCl = Thread.currentThread().getContextClassLoader();
    try {
      // Spark 4 resolves classic.SparkSession via the context classloader / plugin CL
      Thread.currentThread().setContextClassLoader(pluginCl);

      setRunning(true);
      setReadyToStart(false);
      status = ComponentExecutionStatus.STATUS_RUNNING;
      statusDescription = Pipeline.STRING_RUNNING;

      sparkSession = createSparkSession();

      // Register metrics + sample-data accumulators for mapPartitions transforms
      metricsAccumulator = new SparkTransformMetricsAccumulator();
      sparkSession.sparkContext().register(metricsAccumulator, "hop-transform-metrics");
      converter.setMetricsAccumulator(metricsAccumulator);
      sampleDataAccumulator = new SparkExecutionDataAccumulator();
      sparkSession.sparkContext().register(sampleDataAccumulator, "hop-execution-sample-data");
      converter.setSampleDataAccumulator(sampleDataAccumulator);
      try {
        converter.setExecutionSamplingContext(
            getLogChannelId(), SparkTransformExecutionSampling.serializeDataSamplers(dataSamplers));
      } catch (HopException e) {
        logChannel.logError("Error serializing execution data samplers (non-fatal)", e);
        converter.setExecutionSamplingContext(getLogChannelId(), "[]");
      }
      seedEngineMetricsComponents();

      // Register at execution info location (if configured) and keep state updated
      try {
        registerPipelineExecutionInformation();
        startExecutionInfoTimer();
      } catch (HopException e) {
        logChannel.logError("Error starting execution information tracking (non-fatal)", e);
      }

      fireExecutionStartedListeners();
      ExtensionPointHandler.callExtensionPoint(
          logChannel, this, HopExtensionPoint.PipelineStart.id, this);

      // Build + materialize Dataset graph on a worker thread so waitUntilFinished can poll
      sparkThread =
          new Thread(
              () -> {
                ClassLoader prev = Thread.currentThread().getContextClassLoader();
                try {
                  Thread.currentThread().setContextClassLoader(pluginCl);
                  resultDataset = converter.createDataset(sparkSession);
                  // Materialize: count is a safe action when no dedicated sink handler wrote data
                  long rows = resultDataset.count();
                  logChannel.logBasic("Spark pipeline finished, result row count=" + rows);
                  setErrors(0);
                } catch (Throwable e) {
                  logChannel.logError("Error executing Spark pipeline", e);
                  setErrors(getErrors() + 1);
                  setStopped(true);
                } finally {
                  Thread.currentThread().setContextClassLoader(prev);
                  setRunning(false);
                  setFinished(true);
                  executionEndDate = new Date();
                  if (getErrors() == 0) {
                    status = ComponentExecutionStatus.STATUS_FINISHED;
                    statusDescription = Pipeline.STRING_FINISHED;
                  } else {
                    status = ComponentExecutionStatus.STATUS_STOPPED;
                    statusDescription = Pipeline.STRING_STOPPED;
                  }
                  ExecutorUtil.cleanup(metricsRefreshTimer);
                  try {
                    populateEngineMetrics();
                  } catch (Exception emEx) {
                    logChannel.logError("Error populating final engine metrics", emEx);
                  }
                  try {
                    stopExecutionInfoTimer();
                  } catch (Exception eiEx) {
                    logChannel.logError("Error finalizing execution information (non-fatal)", eiEx);
                  }
                  try {
                    fireExecutionFinishedListeners();
                  } catch (HopException ex) {
                    logChannel.logError("Error firing finished listeners", ex);
                  }
                }
              },
              "hop-spark-pipeline");
      sparkThread.setContextClassLoader(pluginCl);
      sparkThread.start();

      // Live progress for the Hop Metrics grid (same cadence as Beam)
      metricsRefreshTimer = new Timer("hop-spark-metrics-refresh", true);
      metricsRefreshTimer.schedule(
          new TimerTask() {
            @Override
            public void run() {
              try {
                if (isFinished() || isStopped()) {
                  ExecutorUtil.cleanup(metricsRefreshTimer);
                  return;
                }
                populateEngineMetrics();
              } catch (Throwable e) {
                if (logChannel != null) {
                  logChannel.logError("Error refreshing Spark engine metrics", e);
                }
              }
            }
          },
          0L,
          1000L);
    } catch (Throwable e) {
      setRunning(false);
      setStopped(true);
      setErrors(getErrors() + 1);
      ExecutorUtil.cleanup(metricsRefreshTimer);
      throw new HopException("Unexpected error starting Spark pipeline", e);
    } finally {
      Thread.currentThread().setContextClassLoader(previousCl);
    }
  }

  /**
   * Seed the metrics grid with one placeholder component per transform so the UI lists them before
   * executor slices arrive.
   */
  private void seedEngineMetricsComponents() {
    EngineMetrics em = new EngineMetrics();
    em.setStartDate(executionStartDate);
    if (pipelineMeta != null) {
      for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
        EngineComponent component = new EngineComponent(transformMeta.getName(), 0);
        component.setRunning(true);
        component.setStatus(ComponentExecutionStatus.STATUS_RUNNING);
        component.setExecutionStartDate(executionStartDate);
        assignComponentIdentity(component);
        em.addComponent(component);
        em.setComponentRunning(component, true);
        em.setComponentStatus(component, ComponentExecutionStatus.STATUS_RUNNING.getDescription());
      }
    }
    synchronized (this) {
      engineMetrics = em;
    }
  }

  /** Stable log-channel id per transform copy for execution info + GUI. */
  private String componentLogChannelId(String transformName, int copyNr) {
    String key = transformName + '\0' + copyNr;
    return componentLogChannelIds.computeIfAbsent(
        key,
        k -> {
          String parent = getLogChannelId();
          if (StringUtils.isEmpty(parent)) {
            parent = "spark";
          }
          return parent + "|" + transformName + "|" + copyNr;
        });
  }

  private void assignComponentIdentity(EngineComponent component) {
    if (component == null) {
      return;
    }
    component.setLogChannelId(componentLogChannelId(component.getName(), component.getCopyNr()));
  }

  /**
   * Build {@link EngineMetrics} from Spark transform metric slices (mapPartitions) plus seeded
   * placeholders for transforms that only have native Dataset handlers.
   */
  protected synchronized void populateEngineMetrics() {
    EngineMetrics em = new EngineMetrics();
    em.setStartDate(getExecutionStartDate());
    em.setEndDate(getExecutionEndDate());

    Set<String> transformsWithSlices = new HashSet<>();
    Map<String, SparkTransformMetricSlice> slices =
        metricsAccumulator != null ? metricsAccumulator.value() : Collections.emptyMap();

    for (SparkTransformMetricSlice slice : slices.values()) {
      transformsWithSlices.add(slice.getTransformName());
      EngineComponent component = new EngineComponent(slice.getTransformName(), slice.getCopyNr());
      component.setLinesRead(slice.getLinesRead());
      component.setLinesWritten(slice.getLinesWritten());
      component.setLinesInput(slice.getLinesInput());
      component.setLinesOutput(slice.getLinesOutput());
      component.setErrors(slice.getErrors());
      applySliceTiming(component, slice);
      if (StringUtils.isNotEmpty(slice.getHost())) {
        component.setLogText("host=" + slice.getHost());
      }

      ComponentExecutionStatus componentStatus = resolveComponentStatus(slice);
      component.setStatus(componentStatus);
      component.setRunning(componentStatus == ComponentExecutionStatus.STATUS_RUNNING);
      component.setStopped(
          componentStatus == ComponentExecutionStatus.STATUS_STOPPED
              || componentStatus == ComponentExecutionStatus.STATUS_HALTING);
      assignComponentIdentity(component);

      em.addComponent(component);
      em.setComponentMetric(component, Pipeline.METRIC_READ, slice.getLinesRead());
      em.setComponentMetric(component, Pipeline.METRIC_WRITTEN, slice.getLinesWritten());
      em.setComponentMetric(component, Pipeline.METRIC_INPUT, slice.getLinesInput());
      em.setComponentMetric(component, Pipeline.METRIC_OUTPUT, slice.getLinesOutput());
      em.setComponentMetric(component, Pipeline.METRIC_ERROR, slice.getErrors());
      em.setComponentStatus(component, componentStatus.getDescription());
      em.setComponentRunning(component, component.isRunning());
      long speedRows =
          Math.max(
              Math.max(slice.getLinesRead(), slice.getLinesWritten()),
              Math.max(slice.getLinesInput(), slice.getLinesOutput()));
      if (speedRows > 0) {
        long durationMs = Math.max(1L, component.getExecutionDuration());
        double speed = (speedRows * 1000.0d) / durationMs;
        em.setComponentSpeed(component, String.format("%.1f", speed));
      }
    }

    // Transforms with no metric slices yet (not scheduled / no accumulator data)
    if (pipelineMeta != null) {
      ComponentExecutionStatus pipelineStatus = status;
      for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
        if (transformsWithSlices.contains(transformMeta.getName())) {
          continue;
        }
        EngineComponent component = new EngineComponent(transformMeta.getName(), 0);
        component.setExecutionStartDate(getExecutionStartDate());
        component.setExecutionEndDate(getExecutionEndDate());
        component.setExecutionDuration(
            calculateDuration(getExecutionStartDate(), getExecutionEndDate()));
        component.setStatus(pipelineStatus);
        component.setRunning(pipelineStatus == ComponentExecutionStatus.STATUS_RUNNING);
        component.setStopped(
            pipelineStatus == ComponentExecutionStatus.STATUS_STOPPED || isStopped());
        assignComponentIdentity(component);
        em.addComponent(component);
        em.setComponentStatus(component, pipelineStatus.getDescription());
        em.setComponentRunning(component, component.isRunning());
      }
    }

    engineMetrics = em;
  }

  // --- Execution information location (Beam/Local pattern, driver-side) ---

  /**
   * Load and initialize the execution information location named on the pipeline run configuration
   * (if any).
   */
  public void lookupExecutionInformationLocation() throws HopException {
    if (pipelineRunConfiguration == null || metadataProvider == null) {
      return;
    }
    String locationName = resolve(pipelineRunConfiguration.getExecutionInfoLocationName());
    if (StringUtils.isEmpty(locationName)) {
      return;
    }
    ExecutionInfoLocation location =
        metadataProvider.getSerializer(ExecutionInfoLocation.class).load(locationName);
    if (location == null) {
      logChannel.logError(
          "Execution information location '"
              + locationName
              + "' could not be found in the metadata");
      return;
    }
    executionInfoLocation = location;
    executionInfoClosed = false;
    IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();
    iLocation.initialize(this, metadataProvider);
    logChannel.logBasic("Using execution information location '" + locationName + "'");
  }

  /**
   * Register this pipeline execution at the configured location (metadata, variables, parameters).
   */
  public void registerPipelineExecutionInformation() throws HopException {
    if (executionInfoLocation == null) {
      return;
    }
    executionInfoLocation
        .getExecutionInfoLocation()
        .registerExecution(ExecutionBuilder.fromExecutor(this).build());
  }

  /** Periodically push pipeline + transform state to the execution information location. */
  public void startExecutionInfoTimer() {
    if (executionInfoLocation == null) {
      return;
    }
    long delay = Const.toLong(resolve(executionInfoLocation.getDataLoggingDelay()), 2000L);
    long interval = Const.toLong(resolve(executionInfoLocation.getDataLoggingInterval()), 5000L);
    final IExecutionInfoLocation iLocation = executionInfoLocation.getExecutionInfoLocation();

    executionInfoTimer = new Timer("hop-spark-execution-info", true);
    executionInfoTimer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            try {
              if (isFinished() || isStopped() || executionInfoClosed) {
                return;
              }
              // Refresh metrics so component state includes latest Spark counters
              populateEngineMetrics();
              updatePipelineState(iLocation);
            } catch (Exception e) {
              if (logChannel != null) {
                logChannel.logBasic(
                    "Warning: unable to register execution info at location "
                        + executionInfoLocation.getName()
                        + " (non-fatal): "
                        + e.getMessage());
              }
            }
          }
        },
        delay,
        interval);
  }

  protected void updatePipelineState(IExecutionInfoLocation iLocation) throws HopException {
    // Register sample rows collected on executors before updating parent/transform state
    registerSampleDataFromExecutors(iLocation);

    ExecutionState executionState =
        ExecutionStateBuilder.fromExecutor(SparkPipelineEngine.this, -1).build();
    iLocation.updateExecutionState(executionState);

    // Transform Execution + state nodes under the parent pipeline (Beam does the same from workers;
    // we do it on the driver so caching locations keep hierarchy in one CacheEntry).
    for (IEngineComponent component : getComponents()) {
      registerTransformExecution(iLocation, component);
      ExecutionState transformState =
          ExecutionStateBuilder.fromTransform(SparkPipelineEngine.this, component).build();
      iLocation.updateExecutionState(transformState);
    }
  }

  /**
   * Register a transform {@link Execution} child of this pipeline so the GUI can resolve parent →
   * transform hierarchy (see {@code PipelineExecutionViewer.loadSelectedTransformData}).
   */
  protected void registerTransformExecution(
      IExecutionInfoLocation iLocation, IEngineComponent component) throws HopException {
    if (iLocation == null || component == null) {
      return;
    }
    String id = component.getLogChannelId();
    if (StringUtils.isEmpty(id)) {
      id = componentLogChannelId(component.getName(), component.getCopyNr());
    }
    Execution execution =
        ExecutionBuilder.of()
            .withId(id)
            .withParentId(getLogChannelId())
            .withName(component.getName())
            .withCopyNr(Integer.toString(component.getCopyNr()))
            .withExecutorType(ExecutionType.Transform)
            .withExecutionStartDate(
                component.getExecutionStartDate() != null
                    ? component.getExecutionStartDate()
                    : getExecutionStartDate())
            .build();
    iLocation.registerExecution(execution);
  }

  /**
   * Pull JSON {@link ExecutionData} samples from the Spark accumulator and register them on the
   * driver-side execution info location (where the parent pipeline CacheEntry lives).
   */
  protected void registerSampleDataFromExecutors(IExecutionInfoLocation iLocation) {
    if (iLocation == null || sampleDataAccumulator == null || sampleDataAccumulator.isZero()) {
      return;
    }
    try {
      Map<String, String> samples = sampleDataAccumulator.value();
      if (samples == null || samples.isEmpty()) {
        return;
      }
      var mapper = HopJson.newMapper();
      int registered = 0;
      for (Map.Entry<String, String> entry : samples.entrySet()) {
        try {
          ExecutionData data = mapper.readValue(entry.getValue(), ExecutionData.class);
          if (data != null) {
            // Ensure parent id points at this pipeline execution
            if (StringUtils.isEmpty(data.getParentId())) {
              data.setParentId(getLogChannelId());
            }
            // Ensure transform Execution child exists (ownerId is the child log channel id)
            registerTransformExecutionFromSample(iLocation, data);
            iLocation.registerData(data);
            registered++;
          }
        } catch (Exception e) {
          if (logChannel != null) {
            logChannel.logError(
                "Error registering executor sample data for owner '"
                    + entry.getKey()
                    + "' (non-fatal)",
                e);
          }
        }
      }
      if (registered > 0 && logChannel != null) {
        logChannel.logBasic(
            "Registered " + registered + " transform sample set(s) from Spark executors");
      }
    } catch (Exception e) {
      if (logChannel != null) {
        logChannel.logError("Error reading sample data accumulator (non-fatal)", e);
      }
    }
  }

  /**
   * Beam {@code TransformBaseFn.registerExecutingTransform}: child Execution id = sample owner id,
   * parent = pipeline log channel id.
   */
  private void registerTransformExecutionFromSample(
      IExecutionInfoLocation iLocation, ExecutionData data) throws HopException {
    if (StringUtils.isEmpty(data.getOwnerId())) {
      return;
    }
    String transformName = null;
    String copyNr = "0";
    if (data.getSetMetaData() != null) {
      for (ExecutionDataSetMeta setMeta : data.getSetMetaData().values()) {
        if (setMeta != null && StringUtils.isNotEmpty(setMeta.getName())) {
          transformName = setMeta.getName();
          if (StringUtils.isNotEmpty(setMeta.getCopyNr())) {
            copyNr = setMeta.getCopyNr();
          }
          break;
        }
      }
    }
    if (StringUtils.isEmpty(transformName)) {
      // ownerId format: parent|transformName|copyNr
      String ownerId = data.getOwnerId();
      int first = ownerId.indexOf('|');
      int last = ownerId.lastIndexOf('|');
      if (first > 0 && last > first) {
        transformName = ownerId.substring(first + 1, last);
        copyNr = ownerId.substring(last + 1);
      } else {
        transformName = ownerId;
      }
    }
    Execution execution =
        ExecutionBuilder.of()
            .withId(data.getOwnerId())
            .withParentId(
                StringUtils.isNotEmpty(data.getParentId()) ? data.getParentId() : getLogChannelId())
            .withName(transformName)
            .withCopyNr(copyNr)
            .withExecutorType(ExecutionType.Transform)
            .withExecutionStartDate(getExecutionStartDate())
            .build();
    iLocation.registerExecution(execution);
  }

  /** Final pipeline/transform state update and close the location. Safe to call multiple times. */
  public void stopExecutionInfoTimer() {
    if (executionInfoLocation == null || executionInfoClosed) {
      ExecutorUtil.cleanup(executionInfoTimer);
      return;
    }
    try {
      ExecutorUtil.cleanup(executionInfoTimer);
      executionInfoTimer = null;

      try {
        populateEngineMetrics();
      } catch (Exception e) {
        // ignore — still try to write state
      }

      IExecutionInfoLocation iLocation = executionInfoLocation.getExecutionInfoLocation();
      // Final sample flush from executors (after jobs complete, accumulator is fully merged)
      registerSampleDataFromExecutors(iLocation);

      ExecutionState executionState =
          ExecutionStateBuilder.fromExecutor(SparkPipelineEngine.this, -1).build();
      iLocation.updateExecutionState(executionState);

      for (IEngineComponent component : getComponents()) {
        registerTransformExecution(iLocation, component);
        ExecutionState transformState =
            ExecutionStateBuilder.fromTransform(SparkPipelineEngine.this, component).build();
        iLocation.updateExecutionState(transformState);
      }

      iLocation.close();
    } catch (Throwable e) {
      if (logChannel != null) {
        logChannel.logError(
            "Error writing final execution state to location (non-fatal): "
                + (executionInfoLocation != null ? executionInfoLocation.getName() : "?"),
            e);
      }
    } finally {
      executionInfoClosed = true;
    }
  }

  /**
   * Map partition wall-clock times onto {@link EngineComponent} fields the GUI Metrics grid uses
   * for Duration ({@code firstRowReadDate} / {@code lastRowWrittenDate}).
   */
  private void applySliceTiming(EngineComponent component, SparkTransformMetricSlice slice) {
    long startMs = slice.getStartTimeMs();
    long endMs = slice.getEndTimeMs();
    if (startMs > 0) {
      Date start = new Date(startMs);
      component.setInitStartDate(start);
      component.setExecutionStartDate(start);
      component.setFirstRowReadDate(start);
    } else if (getExecutionStartDate() != null) {
      // Fallback for slices without timing
      component.setExecutionStartDate(getExecutionStartDate());
      component.setFirstRowReadDate(getExecutionStartDate());
    }

    if (slice.isFinished() && endMs > 0) {
      Date end = new Date(endMs);
      component.setExecutionEndDate(end);
      component.setLastRowWrittenDate(end);
    } else if (slice.isFinished() && getExecutionEndDate() != null) {
      component.setExecutionEndDate(getExecutionEndDate());
      component.setLastRowWrittenDate(getExecutionEndDate());
    }
    // While running, leave lastRowWrittenDate null so the grid ticks duration live

    long durationMs = slice.durationMs();
    if (durationMs <= 0 && startMs > 0) {
      durationMs =
          calculateDuration(component.getFirstRowReadDate(), component.getLastRowWrittenDate());
    }
    if (durationMs <= 0) {
      durationMs = calculateDuration(getExecutionStartDate(), getExecutionEndDate());
    }
    component.setExecutionDuration(durationMs);
  }

  private ComponentExecutionStatus resolveComponentStatus(SparkTransformMetricSlice slice) {
    if (isStopped() || getErrors() > 0) {
      if (slice.isFinished() || slice.getErrors() > 0) {
        return ComponentExecutionStatus.STATUS_STOPPED;
      }
    }
    if (slice.getErrors() > 0) {
      return ComponentExecutionStatus.STATUS_STOPPED;
    }
    if (slice.isFinished() || isFinished()) {
      return ComponentExecutionStatus.STATUS_FINISHED;
    }
    if (slice.isRunning() || isRunning()) {
      return ComponentExecutionStatus.STATUS_RUNNING;
    }
    return status != null ? status : ComponentExecutionStatus.STATUS_EMPTY;
  }

  protected long calculateDuration(Date startTime, Date stopTime) {
    if (startTime != null && stopTime == null) {
      return Calendar.getInstance().getTimeInMillis() - startTime.getTime();
    }
    if (startTime != null) {
      return stopTime.getTime() - startTime.getTime();
    }
    return 0L;
  }

  protected SparkSession createSparkSession() throws HopException {
    ClassLoader pluginCl = getClass().getClassLoader();
    ClassLoader previousCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(pluginCl);
      SparkSession session = buildSparkSession();
      // Ship project package zip to every executor (SparkFiles) when HOP_SPARK_PROJECT_PACKAGE set
      distributeProjectPackageIfConfigured(session);
      return session;
    } catch (IllegalStateException e) {
      // Surface a actionable message when classic SparkSession cannot be loaded
      throw new IllegalStateException(
          e.getMessage()
              + " Ensure the native Spark engine plugin ships spark-sql, hadoop-client-api/runtime,"
              + " and log4j under plugins/engines/spark/lib (Spark 4 loads"
              + " org.apache.spark.sql.classic.SparkSession reflectively).",
          e);
    } finally {
      Thread.currentThread().setContextClassLoader(previousCl);
    }
  }

  /**
   * When a Native Spark project package is configured, register it with {@code
   * SparkContext.addFile} so nested Simple Mapping / Pipeline Executor loads work on every
   * executor. Also re-materializes {@code PROJECT_HOME} on the driver.
   */
  private void distributeProjectPackageIfConfigured(SparkSession session) throws HopException {
    String packageUri = getVariable(SparkProjectPackage.VAR_PACKAGE_URI);
    if (StringUtils.isEmpty(packageUri)) {
      return;
    }
    SparkProjectPackage.distributeToCluster(session, this);
    SparkProjectPackage.ensureMaterializedOnWorker(this);
    if (logChannel != null) {
      logChannel.logBasic(
          "Distributed Spark project package for executors (SparkFiles): "
              + getVariable(SparkProjectPackage.VAR_PACKAGE_SPARK_FILE)
              + " (source "
              + getVariable(SparkProjectPackage.VAR_PACKAGE_URI)
              + "); PROJECT_HOME="
              + getVariable("PROJECT_HOME"));
    }
  }

  private SparkSession buildSparkSession() throws HopException {
    LakeSessionPlan lakePlan = LakeSessionPlan.from(pipelineMeta, metadataProvider, this);
    if (!lakePlan.isEmpty()) {
      lakePlan.verifyClasspath(getClass().getClassLoader());
      logChannel.logBasic(
          "Lakehouse session plan formats="
              + lakePlan.getFormatsNeeded()
              + " catalogs="
              + lakePlan.getCatalogsByMetaName().keySet());
    }

    // Nested under another Native Spark pipeline: always reuse the parent's session when present.
    if (parentPipeline instanceof SparkPipelineEngine parentSpark
        && parentSpark.sparkSession != null) {
      sessionOwnedByThisEngine = false;
      SparkSession session = parentSpark.sparkSession;
      logChannel.logBasic(
          "Nested Native Spark pipeline reusing parent SparkSession (parent='"
              + (parentSpark.pipelineMeta != null ? parentSpark.pipelineMeta.getName() : "?")
              + "', version="
              + session.version()
              + ")");
      if (!lakePlan.isEmpty()) {
        lakePlan.verifyActiveSession(session, logChannel, this);
      }
      return session;
    }

    // spark-submit / existing driver: reuse the active session if present
    Option<SparkSession> active = SparkSession.getActiveSession();
    if (active.isDefined()) {
      sessionOwnedByThisEngine = false;
      SparkSession session = active.get();
      logChannel.logBasic(
          "Reusing active SparkSession (version="
              + session.version()
              + ", spark-submit or existing driver context)");
      if (!lakePlan.isEmpty()) {
        lakePlan.verifyActiveSession(session, logChannel, this);
      }
      return session;
    }
    Option<SparkSession> def = SparkSession.getDefaultSession();
    if (def.isDefined()) {
      sessionOwnedByThisEngine = false;
      SparkSession session = def.get();
      logChannel.logBasic("Reusing default SparkSession (version=" + session.version() + ")");
      if (!lakePlan.isEmpty()) {
        lakePlan.verifyActiveSession(session, logChannel, this);
      }
      return session;
    }

    boolean underSubmit = isRunningUnderSparkSubmit();
    String master = resolve(sparkEngineRunConfiguration.getSparkMaster());
    if (StringUtils.isEmpty(master)) {
      // Prefer master from spark-submit / SparkConf when run config leaves it blank
      String confMaster = System.getProperty("spark.master");
      if (StringUtils.isEmpty(confMaster)) {
        try {
          SparkConf conf = new SparkConf(true);
          if (conf.contains("spark.master")) {
            confMaster = conf.get("spark.master");
          }
        } catch (Exception e) {
          // ignore — fall back to local
        }
      }
      master = StringUtils.isNotEmpty(confMaster) ? confMaster : "local[*]";
    }
    String appName = resolve(sparkEngineRunConfiguration.getSparkAppName());
    if (StringUtils.isEmpty(appName)) {
      appName = "Apache Hop - " + pipelineMeta.getName();
    }

    SparkSession.Builder builder = SparkSession.builder().appName(appName);

    // Under spark-submit, master is already in the conf; re-setting can still work but prefer
    // not fighting the launcher when blank was intended to inherit.
    if (!underSubmit
        || StringUtils.isNotEmpty(resolve(sparkEngineRunConfiguration.getSparkMaster()))) {
      builder = builder.master(master);
    } else if (StringUtils.isNotEmpty(master)) {
      builder = builder.master(master);
    }

    applySparkConfig(builder, "spark.driver.memory", sparkEngineRunConfiguration.getDriverMemory());
    applySparkConfig(
        builder, "spark.executor.memory", sparkEngineRunConfiguration.getExecutorMemory());
    applySparkConfig(
        builder, "spark.executor.cores", sparkEngineRunConfiguration.getExecutorCores());

    // Do not set spark.jars under spark-submit — the application jar is already the fat jar.
    // Setting it again can confuse class loading.
    if (!underSubmit) {
      String fatJar = resolve(sparkEngineRunConfiguration.getFatJar());
      if (StringUtils.isNotEmpty(fatJar)) {
        builder.config("spark.jars", fatJar);
      }
    }

    String extra = sparkEngineRunConfiguration.getSparkConfigs();
    if (StringUtils.isNotEmpty(extra)) {
      for (String line : extra.split("\\r?\\n")) {
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
          continue;
        }
        int eq = trimmed.indexOf('=');
        if (eq > 0) {
          builder.config(
              resolve(trimmed.substring(0, eq).trim()), resolve(trimmed.substring(eq + 1).trim()));
        }
      }
    }

    // Lakehouse: Delta/Iceberg extensions, hop_iceberg PATH catalog, SparkCatalog metadata.
    // Applied after run-config sparkConfigs so lake defaults fill gaps; explicit run-config
    // keys already set above win if the user overrode them (except catalog apply overwrites).
    if (!lakePlan.isEmpty()) {
      lakePlan.applyToBuilder(builder, this);
    }

    // Defaults for local/dev only when not already set by submit or run config extras
    if (!underSubmit && System.getProperty("spark.ui.enabled") == null) {
      builder.config("spark.ui.enabled", "false");
    }
    if (!underSubmit && System.getProperty("spark.sql.shuffle.partitions") == null) {
      builder.config("spark.sql.shuffle.partitions", "4");
    }

    SparkSession session = builder.getOrCreate();
    // getOrCreate may still return a shared session under spark-submit; only claim ownership
    // when we are not nested and no prior active session existed (we already returned above).
    sessionOwnedByThisEngine = true;
    logChannel.logBasic(
        "Created SparkSession version=" + session.version() + " master=" + master + " (owned)");
    return session;
  }

  /**
   * Heuristic: spark-submit sets deploy mode / app id properties and loads SPARK_ENV / conf before
   * main().
   */
  private static boolean isRunningUnderSparkSubmit() {
    if (System.getenv("SPARK_ENV_LOADED") != null) {
      return true;
    }
    if (System.getProperty("spark.submit.deployMode") != null) {
      return true;
    }
    if (System.getProperty("spark.app.id") != null) {
      return true;
    }
    // spark-submit always sets spark.master before invoking main
    String master = System.getProperty("spark.master");
    return StringUtils.isNotEmpty(master) && !master.startsWith("local");
  }

  private void applySparkConfig(SparkSession.Builder builder, String key, String value) {
    String resolved = resolve(value);
    if (StringUtils.isNotEmpty(resolved)) {
      builder.config(key, resolved);
    }
  }

  @Override
  public void execute() throws HopException {
    prepareExecution();
    startThreads();
  }

  @Override
  public void waitUntilFinished() {
    while ((running || paused || readyToStart) && !(stopped || finished)) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    if (sparkThread != null) {
      try {
        sparkThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void stopAll() {
    setStopped(true);
    setRunning(false);
    ExecutorUtil.cleanup(metricsRefreshTimer);
    // Nested / reused sessions share the parent's SparkContext — never cancelAllJobs there.
    if (sparkSession != null && sessionOwnedByThisEngine) {
      try {
        sparkSession.sparkContext().cancelAllJobs();
      } catch (Exception e) {
        if (logChannel != null) {
          logChannel.logError("Error cancelling Spark jobs", e);
        }
      }
    } else if (sparkSession != null && logChannel != null) {
      logChannel.logBasic(
          "Skipping Spark job cancel for nested/reused session (pipeline='"
              + (pipelineMeta != null ? pipelineMeta.getName() : "?")
              + "')");
    }
    try {
      populateEngineMetrics();
    } catch (Exception e) {
      if (logChannel != null) {
        logChannel.logError("Error populating metrics after stop", e);
      }
    }
    stopExecutionInfoTimer();
    try {
      fireExecutionStoppedListeners();
    } catch (HopException e) {
      if (logChannel != null) {
        logChannel.logError("Error firing stopped listeners", e);
      }
    }
  }

  @Override
  public void cleanup() {
    ExecutorUtil.cleanup(metricsRefreshTimer);
    ExecutorUtil.cleanup(executionInfoTimer);
    if (sparkSession != null) {
      try {
        // Only stop sessions this engine created. Nested Pipeline Executor children and
        // spark-submit drivers must leave the shared SparkSession alone.
        if (sessionOwnedByThisEngine) {
          String master =
              sparkEngineRunConfiguration != null
                  ? resolve(sparkEngineRunConfiguration.getSparkMaster())
                  : "";
          if (master != null && master.startsWith("local")) {
            logChannel.logBasic(
                "Stopping owned local SparkSession for pipeline '"
                    + (pipelineMeta != null ? pipelineMeta.getName() : "?")
                    + "'");
            sparkSession.stop();
          }
        } else if (logChannel != null) {
          logChannel.logDetailed(
              "Leaving shared SparkSession running after nested/reused pipeline '"
                  + (pipelineMeta != null ? pipelineMeta.getName() : "?")
                  + "'");
        }
      } catch (Exception e) {
        if (logChannel != null) {
          logChannel.logError("Error stopping SparkSession", e);
        }
      } finally {
        sparkSession = null;
      }
    }
  }

  /** Whether this engine owns (created) the SparkSession and may stop it. */
  boolean isSessionOwnedByThisEngine() {
    return sessionOwnedByThisEngine;
  }

  @Override
  public EngineCompatibility supports(IPlugin transformPlugin) {
    if (transformPlugin == null) {
      return EngineCompatibility.unknown();
    }
    String[] ids = transformPlugin.getIds();
    if (ids != null) {
      for (String id : ids) {
        String ban = HopPipelineMetaToSparkConverter.HARD_BANNED_PLUGIN_IDS.get(id);
        if (ban != null) {
          return EngineCompatibility.unsupported(ban);
        }
        if (HopPipelineMetaToSparkConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(id)) {
          return EngineCompatibility.supported();
        }
      }
    }
    // No opinion for ordinary transforms — generic mapPartitions may still work
    return EngineCompatibility.unknown();
  }

  @Override
  public EngineMetrics getEngineMetrics() {
    return engineMetrics;
  }

  @Override
  public EngineMetrics getEngineMetrics(String componentName, int copyNr) {
    EngineMetrics em = new EngineMetrics();
    em.setStartDate(engineMetrics.getStartDate());
    em.setEndDate(engineMetrics.getEndDate());
    for (IEngineComponent component : engineMetrics.getComponents()) {
      if (componentName != null && !component.getName().equalsIgnoreCase(componentName)) {
        continue;
      }
      if (copyNr >= 0 && component.getCopyNr() != copyNr) {
        continue;
      }
      em.addComponent(component);
      for (IEngineMetric metric : engineMetrics.getMetricsList()) {
        Long value = engineMetrics.getComponentMetric(component, metric);
        if (value != null) {
          em.setComponentMetric(component, metric, value);
        }
      }
      Boolean running = engineMetrics.getComponentRunningMap().get(component);
      if (running != null) {
        em.setComponentRunning(component, running);
      }
      String componentStatus = engineMetrics.getComponentStatusMap().get(component);
      if (componentStatus != null) {
        em.setComponentStatus(component, componentStatus);
      }
      String speed = engineMetrics.getComponentSpeedMap().get(component);
      if (speed != null) {
        em.setComponentSpeed(component, speed);
      }
    }
    return em;
  }

  @Override
  public Result getResult() {
    Result result = new Result();
    result.setNrErrors(errors);
    result.setResult(errors == 0);
    result.setStopped(isStopped());
    result.setLogChannelId(getLogChannelId());
    // Roll up lines from engine metrics when available
    long linesRead = 0;
    long linesWritten = 0;
    long linesInput = 0;
    long linesOutput = 0;
    for (IEngineComponent component : engineMetrics.getComponents()) {
      linesRead += nullToZero(engineMetrics.getComponentMetric(component, Pipeline.METRIC_READ));
      linesWritten +=
          nullToZero(engineMetrics.getComponentMetric(component, Pipeline.METRIC_WRITTEN));
      linesInput += nullToZero(engineMetrics.getComponentMetric(component, Pipeline.METRIC_INPUT));
      linesOutput +=
          nullToZero(engineMetrics.getComponentMetric(component, Pipeline.METRIC_OUTPUT));
    }
    result.setNrLinesRead(linesRead);
    result.setNrLinesWritten(linesWritten);
    result.setNrLinesInput(linesInput);
    result.setNrLinesOutput(linesOutput);
    return result;
  }

  private static long nullToZero(Long value) {
    return value == null ? 0L : value;
  }

  @Override
  public void pauseExecution() {
    // Not supported
  }

  @Override
  public void resumeExecution() {
    // Not supported
  }

  @Override
  public boolean hasHaltedComponents() {
    return hasHaltedComponents;
  }

  @Override
  public String getComponentLogText(String componentName, int copyNr) {
    return "";
  }

  @Override
  public List<IEngineComponent> getComponents() {
    return engineMetrics.getComponents();
  }

  @Override
  public List<IEngineComponent> getComponentCopies(String name) {
    List<IEngineComponent> copies = new ArrayList<>();
    for (IEngineComponent component : engineMetrics.getComponents()) {
      if (component.getName().equalsIgnoreCase(name)) {
        copies.add(component);
      }
    }
    return copies;
  }

  @Override
  public IEngineComponent findComponent(String name, int copyNr) {
    for (IEngineComponent component : engineMetrics.getComponents()) {
      if (component.getName().equalsIgnoreCase(name) && component.getCopyNr() == copyNr) {
        return component;
      }
    }
    return null;
  }

  @Override
  public void retrieveComponentOutput(
      IVariables variables,
      String componentName,
      int copyNr,
      int nrRows,
      IPipelineComponentRowsReceived rowsReceived)
      throws HopException {
    throw new HopException(
        "Retrieving component output is not supported by the native Spark pipeline engine");
  }

  @Override
  public boolean isSafeModeEnabled() {
    return false;
  }

  @Override
  public IRowSet findRowSet(
      String fromTransformName,
      int fromTransformCopy,
      String toTransformName,
      int toTransformCopy) {
    return null;
  }

  @Override
  public void pipelineCompleted() throws HopException {
    stopExecutionInfoTimer();
    cleanup();
  }

  // --- listeners ---

  @Override
  public void addExecutionStartedListener(
      IExecutionStartedListener<IPipelineEngine<PipelineMeta>> listener) {
    executionStartedListeners.add(listener);
  }

  @Override
  public void removeExecutionStartedListener(
      IExecutionStartedListener<IPipelineEngine<PipelineMeta>> listener) {
    executionStartedListeners.remove(listener);
  }

  @Override
  public void firePipelineExecutionStartedListeners() throws HopException {
    fireExecutionStartedListeners();
  }

  @Override
  public void fireExecutionStartedListeners() throws HopException {
    synchronized (executionStartedListeners) {
      for (IExecutionStartedListener<IPipelineEngine<PipelineMeta>> listener :
          executionStartedListeners) {
        listener.started(this);
      }
    }
  }

  @Override
  public void addExecutionFinishedListener(
      IExecutionFinishedListener<IPipelineEngine<PipelineMeta>> listener) {
    executionFinishedListeners.add(listener);
  }

  @Override
  public void removeExecutionFinishedListener(
      IExecutionFinishedListener<IPipelineEngine<PipelineMeta>> listener) {
    executionFinishedListeners.remove(listener);
  }

  @Override
  public void firePipelineExecutionFinishedListeners() throws HopException {
    fireExecutionFinishedListeners();
  }

  @Override
  public void fireExecutionFinishedListeners() throws HopException {
    synchronized (executionFinishedListeners) {
      for (IExecutionFinishedListener<IPipelineEngine<PipelineMeta>> listener :
          executionFinishedListeners) {
        listener.finished(this);
      }
    }
  }

  @Override
  public void addExecutionStoppedListener(
      IExecutionStoppedListener<IPipelineEngine<PipelineMeta>> listener) {
    executionStoppedListeners.add(listener);
  }

  @Override
  public void removeExecutionStoppedListener(
      IExecutionStoppedListener<IPipelineEngine<PipelineMeta>> listener) {
    executionStoppedListeners.remove(listener);
  }

  @Override
  public void firePipelineExecutionStoppedListeners() throws HopException {
    fireExecutionStoppedListeners();
  }

  @Override
  public void fireExecutionStoppedListeners() throws HopException {
    synchronized (executionStoppedListeners) {
      for (IExecutionStoppedListener<IPipelineEngine<PipelineMeta>> listener :
          executionStoppedListeners) {
        listener.stopped(this);
      }
    }
  }

  @Override
  public <Store extends IExecutionDataSamplerStore, Sampler extends IExecutionDataSampler<Store>>
      void addExecutionDataSampler(Sampler sampler) {
    dataSamplers.add(sampler);
  }

  // --- logging object ---

  @Override
  public String getObjectName() {
    return pipelineMeta != null ? pipelineMeta.getName() : SparkConst.PLUGIN_NAME;
  }

  @Override
  public String getFilename() {
    return pipelineMeta != null ? pipelineMeta.getFilename() : null;
  }

  @Override
  public LoggingObjectType getObjectType() {
    return LoggingObjectType.PIPELINE;
  }

  @Override
  public String getObjectCopy() {
    return null;
  }

  @Override
  public Date getRegistrationDate() {
    return null;
  }

  @Override
  public boolean isGatheringMetrics() {
    return logChannel != null && logChannel.isGatheringMetrics();
  }

  @Override
  public void setGatheringMetrics(boolean gatheringMetrics) {
    if (logChannel != null) {
      logChannel.setGatheringMetrics(gatheringMetrics);
    }
  }

  @Override
  public void setForcingSeparateLogging(boolean forcingSeparateLogging) {
    if (logChannel != null) {
      logChannel.setForcingSeparateLogging(forcingSeparateLogging);
    }
  }

  @Override
  public boolean isForcingSeparateLogging() {
    return logChannel != null && logChannel.isForcingSeparateLogging();
  }

  @Override
  public String getLogChannelId() {
    return logChannel != null ? logChannel.getLogChannelId() : null;
  }

  @Override
  public ILogChannel getLogChannel() {
    return logChannel;
  }

  @Override
  public void setLogChannel(ILogChannel log) {
    this.logChannel = log;
  }

  @Override
  public void setParent(ILoggingObject parent) {
    this.parent = parent;
  }

  @Override
  public ILoggingObject getParent() {
    return parent;
  }

  // --- parameters ---

  @Override
  public void addParameterDefinition(String key, String defValue, String description)
      throws DuplicateParamException {
    namedParams.addParameterDefinition(key, defValue, description);
  }

  @Override
  public String getParameterDescription(String key) throws UnknownParamException {
    return namedParams.getParameterDescription(key);
  }

  @Override
  public String getParameterDefault(String key) throws UnknownParamException {
    return namedParams.getParameterDefault(key);
  }

  @Override
  public String getParameterValue(String key) throws UnknownParamException {
    return namedParams.getParameterValue(key);
  }

  @Override
  public String[] listParameters() {
    return namedParams.listParameters();
  }

  @Override
  public void setParameterValue(String key, String value) throws UnknownParamException {
    namedParams.setParameterValue(key, value);
  }

  @Override
  public void removeAllParameters() {
    namedParams.removeAllParameters();
  }

  @Override
  public void clearParameterValues() {
    namedParams.clearParameterValues();
  }

  @Override
  public void copyParametersFromDefinitions(INamedParameterDefinitions definitions) {
    namedParams.copyParametersFromDefinitions(definitions);
  }

  @Override
  public void activateParameters(IVariables variables) {
    namedParams.activateParameters(variables);
  }

  // --- getters / setters ---

  @Override
  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  @Override
  public void setPipelineMeta(PipelineMeta pipelineMeta) {
    this.pipelineMeta = pipelineMeta;
  }

  @Override
  public String getPluginId() {
    return pluginId;
  }

  @Override
  public void setPluginId(String pluginId) {
    this.pluginId = pluginId;
  }

  @Override
  public void setPipelineRunConfiguration(PipelineRunConfiguration pipelineRunConfiguration) {
    this.pipelineRunConfiguration = pipelineRunConfiguration;
  }

  @Override
  public PipelineRunConfiguration getPipelineRunConfiguration() {
    return pipelineRunConfiguration;
  }

  @Override
  public PipelineEngineCapabilities getEngineCapabilities() {
    return engineCapabilities;
  }

  @Override
  public boolean isPreparing() {
    return preparing;
  }

  @Override
  public boolean isReadyToStart() {
    return readyToStart;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public boolean isPaused() {
    return paused;
  }

  @Override
  public int getErrors() {
    return errors;
  }

  @Override
  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }

  @Override
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  @Override
  public void setLogLevel(LogLevel logLevel) {
    this.logLevel = logLevel;
    if (logChannel != null) {
      logChannel.setLogLevel(logLevel);
    }
  }

  @Override
  public LogLevel getLogLevel() {
    return logLevel;
  }

  @Override
  public void setPreview(boolean preview) {
    this.preview = preview;
  }

  @Override
  public boolean isPreview() {
    return preview;
  }

  @Override
  public void setPreviousResult(Result previousResult) {
    this.previousResult = previousResult;
  }

  @Override
  public Result getPreviousResult() {
    return previousResult;
  }

  @Override
  public Date getExecutionStartDate() {
    return executionStartDate;
  }

  @Override
  public Date getExecutionEndDate() {
    return executionEndDate;
  }

  @Override
  public void addActiveSubPipeline(String transformName, IPipelineEngine executorPipeline) {
    activeSubPipelines.put(transformName, executorPipeline);
  }

  @Override
  public IPipelineEngine getActiveSubPipeline(String subPipelineName) {
    return activeSubPipelines.get(subPipelineName);
  }

  @Override
  public void addActiveSubWorkflow(
      String subWorkflowName, IWorkflowEngine<WorkflowMeta> subWorkflow) {
    activeSubWorkflows.put(subWorkflowName, subWorkflow);
  }

  @Override
  public IWorkflowEngine<WorkflowMeta> getActiveSubWorkflow(String subWorkflowName) {
    return activeSubWorkflows.get(subWorkflowName);
  }

  @Override
  public void setInternalHopVariables(IVariables var) {
    var.setVariable(
        Const.INTERNAL_VARIABLE_PIPELINE_NAME,
        Const.NVL(pipelineMeta != null ? pipelineMeta.getName() : "", ""));
    var.setVariable(
        Const.INTERNAL_VARIABLE_PIPELINE_ID,
        logChannel != null ? logChannel.getLogChannelId() : "");
  }

  @Override
  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }

  @Override
  public String getContainerId() {
    return containerId;
  }

  @Override
  public String getStatusDescription() {
    return statusDescription;
  }

  @Override
  public Map<String, Object> getExtensionDataMap() {
    return extensionDataMap;
  }

  @Override
  public IPipelineEngine getParentPipeline() {
    return parentPipeline;
  }

  @Override
  public void setParentPipeline(IPipelineEngine parentPipeline) {
    this.parentPipeline = parentPipeline;
  }

  @Override
  public IWorkflowEngine<WorkflowMeta> getParentWorkflow() {
    return parentWorkflow;
  }

  @Override
  public void setParentWorkflow(IWorkflowEngine<WorkflowMeta> parentWorkflow) {
    this.parentWorkflow = parentWorkflow;
  }

  @Override
  public boolean isFeedbackShown() {
    return false;
  }

  @Override
  public int getFeedbackSize() {
    return 0;
  }

  public ComponentExecutionStatus getStatus() {
    return status;
  }

  /** Exposed for tests. */
  public Dataset<Row> getResultDataset() {
    return resultDataset;
  }
}
