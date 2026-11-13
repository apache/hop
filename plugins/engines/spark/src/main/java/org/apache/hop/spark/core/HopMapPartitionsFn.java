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

import java.io.File;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.vfs.HopVfsNamespace;
import org.apache.hop.core.vfs.HopVfsNamespaces;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileInputMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.spark.execution.SparkTransformExecutionSampling;
import org.apache.hop.spark.pkg.SparkProjectPackage;
import org.apache.hop.spark.util.SparkConst;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

/**
 * Executes a single Hop transform once per Spark partition via {@link
 * SingleThreadedPipelineExecutor}. Metadata is passed as serializable strings (XML/JSON) so
 * executors never receive live Hop graph objects.
 */
public class HopMapPartitionsFn implements MapPartitionsFunction<Row, Row>, Serializable {
  private static final String CONST_PROJECT_HOME = "PROJECT_HOME";
  private static final long serialVersionUID = 5L;

  /** Publish progress at least every this many input rows. */
  private static final int METRICS_ROW_INTERVAL = 1000;

  /** Publish progress at least every this many milliseconds. */
  private static final long METRICS_TIME_INTERVAL_MS = 1000L;

  private final List<SparkVariableValue> variableValues;
  private final String metastoreJson;
  private final String transformName;
  private final String transformPluginId;
  private final String transformMetaInterfaceXml;
  private final String inputRowMetaJson;
  private final String outputRowMetaJson;
  private final boolean inputTransform;
  private final List<String> targetTransforms;

  /** Names of info/side transforms (Stream Lookup "from", etc.). */
  private final List<String> infoTransforms;

  /** Parallel to {@link #infoTransforms}: row meta JSON per info stream. */
  private final List<String> infoRowMetaJsons;

  /** Parallel to {@link #infoTransforms}: broadcast Hop rows for each info stream. */
  private final List<Broadcast<List<Object[]>>> infoBroadcasts;

  private final SparkTransformMetricsAccumulator metricsAccumulator;
  private final SparkExecutionDataAccumulator sampleDataAccumulator;
  private final String runConfigName;
  private final String parentLogChannelId;
  private final String dataSamplersJson;

  /**
   * Transforms that run after this one in the same mini-pipeline (a fused chain), in order. Empty:
   * this transform runs on its own.
   */
  private List<FusedStage> fusedStages = new ArrayList<>();

  /** One transform of a fused chain after the first, with the row layouts it reads and writes. */
  public record FusedStage(
      String name,
      String pluginId,
      String transformMetaXml,
      String inputRowMetaJson,
      String outputRowMetaJson)
      implements Serializable {}

  /**
   * The Spark schema of the input rows when they come as Catalyst rows ({@link #callInternal}): the
   * types the values have, which can differ from the Spark form of the input row layout.
   */
  private String inputSchemaJson;

  public void setInputSchemaJson(String inputSchemaJson) {
    this.inputSchemaJson = inputSchemaJson;
  }

  /** Run {@code stages} after this transform, in the same mini-pipeline. */
  public void setFusedStages(List<FusedStage> stages) {
    this.fusedStages = new ArrayList<>(stages);
  }

  public HopMapPartitionsFn(
      List<SparkVariableValue> variableValues,
      String metastoreJson,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      String outputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms) {
    this(
        variableValues,
        metastoreJson,
        transformName,
        transformPluginId,
        transformMetaInterfaceXml,
        inputRowMetaJson,
        outputRowMetaJson,
        inputTransform,
        targetTransforms,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public HopMapPartitionsFn(
      List<SparkVariableValue> variableValues,
      String metastoreJson,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      String outputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms,
      SparkTransformMetricsAccumulator metricsAccumulator) {
    this(
        variableValues,
        metastoreJson,
        transformName,
        transformPluginId,
        transformMetaInterfaceXml,
        inputRowMetaJson,
        outputRowMetaJson,
        inputTransform,
        targetTransforms,
        null,
        null,
        null,
        metricsAccumulator,
        null,
        null,
        null,
        null);
  }

  public HopMapPartitionsFn(
      List<SparkVariableValue> variableValues,
      String metastoreJson,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      String outputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms,
      SparkTransformMetricsAccumulator metricsAccumulator,
      String runConfigName,
      String parentLogChannelId,
      String dataSamplersJson) {
    this(
        variableValues,
        metastoreJson,
        transformName,
        transformPluginId,
        transformMetaInterfaceXml,
        inputRowMetaJson,
        outputRowMetaJson,
        inputTransform,
        targetTransforms,
        null,
        null,
        null,
        metricsAccumulator,
        null,
        runConfigName,
        parentLogChannelId,
        dataSamplersJson);
  }

  public HopMapPartitionsFn(
      List<SparkVariableValue> variableValues,
      String metastoreJson,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      String outputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms,
      SparkTransformMetricsAccumulator metricsAccumulator,
      SparkExecutionDataAccumulator sampleDataAccumulator,
      String runConfigName,
      String parentLogChannelId,
      String dataSamplersJson) {
    this(
        variableValues,
        metastoreJson,
        transformName,
        transformPluginId,
        transformMetaInterfaceXml,
        inputRowMetaJson,
        outputRowMetaJson,
        inputTransform,
        targetTransforms,
        null,
        null,
        null,
        metricsAccumulator,
        sampleDataAccumulator,
        runConfigName,
        parentLogChannelId,
        dataSamplersJson);
  }

  public HopMapPartitionsFn(
      List<SparkVariableValue> variableValues,
      String metastoreJson,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      String outputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms,
      List<String> infoTransforms,
      List<String> infoRowMetaJsons,
      List<Broadcast<List<Object[]>>> infoBroadcasts,
      SparkTransformMetricsAccumulator metricsAccumulator,
      SparkExecutionDataAccumulator sampleDataAccumulator,
      String runConfigName,
      String parentLogChannelId,
      String dataSamplersJson) {
    this.variableValues = variableValues != null ? variableValues : List.of();
    this.metastoreJson = metastoreJson;
    this.transformName = transformName;
    this.transformPluginId = transformPluginId;
    this.transformMetaInterfaceXml = transformMetaInterfaceXml;
    this.inputRowMetaJson = inputRowMetaJson;
    this.outputRowMetaJson = outputRowMetaJson;
    this.inputTransform = inputTransform;
    this.targetTransforms = targetTransforms != null ? targetTransforms : List.of();
    this.infoTransforms = infoTransforms != null ? infoTransforms : List.of();
    this.infoRowMetaJsons = infoRowMetaJsons != null ? infoRowMetaJsons : List.of();
    this.infoBroadcasts = infoBroadcasts != null ? infoBroadcasts : List.of();
    this.metricsAccumulator = metricsAccumulator;
    this.sampleDataAccumulator = sampleDataAccumulator;
    this.runConfigName = runConfigName;
    this.parentLogChannelId = parentLogChannelId;
    this.dataSamplersJson = dataSamplersJson;
  }

  @Override
  public Iterator<Row> call(Iterator<Row> input) throws Exception {
    return run(input, RowPartitionCodec::new);
  }

  /**
   * As {@link #call}, on Catalyst rows: the partition of {@code Dataset.queryExecution().toRdd()}
   * in, rows for {@code internalCreateDataFrame} out. Skips Spark's conversion to and from its
   * external {@link Row}; the values are the same. Needs {@link #setInputSchemaJson}.
   */
  public Iterator<InternalRow> callInternal(Iterator<InternalRow> input) throws Exception {
    StructType inputSchema =
        StringUtils.isNotEmpty(inputSchemaJson)
            ? (StructType) DataType.fromJson(inputSchemaJson)
            : new StructType();
    return run(input, (in, out) -> new InternalPartitionCodec(in, inputSchema, out));
  }

  /** How the rows of a partition become Hop rows, and Hop rows Spark rows. */
  interface PartitionCodec<I, O> {
    Object[] toHop(I row) throws HopException;

    /** A row of file names for a transform that reads them from a hop. */
    Object[] filenameRowToHop(I row) throws HopException;

    O toSpark(Object[] hopRow) throws HopException;

    O toTaggedSpark(String target, Object[] hopRow) throws HopException;
  }

  /** Builds the codec of a partition from the input and output row layouts. */
  interface PartitionCodecFactory<I, O> {
    PartitionCodec<I, O> create(IRowMeta inputRowMeta, IRowMeta outputRowMeta) throws HopException;
  }

  /** External Spark rows, through {@link HopSparkRowConverter.RowCodec}. */
  static final class RowPartitionCodec implements PartitionCodec<Row, Row> {
    private final IRowMeta inputRowMeta;
    private final HopSparkRowConverter.RowCodec in;
    private final HopSparkRowConverter.RowCodec out;

    RowPartitionCodec(IRowMeta inputRowMeta, IRowMeta outputRowMeta) {
      this.inputRowMeta = inputRowMeta;
      this.in = HopSparkRowConverter.RowCodec.of(inputRowMeta);
      this.out = HopSparkRowConverter.RowCodec.of(outputRowMeta);
    }

    @Override
    public Object[] toHop(Row row) throws HopException {
      return in.toHop(row);
    }

    @Override
    public Object[] filenameRowToHop(Row row) throws HopException {
      return HopSparkRowConverter.toHopRow(inputRowMeta, row);
    }

    @Override
    public Row toSpark(Object[] hopRow) throws HopException {
      return out.toSpark(hopRow);
    }

    @Override
    public Row toTaggedSpark(String target, Object[] hopRow) throws HopException {
      return out.toTaggedSpark(target, hopRow);
    }
  }

  /** Catalyst rows, through {@link SparkInternalRows.Codec}. */
  static final class InternalPartitionCodec implements PartitionCodec<InternalRow, InternalRow> {
    private final SparkInternalRows.Codec in;
    private final SparkInternalRows.Codec out;

    InternalPartitionCodec(IRowMeta inputRowMeta, StructType inputSchema, IRowMeta outputRowMeta)
        throws HopException {
      this.in = SparkInternalRows.Codec.of(inputRowMeta, inputSchema);
      this.out = SparkInternalRows.Codec.of(outputRowMeta);
    }

    @Override
    public Object[] toHop(InternalRow row) throws HopException {
      return in.toHop(row);
    }

    @Override
    public Object[] filenameRowToHop(InternalRow row) throws HopException {
      return in.toHop(row);
    }

    @Override
    public InternalRow toSpark(Object[] hopRow) throws HopException {
      return out.toInternal(hopRow);
    }

    @Override
    public InternalRow toTaggedSpark(String target, Object[] hopRow) throws HopException {
      return out.toTaggedInternal(target, hopRow);
    }
  }

  private <I, O> Iterator<O> run(Iterator<I> input, PartitionCodecFactory<I, O> codecFactory)
      throws Exception {
    int copyNr = partitionId();
    String host = localHost();
    long partitionStartMs = System.currentTimeMillis();
    ITransform mainTransform = null;
    List<ITransform> chainForErrors = null;
    SparkTransformExecutionSampling sampling = null;
    // A prepared mini-pipeline holds a VFS namespace for its own metadata until it is cleaned up
    LocalPipelineEngine preparedPipeline = null;
    try {
      SparkHop.init();

      IHopMetadataProvider metadataProvider = new SerializableMetadataProvider(metastoreJson);
      IVariables variables = new Variables();
      for (SparkVariableValue variableValue : variableValues) {
        if (StringUtils.isNotEmpty(variableValue.getVariable())) {
          variables.setVariable(variableValue.getVariable(), variableValue.getValue());
        }
      }
      // Spark project package: extract once per JVM into java.io.tmpdir and set PROJECT_HOME
      // so Simple Mapping / Pipeline Executor path loads work on executors.
      SparkProjectPackage.ensureMaterializedOnWorker(variables);
      validateProjectHomeOnWorker(variables);
      // Partition-scoped internal variables for classic I/O filenames (e.g. Text File Output
      // with ${Internal.Transform.ID}). Readable stable ID = transform name + partition id.
      applyPartitionInternalVariables(variables, transformName, copyNr);
      // Nested Workflow/Pipeline Executor parent linkage for execution perspective drill-down.
      applySparkTransformOwnerId(variables, parentLogChannelId, transformName, copyNr);
      // A partitioned transform: the Spark partition is its partition
      SparkHopPartitioning.applyPartitionVariables(variables, copyNr);

      IRowMeta inputRowMeta =
          StringUtils.isNotEmpty(inputRowMetaJson)
              ? JsonRowMeta.fromJson(inputRowMetaJson)
              : new org.apache.hop.core.row.RowMeta();
      IRowMeta outputRowMeta = JsonRowMeta.fromJson(outputRowMetaJson);
      final PartitionCodec<I, O> codec = codecFactory.create(inputRowMeta, outputRowMeta);

      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName(transformName);
      pipelineMeta.setMetadataProvider(metadataProvider);

      if (infoTransforms.size() != infoRowMetaJsons.size()
          || infoTransforms.size() != infoBroadcasts.size()) {
        throw new HopException(
            "Info stream configuration mismatch for transform '"
                + transformName
                + "': names="
                + infoTransforms.size()
                + " metas="
                + infoRowMetaJsons.size()
                + " broadcasts="
                + infoBroadcasts.size());
      }

      List<IRowMeta> infoRowMetas = new ArrayList<>();
      for (String infoRowMetaJson : infoRowMetaJsons) {
        infoRowMetas.add(JsonRowMeta.fromJson(infoRowMetaJson));
      }

      TransformMeta mainInjectorTransformMeta = null;
      if (!inputTransform) {
        mainInjectorTransformMeta =
            createInjectorTransform(
                pipelineMeta, SparkConst.INJECTOR_TRANSFORM_NAME, inputRowMeta, 200, 200);
      }

      // Injectors for info/side streams (Stream Lookup, Validator, …) — Beam side-input pattern
      List<TransformMeta> infoTransformMetas = new ArrayList<>();
      for (int i = 0; i < infoTransforms.size(); i++) {
        infoTransformMetas.add(
            createInjectorTransform(
                pipelineMeta, infoTransforms.get(i), infoRowMetas.get(i), 200, 350 + 150 * i));
      }

      int targetLocationY = 200;
      List<TransformMeta> targetTransformMetas = new ArrayList<>();
      for (String targetTransform : targetTransforms) {
        DummyMeta dummyMeta = new DummyMeta();
        TransformMeta targetTransformMeta = new TransformMeta(targetTransform, dummyMeta);
        targetTransformMeta.setLocation(600, targetLocationY);
        targetLocationY += 150;
        targetTransformMetas.add(targetTransformMeta);
        pipelineMeta.addTransform(targetTransformMeta);
      }

      PluginRegistry registry = PluginRegistry.getInstance();
      ITransformMeta iTransformMeta =
          registry.loadClass(TransformPluginType.class, transformPluginId, ITransformMeta.class);
      if (iTransformMeta == null) {
        throw new HopException(
            "Unable to load transform plugin with ID "
                + transformPluginId
                + ", this plugin isn't in the plugin registry or classpath");
      }

      HopSparkUtil.loadTransformMetadataFromXml(
          transformName, iTransformMeta, transformMetaInterfaceXml, metadataProvider);

      // Resolve ${PROJECT_HOME}/… mapping/executor filenames on this JVM after package materialize
      // so Simple Mapping / Pipeline Executor open the child .hpl from the local extract.
      resolveNestedPipelineFilename(iTransformMeta, variables, transformName);

      // Text File Input / Excel / etc. accept filenames from a *main* hop (not an INFO stream)
      // and call findInputRowSet(accept_transform_name). The mini-pipeline only has the main
      // injector named _INJECTOR_, so re-bind accept_transform_name to that injector. Filename
      // rows must also be fully loaded before processRow drains the source (see below).
      boolean acceptFilenamesFromMain = bindAcceptingFilenamesToMainInjector(iTransformMeta);

      TransformMeta transformMeta = new TransformMeta(transformName, iTransformMeta);
      transformMeta.setTransformPluginId(transformPluginId);
      transformMeta.setLocation(400, 200);
      pipelineMeta.addTransform(transformMeta);
      // Give CurrentDirectoryResolver a folder under PROJECT_HOME when resolving relatives
      String projectHome = variables.getVariable(CONST_PROJECT_HOME);
      if (StringUtils.isNotEmpty(projectHome)) {
        pipelineMeta.setFilename(
            projectHome
                + (projectHome.endsWith("/") || projectHome.endsWith("\\") ? "" : File.separator)
                + "_spark_map_partitions.hpl");
      }
      if (!inputTransform) {
        pipelineMeta.addPipelineHop(new PipelineHopMeta(mainInjectorTransformMeta, transformMeta));
      }
      List<ITransformMeta> fusedTransformMetas = new ArrayList<>();
      TransformMeta previousStageMeta = transformMeta;
      for (FusedStage stage : fusedStages) {
        ITransformMeta stageMeta =
            registry.loadClass(TransformPluginType.class, stage.pluginId(), ITransformMeta.class);
        if (stageMeta == null) {
          throw new HopException(
              "Unable to load transform plugin with ID "
                  + stage.pluginId()
                  + ", this plugin isn't in the plugin registry or classpath");
        }
        HopSparkUtil.loadTransformMetadataFromXml(
            stage.name(), stageMeta, stage.transformMetaXml(), metadataProvider);
        resolveNestedPipelineFilename(stageMeta, variables, stage.name());
        TransformMeta stageTransformMeta = new TransformMeta(stage.name(), stageMeta);
        stageTransformMeta.setTransformPluginId(stage.pluginId());
        stageTransformMeta.setLocation(400 + 150 * (fusedTransformMetas.size() + 1), 200);
        pipelineMeta.addTransform(stageTransformMeta);
        pipelineMeta.addPipelineHop(new PipelineHopMeta(previousStageMeta, stageTransformMeta));
        fusedTransformMetas.add(stageMeta);
        previousStageMeta = stageTransformMeta;
      }
      for (TransformMeta infoTransformMeta : infoTransformMetas) {
        pipelineMeta.addPipelineHop(new PipelineHopMeta(infoTransformMeta, transformMeta));
      }
      for (TransformMeta targetTransformMeta : targetTransformMetas) {
        pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, targetTransformMeta));
      }

      // After injectors exist so Stream Lookup "from" can bind to the info injector by name
      iTransformMeta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
      for (ITransformMeta stageMeta : fusedTransformMetas) {
        stageMeta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
      }

      LocalPipelineEngine pipeline =
          new LocalPipelineEngine(pipelineMeta, variables, miniPipelineParent(parentLogChannelId));
      pipeline.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
      pipeline.setMetadataProvider(metadataProvider);
      // The driver did the once-per-run work (truncate, create table ...) before the job: this
      // partition only flags the transforms it prepared (Internal.Pipeline.PreparedOnceTransforms)
      pipeline.setPreparingTransformsOnce(false);
      pipeline
          .getPipelineRunConfiguration()
          .setName("spark-transform-local (" + transformName + ")");
      try {
        pipeline.prepareExecution();
      } catch (Exception e) {
        throw new HopException(
            "Failed to prepare transform '"
                + transformName
                + "' (plugin id="
                + transformPluginId
                + ") on Spark partition "
                + copyNr
                + " host="
                + host
                + " PROJECT_HOME="
                + variables.getVariable(CONST_PROJECT_HOME)
                + " HOP_SPARK_PROJECT_PACKAGE="
                + variables.getVariable(SparkProjectPackage.VAR_PACKAGE_URI),
            e);
      }
      preparedPipeline = pipeline;

      logAsPartition(pipeline, copyNr);

      RowProducer rowProducer = null;
      TransformMetaDataCombi mainInjectorCombi = null;
      if (!inputTransform) {
        rowProducer = pipeline.addRowProducer(SparkConst.INJECTOR_TRANSFORM_NAME, 0);
        mainInjectorCombi = findCombi(pipeline, SparkConst.INJECTOR_TRANSFORM_NAME);
      }
      List<RowProducer> infoRowProducers = new ArrayList<>();
      for (String infoTransform : infoTransforms) {
        infoRowProducers.add(pipeline.addRowProducer(infoTransform, 0));
      }

      // Non-blocking getRow/putRow (Beam pattern). Default handler busy-waits on empty hops and
      // deadlocks Stream Lookup after readLookupValues when the main hop is empty in this thread.
      for (TransformMetaDataCombi c : pipeline.getTransforms()) {
        if (c.transform instanceof BaseTransform baseTransform) {
          baseTransform.setRowHandler(new SparkRowHandler(baseTransform));
        }
      }

      TransformMetaDataCombi transformCombi = findCombi(pipeline, transformName);
      mainTransform = transformCombi.transform;
      // Every transform of the mini-pipeline that reports metrics: this one, then a fused chain
      List<ITransform> chain = new ArrayList<>();
      chain.add(mainTransform);
      for (FusedStage stage : fusedStages) {
        chain.add(findCombi(pipeline, stage.name()).transform);
      }
      chainForErrors = chain;
      publishMetrics(chain, copyNr, host, partitionStartMs, true, false);

      // Execution data sampling → driver accumulator + optional local registerData
      sampling =
          new SparkTransformExecutionSampling(
              transformName, parentLogChannelId, copyNr, sampleDataAccumulator);
      List<SparkTransformExecutionSampling> stageSamplings = new ArrayList<>();
      try {
        sampling.lookup(variables, metadataProvider, runConfigName, dataSamplersJson);
        if (sampling.isActive()) {
          sampling.registerExecutingTransform(pipeline);
          sampling.attach(
              variables,
              pipeline,
              mainTransform,
              inputRowMeta,
              fusedStages.isEmpty()
                  ? outputRowMeta
                  : JsonRowMeta.fromJson(fusedStages.get(0).inputRowMetaJson()));
          for (int s = 0; s < fusedStages.size(); s++) {
            FusedStage stage = fusedStages.get(s);
            SparkTransformExecutionSampling stageSampling =
                new SparkTransformExecutionSampling(
                    stage.name(), parentLogChannelId, copyNr, sampleDataAccumulator);
            stageSampling.lookup(variables, metadataProvider, runConfigName, dataSamplersJson);
            if (stageSampling.isActive()) {
              stageSampling.registerExecutingTransform(pipeline);
              stageSampling.attach(
                  variables,
                  pipeline,
                  chain.get(s + 1),
                  JsonRowMeta.fromJson(stage.inputRowMetaJson()),
                  JsonRowMeta.fromJson(stage.outputRowMetaJson()));
              stageSamplings.add(stageSampling);
            }
          }
        }
      } catch (Throwable sampleEx) {
        // Non-fatal: pipeline must still process data. Catch Throwable so LinkageError
        // (Jackson dual classloaders on local[*]) cannot abort the Spark stage.
        LogChannel.GENERAL.logError(
            "Execution data sampling disabled for transform '"
                + transformName
                + "' on partition "
                + copyNr
                + " (non-fatal): "
                + sampleEx.getMessage(),
            sampleEx instanceof Exception ex ? ex : new Exception(sampleEx));
        sampling = null;
      }

      // Untagged main-path capture (no target streams) or per-target capture lists (Beam pattern)
      List<Object[]> resultRows = new ArrayList<>();
      List<List<Object[]>> targetResultRowsList = new ArrayList<>();
      final boolean multiTarget = !targetTransforms.isEmpty();
      // Plain case: one main input, no info or target streams — rows go through a one-slot
      // handler straight into the Spark output queue; no Injector, row sets or executor loop.
      final boolean direct =
          !inputTransform
              && !acceptFilenamesFromMain
              && !multiTarget
              && infoTransforms.isEmpty()
              && mainTransform instanceof BaseTransform
              && !needsBatchComplete(transformPluginId);
      if (!fusedStages.isEmpty() && !direct) {
        throw new HopException(
            "Transform '"
                + transformName
                + "' can not run in a fused chain: it needs more than one plain input and output");
      }
      if (direct) {
        // output captured by SparkDirectRowHandler.putRow
      } else if (!multiTarget) {
        transformCombi.transform.addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                resultRows.add(HopSparkRowConverter.toNormalStorage(rowMeta, row));
              }
            });
      } else {
        for (String targetTransform : targetTransforms) {
          List<Object[]> targetResultRows = new ArrayList<>();
          targetResultRowsList.add(targetResultRows);
          TransformMetaDataCombi targetCombi = findCombi(pipeline, targetTransform);
          targetCombi.transform.addRowListener(
              new RowAdapter() {
                @Override
                public void rowReadEvent(IRowMeta rowMeta, Object[] row)
                    throws HopTransformException {
                  targetResultRows.add(HopSparkRowConverter.toNormalStorage(rowMeta, row));
                }
              });
        }
      }

      SingleThreadedPipelineExecutor executor = new SingleThreadedPipelineExecutor(pipeline);
      if (!executor.init()) {
        throw new HopException(
            "Error initializing single-threaded executor for transform '" + transformName + "'");
      }
      // Init calls setInternalVariables() with log-channel UUID / copy 0 — re-apply partition
      // scope and beamContext so Text File Output auto-suffix and ${Internal.Transform.*} work.
      applySparkParallelFileContext(pipeline, transformName, copyNr);

      pipeline.startThreads();

      // Load info streams once per partition (before main rows), same pattern as Beam TransformFn:
      // put each side-input row through the info Injector via RowProducer, then processRow so the
      // hop into Stream Lookup (etc.) is filled. finished() + one more processRow flags the
      // injector done so readLookupValues / getRowFrom see isDone() and do not busy-wait.
      for (int i = 0; i < infoTransforms.size(); i++) {
        String infoName = infoTransforms.get(i);
        IRowMeta infoRowMeta = infoRowMetas.get(i);
        List<Object[]> infoData =
            infoBroadcasts.get(i) != null ? infoBroadcasts.get(i).value() : Collections.emptyList();
        RowProducer infoRowProducer = infoRowProducers.get(i);
        TransformMetaDataCombi infoCombi = findCombi(pipeline, infoName);
        for (Object[] infoRow : infoData) {
          infoRowProducer.putRow(infoRowMeta, infoRow);
          infoCombi.transform.processRow();
        }
        infoRowProducer.finished();
        infoCombi.transform.processRow();
      }

      MetricsThrottle throttle = new MetricsThrottle();
      final SparkTransformExecutionSampling samplingRef = sampling;
      final ITransform mainRef = mainTransform;
      final ITransform lastRef = chain.get(chain.size() - 1);
      final RowProducer mainProducer = rowProducer;
      final TransformMetaDataCombi injectorCombi = mainInjectorCombi;
      final long[] rowsSeen = {0L};

      // Rows are handed to Spark lazily: every step() pushes at most one input row (or runs one
      // iteration of a source) and queues what the transform produced. The partition output is
      // never materialised, so memory is bounded by the transform's own state, not by the
      // partition size, and the downstream Spark operator can start consuming immediately.
      PartitionIterator<O> rows;
      if (inputTransform) {
        // Source transform (or one that only reads info streams): drive until finished with no
        // external input
        rows =
            new PartitionIterator<O>(
                codec, multiTarget, targetTransforms, resultRows, targetResultRowsList) {
              private boolean more = true;

              @Override
              boolean step() throws HopException {
                if (!more || pipeline.isFinished() || pipeline.getErrors() != 0) {
                  return false;
                }
                clearCapture(resultRows, targetResultRowsList);
                // One processRow() at a time, not the executor's oneIteration(): for a transform
                // without input that reads until the source is exhausted, and the whole partition
                // would be held in memory before Spark gets its first row
                more = mainRef.processRow();
                drainDownstream(pipeline, mainRef);
                if (throttle.shouldPublish(resultRows.size())) {
                  publishMetrics(mainRef, copyNr, host, partitionStartMs, true, false);
                  flushSamplesQuietly(samplingRef, false);
                }
                return true;
              }
            };
      } else if (acceptFilenamesFromMain) {
        // Accept-filenames (Text File Input, Excel, …):
        // 1) Pre-load every partition row (filenames) into the main injector and mark finished.
        // 2) Drive the file reader with processRow() directly until it returns false.
        //
        // We cannot use SingleThreadedPipelineExecutor.oneIteration() after filenames are
        // drained: with a non-empty input rowset list it only calls processRow once per
        // remaining hop size. After the first processRow consumes all filenames, size is 0
        // and further oneIteration() calls never advance the reader (infinite stall after
        // openNextFile/createReader — the log line users see just before hang).
        while (input.hasNext()) {
          Object[] hopRow = codec.filenameRowToHop(input.next());
          rowProducer.putRow(inputRowMeta, hopRow, false);
          if (mainInjectorCombi != null) {
            mainInjectorCombi.transform.processRow();
          }
          rowsSeen[0]++;
        }
        if (rowProducer != null) {
          rowProducer.finished();
          if (mainInjectorCombi != null) {
            mainInjectorCombi.transform.processRow();
          }
        }
        if (rowsSeen[0] > 0 || throttle.shouldPublish(0)) {
          publishMetrics(mainTransform, copyNr, host, partitionStartMs, true, false);
        }
        rows =
            new PartitionIterator<O>(
                codec, multiTarget, targetTransforms, resultRows, targetResultRowsList) {
              private boolean more = true;
              private long contentRows = 0;

              @Override
              boolean step() throws HopException {
                if (!more || pipeline.isFinished() || pipeline.getErrors() != 0) {
                  return false;
                }
                clearCapture(resultRows, targetResultRowsList);
                more = mainRef.processRow();
                contentRows += resultRows.size();
                if (throttle.shouldPublish(resultRows.size())
                    || contentRows % METRICS_ROW_INTERVAL == 0) {
                  publishMetrics(mainRef, copyNr, host, partitionStartMs, true, false);
                  flushSamplesQuietly(samplingRef, false);
                }
                return true;
              }
            };
      } else if (direct) {
        rows =
            new PartitionIterator<O>(
                codec, multiTarget, targetTransforms, resultRows, targetResultRowsList) {
              private boolean more = true;
              private boolean batchCompleted = false;

              @Override
              boolean step() throws HopException {
                if (chainErrors(chain)) {
                  return false;
                }
                if (!more) {
                  // A transform that buffers (Table output, Update, Insert / update, Neo4j
                  // output ...) writes its last batch, and the rows of it, in batchComplete(),
                  // which the single-threaded executor calls after every iteration. Here it is
                  // called once, at the end of the partition
                  if (batchCompleted) {
                    return false;
                  }
                  batchCompleted = true;
                  completeLastBatch(lastRef);
                  return !chainErrors(chain);
                }
                // The last transform pulls its rows through the handler: from the partition, or
                // in a fused chain from the transform before it, which pulls from the one before
                more = lastRef.processRow();
                if (chainErrors(chain)) {
                  return false;
                }
                if (throttle.shouldPublish(1) || rowsSeen[0] % METRICS_ROW_INTERVAL == 0) {
                  publishMetrics(chain, copyNr, host, partitionStartMs, true, false);
                  flushSamplesQuietly(samplingRef, false);
                  stageSamplings.forEach(s -> flushSamplesQuietly(s, false));
                }
                return true;
              }
            };
        Supplier<Object[]> partitionInput =
            () -> {
              if (!input.hasNext()) {
                return null;
              }
              rowsSeen[0]++;
              try {
                return codec.toHop(input.next());
              } catch (HopException e) {
                throw new HopRuntimeException(e);
              }
            };
        if (fusedStages.isEmpty()) {
          ((BaseTransform) mainTransform)
              .setRowHandler(
                  new SparkDirectRowHandler(
                      (BaseTransform) mainTransform,
                      inputRowMeta,
                      outputRowMeta,
                      partitionInput,
                      rows::emitHop));
        } else {
          List<IRowMeta> handOffRowMetas = new ArrayList<>();
          for (FusedStage stage : fusedStages) {
            handOffRowMetas.add(JsonRowMeta.fromJson(stage.inputRowMetaJson()));
          }
          wireFusedChain(
              chain, inputRowMeta, handOffRowMetas, outputRowMeta, partitionInput, rows::emitHop);
        }
      } else {
        rows =
            new PartitionIterator<O>(
                codec, multiTarget, targetTransforms, resultRows, targetResultRowsList) {
              private boolean inputDrained = false;
              private boolean more = true;

              @Override
              boolean step() throws HopException {
                if (input.hasNext()) {
                  Object[] hopRow = codec.toHop(input.next());
                  clearCapture(resultRows, targetResultRowsList);
                  mainProducer.putRow(inputRowMeta, hopRow, false);
                  // Forward the main row onto the hop before Stream Lookup's info-first
                  // processRow calls getRow() for the main stream (same timing Beam gets from
                  // topo order + non-blocking handler).
                  if (injectorCombi != null) {
                    injectorCombi.transform.processRow();
                  }
                  executor.oneIteration();
                  rowsSeen[0]++;
                  if (throttle.shouldPublish(1) || rowsSeen[0] % METRICS_ROW_INTERVAL == 0) {
                    publishMetrics(mainRef, copyNr, host, partitionStartMs, true, false);
                    flushSamplesQuietly(samplingRef, false);
                  }
                  return true;
                }
                if (inputDrained) {
                  // After the last input row a transform can still have work (a reader working
                  // through the file a row named, a mapping, a blocking transform): iterate until
                  // every transform of the mini-pipeline is done
                  if (!more || pipeline.getErrors() != 0) {
                    return false;
                  }
                  clearCapture(resultRows, targetResultRowsList);
                  more = executor.oneIteration();
                  return pipeline.getErrors() == 0;
                }
                // End of input: flag the injector done and give buffering transforms one last
                // iteration to emit what they hold.
                inputDrained = true;
                if (mainProducer != null) {
                  mainProducer.finished();
                  if (injectorCombi != null) {
                    injectorCombi.transform.processRow();
                  }
                  clearCapture(resultRows, targetResultRowsList);
                  more = executor.oneIteration();
                }
                return true;
              }
            };
      }

      // End-of-partition duties run when the consumer exhausts the iterator, or on task
      // completion if it stops early (limit, cancelled stage).
      final SparkTransformExecutionSampling samplingToClose = sampling;
      Runnable finish =
          () -> {
            try {
              if (pipeline.getErrors() > 0) {
                publishMetrics(chain, copyNr, host, partitionStartMs, false, true);
                throw new HopException(
                    "Errors detected while executing transform '"
                        + transformName
                        + "' on a Spark partition");
              }
              executor.dispose();
              publishMetrics(chain, copyNr, host, partitionStartMs, false, true);
              if (samplingToClose != null) {
                samplingToClose.close();
              }
              stageSamplings.forEach(SparkTransformExecutionSampling::close);
            } catch (HopException e) {
              throw new HopRuntimeException(
                  "Error executing Hop transform '" + transformName + "' in Spark mapPartitions",
                  e);
            } finally {
              // A single-threaded run never goes through the pipeline's own finish: let go of the
              // namespace here, or every partition leaves a file system manager behind.
              pipeline.cleanup();
            }
          };
      rows.onFinish(finish);
      // The transform resolves its files while Spark pulls the rows, on a task thread Spark reuses
      // across runs. That thread carries the VFS namespace of whichever run created it - closed
      // once a later run reuses the session, and never there on an executor of a cluster. Bind the
      // mini-pipeline's own namespace, the one holding this run's named connections, instead.
      rows.inNamespace(HopVfsNamespaces.resolve(pipeline));
      TaskContext taskContext = TaskContext.get();
      if (taskContext != null) {
        taskContext.addTaskCompletionListener(
            (org.apache.spark.util.TaskCompletionListener) ctx -> rows.finishQuietly());
      }
      // Sampling and the pipeline are closed by finish(); the catch block below only handles setup
      // failures
      sampling = null;
      preparedPipeline = null;
      return rows;
    } catch (Exception e) {
      if (mainTransform != null) {
        try {
          publishMetrics(
              chainForErrors != null ? chainForErrors : List.of(mainTransform),
              copyNr,
              host,
              partitionStartMs,
              false,
              true);
        } catch (Exception ignored) {
          // best-effort metrics on failure
        }
      } else {
        publishErrorSlice(copyNr, host, partitionStartMs);
      }
      if (sampling != null) {
        try {
          sampling.close();
        } catch (Exception ignored) {
          // best-effort
        }
      }
      if (preparedPipeline != null) {
        try {
          preparedPipeline.cleanup();
        } catch (Exception ignored) {
          // best-effort
        }
      }
      throw new HopRuntimeException(
          "Error executing Hop transform '" + transformName + "' in Spark mapPartitions", e);
    }
  }

  private static void flushSamplesQuietly(
      SparkTransformExecutionSampling sampling, boolean finished) {
    if (sampling == null || !sampling.isActive()) {
      return;
    }
    try {
      sampling.sendSamplesToLocation(finished);
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Error flushing transform samples to execution location (non-fatal)", e);
    }
  }

  private void publishMetrics(
      List<ITransform> transforms,
      int copyNr,
      String host,
      long startTimeMs,
      boolean running,
      boolean finished) {
    for (ITransform transform : transforms) {
      publishMetrics(transform, copyNr, host, startTimeMs, running, finished);
    }
  }

  private void publishMetrics(
      ITransform transform,
      int copyNr,
      String host,
      long startTimeMs,
      boolean running,
      boolean finished) {
    if (metricsAccumulator == null || transform == null) {
      return;
    }
    long endTimeMs = finished ? System.currentTimeMillis() : 0L;
    metricsAccumulator.add(
        new SparkTransformMetricSlice(
            StringUtils.isNotEmpty(transform.getTransformName())
                ? transform.getTransformName()
                : transformName,
            copyNr,
            host,
            transform.getLinesRead(),
            transform.getLinesWritten(),
            transform.getLinesInput(),
            transform.getLinesOutput(),
            transform.getErrors(),
            running,
            finished,
            startTimeMs,
            endTimeMs));
  }

  private void publishErrorSlice(int copyNr, String host, long startTimeMs) {
    if (metricsAccumulator == null) {
      return;
    }
    long now = System.currentTimeMillis();
    long start = startTimeMs > 0 ? startTimeMs : now;
    metricsAccumulator.add(
        new SparkTransformMetricSlice(
            transformName, copyNr, host, 0, 0, 0, 0, 1, false, true, start, now));
  }

  /**
   * The parent of a mini-pipeline: the Spark pipeline when it runs in this JVM (local master), so
   * the lines its transforms log (Write to log ...) are part of the pipeline's log. The registry
   * keeps no log level for it: the mini-pipeline gets the level it had, or Basic.
   */
  static ILoggingObject miniPipelineParent(String parentLogChannelId) {
    ILoggingObject registered =
        StringUtils.isEmpty(parentLogChannelId)
            ? null
            : LoggingRegistry.getInstance().getLoggingObject(parentLogChannelId);
    if (registered == null) {
      return new LoggingObject("apache-spark-transform");
    }
    SimpleLoggingObject parent =
        new SimpleLoggingObject(
            registered.getObjectName(), registered.getObjectType(), registered.getParent());
    parent.setLogChannelId(parentLogChannelId);
    parent.setContainerObjectId(registered.getContainerId());
    parent.setLogLevel(
        registered.getLogLevel() != null ? registered.getLogLevel() : LogLevel.BASIC);
    return parent;
  }

  /**
   * The transforms of a mini-pipeline log as copy {@code partition}, the copy number {@code
   * Internal.Transform.CopyNr} gives them: {@code name.3} for Spark partition 3. A transform keeps
   * copy number 0 itself, which the hops of the mini-pipeline are made with.
   */
  static void logAsPartition(LocalPipelineEngine pipeline, int partition) {
    String copy = Integer.toString(partition);
    for (TransformMetaDataCombi combi : pipeline.getTransforms()) {
      if (combi.transform == null || combi.transform.getLogChannel() == null) {
        continue;
      }
      ILoggingObject registered =
          LoggingRegistry.getInstance()
              .getLoggingObject(combi.transform.getLogChannel().getLogChannelId());
      if (registered instanceof LoggingObject loggingObject) {
        loggingObject.setObjectCopy(copy);
      }
    }
  }

  private static int partitionId() {
    TaskContext ctx = TaskContext.get();
    return ctx != null ? ctx.partitionId() : 0;
  }

  private static String localHost() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return null;
    }
  }

  private static TransformMeta createInjectorTransform(
      PipelineMeta pipelineMeta,
      String injectorTransformName,
      IRowMeta injectorRowMeta,
      int x,
      int y) {
    InjectorMeta injectorMeta = new InjectorMeta();
    for (IValueMeta valueMeta : injectorRowMeta.getValueMetaList()) {
      injectorMeta
          .getInjectorFields()
          .add(
              new InjectorField(
                  valueMeta.getName(),
                  valueMeta.getTypeDesc(),
                  Integer.toString(valueMeta.getLength()),
                  Integer.toString(valueMeta.getPrecision())));
    }
    TransformMeta injectorTransformMeta = new TransformMeta(injectorTransformName, injectorMeta);
    injectorTransformMeta.setLocation(x, y);
    pipelineMeta.addTransform(injectorTransformMeta);
    return injectorTransformMeta;
  }

  /**
   * Lets the transforms after a source (the Dummy of every target stream) read the rows the source
   * just wrote, so their row listeners capture them and the row sets stay empty.
   */
  static void drainDownstream(LocalPipelineEngine pipeline, ITransform source) throws HopException {
    for (TransformMetaDataCombi combi : pipeline.getTransforms()) {
      if (combi.transform == source) {
        continue;
      }
      int rows = 0;
      for (IRowSet rowSet : combi.transform.getInputRowSets()) {
        rows += rowSet.size();
      }
      for (int i = 0; i < rows; i++) {
        combi.transform.processRow();
      }
    }
  }

  private static TransformMetaDataCombi findCombi(LocalPipelineEngine pipeline, String name) {
    for (TransformMetaDataCombi combi : pipeline.getTransforms()) {
      if (combi.transformName.equals(name)) {
        return combi;
      }
    }
    throw new HopRuntimeException(
        "Configuration error, transform '" + name + "' not found in mini-pipeline");
  }

  /**
   * Sets partition-scoped {@code Internal.Transform.*} variables on a variable space (readable ID =
   * {@code transformName-partitionId}).
   */
  static void applyPartitionInternalVariables(
      IVariables variables, String transformName, int partitionId) {
    if (variables == null) {
      return;
    }
    String partitionIdStr = Integer.toString(partitionId);
    String instanceId = transformName + "-" + partitionIdStr;
    variables.setVariable(Const.INTERNAL_VARIABLE_TRANSFORM_NAME, transformName);
    variables.setVariable(Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR, partitionIdStr);
    variables.setVariable(Const.INTERNAL_VARIABLE_TRANSFORM_ID, instanceId);
    variables.setVariable(Const.INTERNAL_VARIABLE_TRANSFORM_BUNDLE_NR, partitionIdStr);
    variables.setVariable(SparkConst.VAR_PARTITION_NR, partitionIdStr);
  }

  /**
   * Publish the same synthetic owner id used by {@link
   * org.apache.hop.spark.execution.SparkTransformExecutionSampling} so nested engines can parent
   * their execution-info rows for GUI drill-down.
   */
  static void applySparkTransformOwnerId(
      IVariables variables, String parentLogChannelId, String transformName, int partitionId) {
    if (variables == null || StringUtils.isEmpty(transformName)) {
      return;
    }
    String parent =
        StringUtils.isNotEmpty(parentLogChannelId) ? parentLogChannelId : "spark-pipeline";
    String ownerId = parent + "|" + transformName + "|" + partitionId;
    variables.setVariable(SparkConst.VAR_TRANSFORM_OWNER_ID, ownerId);
  }

  /**
   * After package materialize, resolve nested pipeline paths (Simple Mapping, Pipeline Executor, …)
   * to absolute files on this JVM and verify they exist.
   */
  static void resolveNestedPipelineFilename(
      ITransformMeta iTransformMeta, IVariables variables, String transformName)
      throws HopException {
    if (!(iTransformMeta instanceof TransformWithMappingMeta<?, ?> mappingMeta)) {
      return;
    }
    String filename = mappingMeta.getFilename();
    if (StringUtils.isEmpty(filename)) {
      throw new HopException(
          "Transform '"
              + transformName
              + "' is a nested pipeline transform but has no filename configured");
    }
    String resolved = variables != null ? variables.resolve(filename) : filename;
    if (StringUtils.isEmpty(resolved) || resolved.contains("${") || resolved.contains("%")) {
      throw new HopException(
          "Could not resolve nested pipeline filename for transform '"
              + transformName
              + "': '"
              + filename
              + "' → '"
              + resolved
              + "' (PROJECT_HOME="
              + (variables != null ? variables.getVariable(CONST_PROJECT_HOME) : null)
              + ", package="
              + (variables != null
                  ? variables.getVariable(SparkProjectPackage.VAR_PACKAGE_URI)
                  : null)
              + "). Deploy with Upload Spark project package so PROJECT_HOME is set on every"
              + " executor.");
    }
    File f = new File(resolved);
    boolean exists = f.isFile();
    if (!exists) {
      try {
        exists = HopVfs.fileExists(resolved);
      } catch (Exception e) {
        throw new HopException(
            "Nested pipeline file not found for transform '"
                + transformName
                + "': "
                + resolved
                + " (from '"
                + filename
                + "')",
            e);
      }
    }
    if (!exists) {
      throw new HopException(
          "Nested pipeline file not found for transform '"
              + transformName
              + "': "
              + resolved
              + " (from '"
              + filename
              + "'). Check that hop-spark-package.zip contains this path under project/ "
              + "(e.g. project/mappings/add-uuid.hpl) and re-run Deploy & run with package"
              + " upload.");
    }
    mappingMeta.setFilename(resolved);
  }

  /**
   * After package materialize, ensure PROJECT_HOME is a real directory on this JVM. Detects the
   * common failure where only the driver extract path was broadcast and workers never re-extracted
   * the package (nested Simple Mapping / Pipeline Executor then fail to open {@code
   * ${PROJECT_HOME}/…}).
   */
  static void validateProjectHomeOnWorker(IVariables variables) throws HopException {
    if (variables == null) {
      return;
    }
    String packageUri = variables.getVariable(SparkProjectPackage.VAR_PACKAGE_URI);
    String sparkFile = variables.getVariable(SparkProjectPackage.VAR_PACKAGE_SPARK_FILE);
    String projectHome = variables.getVariable(CONST_PROJECT_HOME);
    if (StringUtils.isEmpty(packageUri)
        && StringUtils.isEmpty(sparkFile)
        && StringUtils.isEmpty(projectHome)) {
      return;
    }
    if (StringUtils.isNotEmpty(projectHome)) {
      File home = new File(projectHome);
      if (home.isDirectory()) {
        return;
      }
      throw new HopException(
          "PROJECT_HOME is not a directory on this Spark executor: "
              + projectHome
              + ". Package URI="
              + packageUri
              + ", SparkFiles basename="
              + sparkFile
              + ". The driver extract under /tmp is not visible on workers — re-run with a fat jar"
              + " that stages the project package via SparkFiles (local copy + addFile) and/or a"
              + " UC Volume path every node can open. Nested definition files (mappings,"
              + " sub-pipelines) live under project/ inside the package zip.");
    }
    if (StringUtils.isNotEmpty(packageUri) || StringUtils.isNotEmpty(sparkFile)) {
      throw new HopException(
          "HOP_SPARK_PROJECT_PACKAGE is set ("
              + packageUri
              + ", sparkFile="
              + sparkFile
              + ") but PROJECT_HOME was not set after package materialization on this executor.");
    }
  }

  /**
   * Beam-parity parallel file context for classic I/O in mapPartitions: enable {@code beamContext}
   * (Text File Output auto-appends {@code _transformId_bundleNr}) and re-apply partition-scoped
   * internal variables after transform init overwrote them with log-channel UUIDs.
   */
  static void applySparkParallelFileContext(
      LocalPipelineEngine pipeline, String primaryTransformName, int partitionId) {
    if (pipeline == null) {
      return;
    }
    for (TransformMetaDataCombi combi : pipeline.getTransforms()) {
      if (combi.data != null) {
        combi.data.setBeamContext(true);
        combi.data.setBeamBundleNr(partitionId);
      }
    }
    applyPartitionTransformVariables(pipeline, primaryTransformName, partitionId);
  }

  /**
   * The partition as {@code Internal.Transform.CopyNr} and bundle number of every transform of the
   * mini-pipeline, with {@code Internal.Transform.ID} = transform name + partition. A transform
   * sets its own copy number, 0, when it is created.
   */
  static void applyPartitionTransformVariables(
      LocalPipelineEngine pipeline, String primaryTransformName, int partitionId) {
    if (pipeline == null) {
      return;
    }
    String partitionIdStr = Integer.toString(partitionId);
    String ownerId = pipeline.getVariable(SparkConst.VAR_TRANSFORM_OWNER_ID);
    for (TransformMetaDataCombi combi : pipeline.getTransforms()) {
      if (combi.transform instanceof BaseTransform baseTransform) {
        String name =
            StringUtils.isNotEmpty(combi.transformName)
                ? combi.transformName
                : primaryTransformName;
        String instanceId = name + "-" + partitionIdStr;
        baseTransform.setVariable(Const.INTERNAL_VARIABLE_TRANSFORM_NAME, name);
        baseTransform.setVariable(Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR, partitionIdStr);
        baseTransform.setVariable(Const.INTERNAL_VARIABLE_TRANSFORM_ID, instanceId);
        baseTransform.setVariable(Const.INTERNAL_VARIABLE_TRANSFORM_BUNDLE_NR, partitionIdStr);
        if (StringUtils.isNotEmpty(ownerId)) {
          baseTransform.setVariable(SparkConst.VAR_TRANSFORM_OWNER_ID, ownerId);
        }
      }
    }
  }

  /**
   * When a file input transform accepts filenames from a previous hop, re-point {@code
   * accept_transform_name} at the mini-pipeline main injector so {@code findInputRowSet} succeeds.
   * Covers {@link BaseFileInputMeta} (Text File Input, JSON, VCard, …) and Excel-style metas with
   * {@code isAcceptingFilenames}/{@code setAcceptingTransformName}.
   *
   * @return true when the transform consumes filename rows from the main injector this way
   */
  static boolean bindAcceptingFilenamesToMainInjector(ITransformMeta meta) {
    if (meta == null) {
      return false;
    }
    if (meta instanceof BaseFileInputMeta<?, ?, ?> baseFile) {
      if (baseFile.isAcceptingFilenames()) {
        baseFile.setAcceptingTransformName(SparkConst.INJECTOR_TRANSFORM_NAME);
        return true;
      }
      return false;
    }
    // ExcelInputMeta (and similar) are not BaseFileInputMeta but use the same contract
    try {
      Object accepting = meta.getClass().getMethod("isAcceptingFilenames").invoke(meta);
      if (Boolean.TRUE.equals(accepting)) {
        meta.getClass()
            .getMethod("setAcceptingTransformName", String.class)
            .invoke(meta, SparkConst.INJECTOR_TRANSFORM_NAME);
        return true;
      }
    } catch (ReflectiveOperationException ignored) {
      // not an accept-filenames transform
    }
    return false;
  }

  /**
   * Drive the single-threaded mini-pipeline until it finishes (source transforms with no main input
   * hop — Row Generator, Get File Names, etc.).
   */
  private static void clearCapture(
      List<Object[]> resultRows, List<List<Object[]>> targetResultRowsList) {
    resultRows.clear();
    for (List<Object[]> list : targetResultRowsList) {
      list.clear();
    }
  }

  /**
   * Lazy partition output. {@link #step()} advances the mini-pipeline by one unit of work and
   * leaves whatever the transform emitted in the capture lists; those rows are converted and
   * queued, and handed to Spark one at a time.
   */
  private abstract static class PartitionIterator<O> implements Iterator<O> {
    private final PartitionCodec<?, O> codec;
    private final boolean multiTarget;
    private final List<String> targetTransforms;
    private final List<Object[]> resultRows;
    private final List<List<Object[]>> targetResultRowsList;
    private final ArrayDeque<O> pending = new ArrayDeque<>();
    private Runnable finish;
    private boolean finished;
    private boolean exhausted;

    /** The namespace the transform resolves its files in, bound for as long as it works. */
    private HopVfsNamespace vfsNamespace;

    PartitionIterator(
        PartitionCodec<?, O> codec,
        boolean multiTarget,
        List<String> targetTransforms,
        List<Object[]> resultRows,
        List<List<Object[]>> targetResultRowsList) {
      this.codec = codec;
      this.multiTarget = multiTarget;
      this.targetTransforms = targetTransforms;
      this.resultRows = resultRows;
      this.targetResultRowsList = targetResultRowsList;
    }

    /** Do one unit of work; return false when the transform has nothing left to produce. */
    abstract boolean step() throws HopException;

    /** Queue a row the transform wrote, in normal storage (direct handler path). */
    void emitHop(Object[] hopRow) {
      try {
        pending.add(codec.toSpark(hopRow));
      } catch (HopException e) {
        throw new HopRuntimeException("Error converting a Hop row for Spark", e);
      }
    }

    void onFinish(Runnable finish) {
      this.finish = finish;
    }

    void inNamespace(HopVfsNamespace vfsNamespace) {
      this.vfsNamespace = vfsNamespace;
    }

    @Override
    public boolean hasNext() {
      HopVfsNamespace previous = HopVfsNamespaces.bindThread(vfsNamespace);
      try {
        return advance();
      } finally {
        HopVfsNamespaces.restoreThread(previous);
      }
    }

    private boolean advance() {
      try {
        while (pending.isEmpty() && !exhausted) {
          if (step()) {
            queueCaptured();
          } else {
            exhausted = true;
          }
        }
      } catch (Exception e) {
        throw new HopRuntimeException("Error executing Hop transform in Spark mapPartitions", e);
      }
      if (pending.isEmpty()) {
        runFinish();
        return false;
      }
      return true;
    }

    @Override
    public O next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return pending.poll();
    }

    private void queueCaptured() throws HopException {
      if (!multiTarget) {
        for (Object[] hopRow : resultRows) {
          pending.add(codec.toSpark(hopRow));
        }
        return;
      }
      for (int t = 0; t < targetResultRowsList.size(); t++) {
        for (Object[] hopRow : targetResultRowsList.get(t)) {
          pending.add(codec.toTaggedSpark(targetTransforms.get(t), hopRow));
        }
      }
    }

    private void runFinish() {
      if (!finished) {
        finished = true;
        if (finish != null) {
          finish.run();
        }
      }
    }

    /** Task-completion hook: release the mini-pipeline even when Spark stopped pulling early. */
    void finishQuietly() {
      HopVfsNamespace previous = HopVfsNamespaces.bindThread(vfsNamespace);
      try {
        runFinish();
      } catch (Exception e) {
        LogChannel.GENERAL.logError("Error finishing Hop transform partition (non-fatal)", e);
      } finally {
        HopVfsNamespaces.restoreThread(previous);
      }
    }
  }

  /** Throttles metric publishes by wall-clock time (row interval handled by caller). */
  private static final class MetricsThrottle implements Serializable {
    private static final long serialVersionUID = 1L;
    private long lastPublishMs = 0L;

    private int calls = 0;

    boolean shouldPublish(int rowsThisBatch) {
      // The clock read is a syscall; sampling it every 128 calls keeps the ~1 s cadence for slow
      // transforms without paying for it on every row of a fast one.
      if (lastPublishMs != 0L && (++calls & 127) != 0) {
        return false;
      }
      long now = System.currentTimeMillis();
      if (lastPublishMs == 0L || now - lastPublishMs >= METRICS_TIME_INTERVAL_MS) {
        lastPublishMs = now;
        return true;
      }
      return false;
    }
  }

  /**
   * Transforms that drive a child pipeline from {@code batchComplete()}. The direct row path calls
   * {@code processRow()} per row and never {@code batchComplete()}, so these hang there and need
   * the single-threaded executor path.
   */
  private static final Set<String> BATCH_COMPLETE_PLUGIN_IDS = Set.of("SimpleMapping");

  /**
   * Transforms that already write their last group or row when the input ends, and whose
   * batchComplete() would write it again (it starts a new batch with what they still hold).
   */
  private static final Set<String> LAST_OUTPUT_AT_END_PLUGIN_IDS =
      Set.of("GroupBy", "MemoryGroupBy", "Denormaliser", "Unique");

  /**
   * At the end of the partition, let a transform that buffers write its last batch: Table output,
   * Update, Insert / update, the Neo4j outputs ... write it, and its rows, in batchComplete(),
   * which the single-threaded executor calls after every iteration.
   */
  static void completeLastBatch(ITransform transform) throws HopException {
    String pluginId =
        transform.getTransformMeta() != null
            ? transform.getTransformMeta().getTransformPluginId()
            : null;
    if (!LAST_OUTPUT_AT_END_PLUGIN_IDS.contains(pluginId)) {
      transform.batchComplete();
    }
  }

  /**
   * Wire a fused chain: the first transform reads the partition, every next one pulls the rows of
   * the one before it, and the last one writes into the Spark output. Rows go from one transform to
   * the next in the layout of {@code handOffRowMetas} (one per link, the input of the next
   * transform as the driver computed it), as they would through Spark.
   */
  static void wireFusedChain(
      List<ITransform> chain,
      IRowMeta inputRowMeta,
      List<IRowMeta> handOffRowMetas,
      IRowMeta outputRowMeta,
      Supplier<Object[]> partitionInput,
      Consumer<Object[]> sparkOutput)
      throws HopException {
    Supplier<Object[]> input = partitionInput;
    IRowMeta stageInputRowMeta = inputRowMeta;
    for (int i = 0; i < chain.size(); i++) {
      if (!(chain.get(i) instanceof BaseTransform transform)) {
        throw new HopException(
            "Transform '" + chain.get(i).getTransformName() + "' can not run in a fused chain");
      }
      if (i == chain.size() - 1) {
        transform.setRowHandler(
            new SparkDirectRowHandler(
                transform, stageInputRowMeta, outputRowMeta, input, sparkOutput));
      } else {
        // The layout this transform writes = what the driver computed as the next one's input
        IRowMeta stageOutputRowMeta = handOffRowMetas.get(i);
        ArrayDeque<Object[]> handOff = new ArrayDeque<>();
        transform.setRowHandler(
            SparkDirectRowHandler.toNextTransform(
                transform, stageInputRowMeta, stageOutputRowMeta, input, handOff::add));
        input = new UpstreamPull(transform, handOff);
        stageInputRowMeta = stageOutputRowMeta;
      }
    }
  }

  /**
   * The input of a transform in a fused chain: the rows the transform before it wrote, asking it
   * for more until it has some or is done. Null only at the real end of the stream.
   */
  static final class UpstreamPull implements Supplier<Object[]> {
    private final ITransform upstream;
    private final ArrayDeque<Object[]> rows;
    private boolean more = true;

    UpstreamPull(ITransform upstream, ArrayDeque<Object[]> rows) {
      this.upstream = upstream;
      this.rows = rows;
    }

    @Override
    public Object[] get() {
      while (rows.isEmpty() && more) {
        try {
          more = upstream.processRow();
          if (!more) {
            // The last batch of a buffering transform, and its rows (see the direct step)
            completeLastBatch(upstream);
          }
        } catch (HopException e) {
          throw new HopRuntimeException(
              "Error in transform '" + upstream.getTransformName() + "'", e);
        }
        if (upstream.getErrors() > 0) {
          throw new HopRuntimeException(
              "Errors detected while executing transform '" + upstream.getTransformName() + "'");
        }
      }
      return rows.poll();
    }
  }

  private static boolean chainErrors(List<ITransform> chain) {
    for (ITransform transform : chain) {
      if (transform.getErrors() > 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether a transform reads file names from a previous hop (Text file input, Excel input, ...
   * with "accept file names"). Does not change the metadata, unlike {@link
   * #bindAcceptingFilenamesToMainInjector}.
   */
  public static boolean acceptsFilenamesFromHop(ITransformMeta meta) {
    if (meta == null) {
      return false;
    }
    if (meta instanceof BaseFileInputMeta<?, ?, ?> baseFile) {
      return baseFile.isAcceptingFilenames();
    }
    try {
      return Boolean.TRUE.equals(meta.getClass().getMethod("isAcceptingFilenames").invoke(meta));
    } catch (ReflectiveOperationException e) {
      return false;
    }
  }

  public static boolean needsBatchComplete(String transformPluginId) {
    return BATCH_COMPLETE_PLUGIN_IDS.contains(transformPluginId);
  }
}
