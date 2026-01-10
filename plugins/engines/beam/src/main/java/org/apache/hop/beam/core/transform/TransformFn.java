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
 *
 */

package org.apache.hop.beam.core.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.Serial;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.shared.VariableValue;
import org.apache.hop.beam.core.util.HopBeamUtil;
import org.apache.hop.beam.engines.HopPipelineExecutionOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.joda.time.Instant;
import org.json.simple.parser.ParseException;

public class TransformFn extends TransformBaseFn {
  @Serial private static final long serialVersionUID = 95700000000000001L;

  public static final String INJECTOR_TRANSFORM_NAME = "_INJECTOR_";

  protected List<VariableValue> variableValues;
  protected String metastoreJson;
  protected String transformPluginId;
  protected String transformMetaInterfaceXml;
  protected String inputRowMetaJson;
  protected String dataSamplersJson;
  protected List<String> targetTransforms;
  protected List<String> infoTransforms;
  protected List<String> infoRowMetaJsons;
  protected boolean inputTransform;
  protected boolean initialize;

  protected List<PCollectionView<List<HopRow>>> infoCollectionViews;
  protected List<PCollection<HopRow>> infoCollections;

  // Log and count parse errors.
  private final Counter numErrors = Metrics.counter("main", "TransformProcessErrors");

  private transient PipelineMeta pipelineMeta;
  private transient TransformMeta transformMeta;
  private transient IRowMeta inputRowMeta;
  private transient List<TransformMetaDataCombi> transformCombis;
  private transient LocalPipelineEngine pipeline;
  private transient RowProducer rowProducer;
  private transient List<HopRow> resultRows;
  private transient List<List<Object[]>> targetResultRowsList;

  private transient TupleTag<HopRow> mainTupleTag;
  private transient List<TupleTag<HopRow>> tupleTagList;

  private transient Counter readCounter;
  private transient Counter writtenCounter;

  public TransformFn() {
    super(null, null, null);
  }

  // I created a private class because instances of this one need access to infoCollectionViews
  //

  public TransformFn(
      List<VariableValue> variableValues,
      String metastoreJson,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms,
      List<String> infoTransforms,
      List<String> infoRowMetaJsons,
      String dataSamplersJson,
      String runConfigName,
      String parentLogChannelId,
      List<PCollectionView<List<HopRow>>> infoCollectionViews) {
    super(parentLogChannelId, runConfigName, dataSamplersJson);
    this.variableValues = variableValues;
    this.metastoreJson = metastoreJson;
    this.transformName = transformName;
    this.transformPluginId = transformPluginId;
    this.transformMetaInterfaceXml = transformMetaInterfaceXml;
    this.inputRowMetaJson = inputRowMetaJson;
    this.inputTransform = inputTransform;
    this.targetTransforms = targetTransforms;
    this.infoTransforms = infoTransforms;
    this.infoRowMetaJsons = infoRowMetaJsons;
    this.dataSamplersJson = dataSamplersJson;
    this.infoCollectionViews = infoCollectionViews;
    this.initialize = true;
  }

  @Setup
  public void setup() {
    // Do nothing
  }

  /**
   * Reset the row buffer every time we start a new bundle to prevent the output of double rows
   *
   * @param startBundleContext
   */
  @StartBundle
  public void startBundle(StartBundleContext startBundleContext) {
    try {
      // TODO: create a test to see if this is still needed
      //
      //      if ("ScriptValueMod".equals(transformPluginId) && pipeline != null) {
      //        // Force re-initialization for this specific transform plugin
      //        initialize = true;
      //      }
      //

      // Call start of the bundle on the transform
      //
      if (executor != null) {
        // Increment the bundle number before calling the next startBundle() method in the Hop
        // transforms.
        //
        executor
            .getPipeline()
            .getTransforms()
            .forEach(combi -> combi.data.setBeamBundleNr(combi.data.getBeamBundleNr() + 1));

        executor.startBundle();
      }
    } catch (HopException e) {
      throw new RuntimeException("Error at start of bundle!", e);
    }
  }

  @ProcessElement
  public void processElement(ProcessContext context, BoundedWindow window) {
    try {
      if (initialize) {
        initializeTransformPipeline(context);
      }

      // Get one row from the context main input and make a copy, so we can change it.
      //
      HopRow originalInputRow = context.element();
      HopRow inputRow = HopBeamUtil.copyHopRow(originalInputRow, inputRowMeta);
      readCounter.inc();

      emptyRowBuffer(new TransformProcessContext(context), inputRow);
    } catch (Exception e) {
      numErrors.inc();
      LOG.error("Transform execution error :" + e.getMessage());
      throw new RuntimeException("Error executing TransformFn", e);
    }
  }

  private void initializeTransformPipeline(DoFn<HopRow, HopRow>.ProcessContext context)
      throws HopException, ParseException, JsonProcessingException {
    initialize = false;
    // Initialize Hop and load extra plugins as well
    //
    BeamHop.init();

    // The content of the metadata is JSON serialized and inflated below.
    //
    IHopMetadataProvider metadataProvider = new SerializableMetadataProvider(metastoreJson);
    IVariables variables = new Variables();
    for (VariableValue variableValue : variableValues) {
      if (StringUtils.isNotEmpty(variableValue.getVariable())) {
        variables.setVariable(variableValue.getVariable(), variableValue.getValue());
      }
    }

    // Create a very simple new transformation to run single threaded...
    // Single threaded...
    //
    pipelineMeta = new PipelineMeta();
    pipelineMeta.setName(transformName);
    pipelineMeta.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    pipelineMeta.setMetadataProvider(metadataProvider);

    // Input row metadata...
    //
    inputRowMeta = JsonRowMeta.fromJson(inputRowMetaJson);
    List<IRowMeta> infoRowMetas = new ArrayList<>();
    for (String infoRowMetaJson : infoRowMetaJsons) {
      IRowMeta infoRowMeta = JsonRowMeta.fromJson(infoRowMetaJson);
      infoRowMetas.add(infoRowMeta);
    }

    // Create an Injector transform with the right row layout...
    // This will help all transforms see the row layout statically...
    //
    TransformMeta mainInjectorTransformMeta = null;
    if (!inputTransform) {
      mainInjectorTransformMeta =
          createInjectorTransform(pipelineMeta, INJECTOR_TRANSFORM_NAME, inputRowMeta, 200, 200);
    }

    // Our main transform writes to a bunch of targets
    // Add a dummy transform for each one so the transform can target them
    //
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

    // The transform might read information from info transforms
    // like "Stream Lookup" or "Validator".
    // They read all the data on input from a side input.
    //
    List<List<HopRow>> infoDataSets = new ArrayList<>();
    List<TransformMeta> infoTransformMetas = new ArrayList<>();
    for (int i = 0; i < infoTransforms.size(); i++) {
      String infoTransform = infoTransforms.get(i);
      PCollectionView<List<HopRow>> cv = infoCollectionViews.get(i);

      // Get the data from the side input, from the info transform(s)
      //
      List<HopRow> infoDataSet = context.sideInput(cv);
      infoDataSets.add(infoDataSet);

      IRowMeta infoRowMeta = infoRowMetas.get(i);

      // Add an Injector transform for every info transform so the transform can read from it
      //
      TransformMeta infoTransformMeta =
          createInjectorTransform(pipelineMeta, infoTransform, infoRowMeta, 200, 350 + 150 * i);
      infoTransformMetas.add(infoTransformMeta);
    }

    transformCombis = new ArrayList<>();

    // The main transform inflated from XML metadata...
    //
    PluginRegistry registry = PluginRegistry.getInstance();
    ITransformMeta iTransformMeta =
        registry.loadClass(TransformPluginType.class, transformPluginId, ITransformMeta.class);
    if (iTransformMeta == null) {
      throw new HopException(
          "Unable to load transform plugin with ID "
              + transformPluginId
              + ", this plugin isn't in the plugin registry or classpath");
    }

    HopBeamUtil.loadTransformMetadataFromXml(
        transformName,
        iTransformMeta,
        transformMetaInterfaceXml,
        pipelineMeta.getMetadataProvider());

    transformMeta = new TransformMeta(transformName, iTransformMeta);
    transformMeta.setTransformPluginId(transformPluginId);
    transformMeta.setLocation(400, 200);
    pipelineMeta.addTransform(transformMeta);
    if (!inputTransform) {
      pipelineMeta.addPipelineHop(new PipelineHopMeta(mainInjectorTransformMeta, transformMeta));
    }
    // The target hops as well
    //
    for (TransformMeta targetTransformMeta : targetTransformMetas) {
      pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, targetTransformMeta));
    }

    // And the info hops...
    //
    for (TransformMeta infoTransformMeta : infoTransformMetas) {
      pipelineMeta.addPipelineHop(new PipelineHopMeta(infoTransformMeta, transformMeta));
    }

    // If we are sending execution information to a location, see if we have any extra data
    // samplers
    // The data samplers list is composed of those in the data profile along with the set from
    // the extra ones in the parent pipeline.
    //
    lookupExecutionInformation(variables, metadataProvider);

    iTransformMeta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());

    // Create the transformation...
    //
    pipeline =
        new LocalPipelineEngine(
            pipelineMeta, variables, new LoggingObject("apache-beam-transform"));
    pipeline.setLogLevel(
        context.getPipelineOptions().as(HopPipelineExecutionOptions.class).getLogLevel());
    pipeline.setMetadataProvider(pipelineMeta.getMetadataProvider());

    // Change the name to make the logging less confusing.
    //
    pipeline.getPipelineRunConfiguration().setName("beam-transform-local (" + transformName + ")");

    pipeline.prepareExecution();

    // Indicate that we're dealing with a Beam pipeline during execution
    // Start counting bundle numbers from 1
    //
    pipeline
        .getTransforms()
        .forEach(
            c -> {
              c.data.setBeamContext(true);
              c.data.setBeamBundleNr(1);
            });

    // Create producers so we can efficiently pass data
    //
    rowProducer = null;
    if (!inputTransform) {
      rowProducer = pipeline.addRowProducer(INJECTOR_TRANSFORM_NAME, 0);
    }
    List<RowProducer> infoRowProducers = new ArrayList<>();
    for (String infoTransform : infoTransforms) {
      RowProducer infoRowProducer = pipeline.addRowProducer(infoTransform, 0);
      infoRowProducers.add(infoRowProducer);
    }

    // Find the right interfaces for execution later...
    //
    if (!inputTransform) {
      TransformMetaDataCombi injectorCombi = findCombi(pipeline, INJECTOR_TRANSFORM_NAME);
      transformCombis.add(injectorCombi);
    }

    TransformMetaDataCombi transformCombi = findCombi(pipeline, transformName);
    transformCombis.add(transformCombi);

    if (targetTransforms.isEmpty()) {
      IRowListener rowListener =
          new RowAdapter() {
            @Override
            public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                throws HopTransformException {
              resultRows.add(new HopRow(row, rowMeta.size()));
            }
          };
      transformCombi.transform.addRowListener(rowListener);
    }

    // Create a list of TupleTag to direct the target rows
    //
    mainTupleTag = new TupleTag<>(HopBeamUtil.createMainOutputTupleId(transformName)) {};
    tupleTagList = new ArrayList<>();

    // The lists in here will contain all the rows that ended up in the various target
    // transforms (if any)
    //
    targetResultRowsList = new ArrayList<>();

    for (String targetTransform : targetTransforms) {
      TransformMetaDataCombi targetCombi = findCombi(pipeline, targetTransform);
      transformCombis.add(targetCombi);

      String tupleId = HopBeamUtil.createTargetTupleId(transformName, targetTransform);
      TupleTag<HopRow> tupleTag = new TupleTag<>(tupleId) {};
      tupleTagList.add(tupleTag);
      final List<Object[]> targetResultRows = new ArrayList<>();
      targetResultRowsList.add(targetResultRows);

      targetCombi.transform.addRowListener(
          new RowAdapter() {
            @Override
            public void rowReadEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
              // We send the target row to a specific list...
              //
              targetResultRows.add(row);
            }
          });
    }

    attachExecutionSamplersToOutput(
        variables,
        transformName,
        pipeline.getLogChannelId(),
        inputRowMeta,
        pipelineMeta.getTransformFields(variables, transformName),
        pipeline.getTransform(transformName, 0));

    registerExecutingTransform(pipeline);

    // Change the row handler
    //
    for (TransformMetaDataCombi c : pipeline.getTransforms()) {
      ((BaseTransform) c.transform).setRowHandler(new BeamRowHandler((BaseTransform) c.transform));
    }

    executor = new SingleThreadedPipelineExecutor(pipeline);

    // Initialize the transforms...
    //
    executor.init();

    Counter initCounter = Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName);
    readCounter = Metrics.counter(Pipeline.METRIC_NAME_READ, transformName);
    writtenCounter = Metrics.counter(Pipeline.METRIC_NAME_WRITTEN, transformName);

    initCounter.inc();

    // Doesn't really start the threads in single threaded mode
    // Just sets some flags all over the place
    //
    pipeline.startThreads();

    resultRows = new ArrayList<>();

    // Copy the info data sets to the info transforms...
    // We do this only once so all subsequent rows can use this.
    //
    for (int i = 0; i < infoTransforms.size(); i++) {
      RowProducer infoRowProducer = infoRowProducers.get(i);
      List<HopRow> infoDataSet = infoDataSets.get(i);
      TransformMetaDataCombi combi = findCombi(pipeline, infoTransforms.get(i));
      IRowMeta infoRowMeta = infoRowMetas.get(i);

      // Pass and process the rows in the info transforms
      //
      for (HopRow infoRowData : infoDataSet) {
        infoRowProducer.putRow(infoRowMeta, infoRowData.getRow());
        combi.transform.processRow();
      }

      // By calling finished() transforms like Stream Lookup know no more rows are going to
      // come, and they can start to work with the info data set
      //
      infoRowProducer.finished();

      // Call once more to flag input as done, transform as finished.
      //
      combi.transform.processRow();
    }

    // Flag the start of a bundle for the first time.
    //
    executor.startBundle();
  }

  /**
   * Attempt to empty the row buffer
   *
   * @param context
   * @param inputRow
   * @throws HopException
   */
  private synchronized void emptyRowBuffer(TupleOutputContext<HopRow> context, HopRow inputRow)
      throws HopException {

    // Empty all the row buffers for another iteration
    //
    resultRows.clear();
    for (int t = 0; t < targetTransforms.size(); t++) {
      targetResultRowsList.get(t).clear();
    }

    // Pass the rows in the rowBuffer to the input RowSet
    //
    if (!inputTransform) {
      rowProducer.putRow(inputRowMeta, inputRow.getRow(), false);
    }

    // Execute all transforms in the transformation
    //
    executor.oneIteration();

    // Evaluate the results...
    //

    // Pass all rows in the output to the process context
    //
    for (HopRow resultRow : resultRows) {

      // Pass the row to the process context
      //
      context.output(mainTupleTag, resultRow);
      writtenCounter.inc();
    }

    // Pass whatever ended up on the target nodes
    //
    for (int t = 0; t < targetResultRowsList.size(); t++) {
      List<Object[]> targetRowsList = targetResultRowsList.get(t);
      TupleTag<HopRow> tupleTag = tupleTagList.get(t);

      for (Object[] targetRow : targetRowsList) {
        context.output(tupleTag, new HopRow(targetRow));
      }
    }
  }

  private TransformMeta createInjectorTransform(
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

  private TransformMetaDataCombi findCombi(Pipeline pipeline, String transformName) {
    for (TransformMetaDataCombi combi : pipeline.getTransforms()) {
      if (combi.transformName.equals(transformName)) {
        return combi;
      }
    }
    throw new RuntimeException(
        "Configuration error, transform '" + transformName + "' not found in transformation");
  }

  private class TransformProcessContext implements TupleOutputContext<HopRow> {

    private DoFn.ProcessContext context;

    public TransformProcessContext(DoFn.ProcessContext processContext) {
      this.context = processContext;
    }

    @Override
    public void output(TupleTag<HopRow> tupleTag, HopRow output) {
      context.output(tupleTag, output);
    }
  }

  private interface TupleOutputContext<T> {
    void output(TupleTag<T> tupleTag, T output);
  }

  private class TransformFinishBundleContext implements TupleOutputContext<HopRow> {

    private DoFn.FinishBundleContext context;
    private BoundedWindow batchWindow;

    public TransformFinishBundleContext(
        DoFn.FinishBundleContext context, BoundedWindow batchWindow) {
      this.context = context;
      this.batchWindow = batchWindow;
    }

    @Override
    public void output(TupleTag<HopRow> tupleTag, HopRow output) {
      context.output(tupleTag, output, Instant.now(), batchWindow);
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    try {
      // Signal the end of the bundle on the transform
      //
      if (executor != null) {
        executor.finishBundle();
      }
    } catch (Exception e) {
      numErrors.inc();
      LOG.error("Transform finishing bundle error :" + e.getMessage());
      throw new RuntimeException("Error finalizing bundle of transform '" + transformName + "'", e);
    }
  }

  @Teardown
  public void tearDown() {
    try {
      if (executor != null) {
        executor.dispose();
      }

      // Send last data from the data samplers over to the location (if any)
      //
      if (executionInfoLocation != null) {
        if (executionInfoTimer != null) {
          executionInfoTimer.cancel();
        }
        sendSamplesToLocation(true);

        // Close the location
        //
        executionInfoLocation.getExecutionInfoLocation().close();
      }
    } catch (Exception e) {
      LOG.error(
          "Error sending row samples to execution info location for transform "
              + transformName
              + " (non-fatal)",
          e);
    }
  }
}
