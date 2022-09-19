/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.core.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.shared.VariableValue;
import org.apache.hop.beam.core.util.HopBeamUtil;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.HopPipelineExecutionOptions;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.execution.sampler.ExecutionDataSamplerMeta;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.*;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TransformTransform extends PTransform<PCollection<HopRow>, PCollectionTuple> {

  protected List<VariableValue> variableValues;
  protected String metastoreJson;
  protected List<String> transformPluginClasses;
  protected List<String> xpPluginClasses;
  protected int batchSize;
  protected String transformName;
  protected String transformPluginId;
  protected String inputRowMetaJson;
  protected boolean inputTransform;
  protected String transformMetaInterfaceXml;
  protected List<String> targetTransforms;
  protected List<String> infoTransforms;
  protected List<String> infoRowMetaJsons;
  protected int flushIntervalMs;

  // Execution information vectors
  protected String runConfigName;
  protected String dataSamplersJson;
  protected String parentLogChannelId;

  // Used in the private TransformFn class below
  //
  protected List<PCollectionView<List<HopRow>>> infoCollectionViews;

  // Log and count errors.
  protected static final Logger LOG = LoggerFactory.getLogger(TransformTransform.class);
  protected static final Counter numErrors = Metrics.counter("main", "TransformErrors");

  public TransformTransform() {
    variableValues = new ArrayList<>();
  }

  public TransformTransform(
      List<VariableValue> variableValues,
      String metastoreJson,
      List<String> transformPluginClasses,
      List<String> xpPluginClasses,
      int batchSize,
      int flushIntervalMs,
      String transformName,
      String transformPluginId,
      String transformMetaInterfaceXml,
      String inputRowMetaJson,
      boolean inputTransform,
      List<String> targetTransforms,
      List<String> infoTransforms,
      List<String> infoRowMetaJsons,
      List<PCollectionView<List<HopRow>>> infoCollectionViews,
      String runConfigName,
      String dataSamplersJson,
      String parentLogChannelId) {
    this.variableValues = variableValues;
    this.metastoreJson = metastoreJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
    this.batchSize = batchSize;
    this.flushIntervalMs = flushIntervalMs;
    this.transformName = transformName;
    this.transformPluginId = transformPluginId;
    this.transformMetaInterfaceXml = transformMetaInterfaceXml;
    this.inputRowMetaJson = inputRowMetaJson;
    this.inputTransform = inputTransform;
    this.targetTransforms = targetTransforms;
    this.infoTransforms = infoTransforms;
    this.infoRowMetaJsons = infoRowMetaJsons;
    this.infoCollectionViews = infoCollectionViews;
    this.runConfigName = runConfigName;
    this.dataSamplersJson = dataSamplersJson;
    this.parentLogChannelId = parentLogChannelId;
  }

  @Override
  public PCollectionTuple expand(PCollection<HopRow> input) {
    try {
      // Only initialize once on this node/vm
      //
      BeamHop.init(transformPluginClasses, xpPluginClasses);

      // Similar for the output : treat a TupleTag list for the target transforms...
      //
      TupleTag<HopRow> mainOutputTupleTag =
          new TupleTag<>(HopBeamUtil.createMainOutputTupleId(transformName)) {};
      List<TupleTag<HopRow>> targetTupleTags = new ArrayList<>();
      TupleTagList targetTupleTagList = null;
      for (String targetTransform : targetTransforms) {
        String tupleId = HopBeamUtil.createTargetTupleId(transformName, targetTransform);
        TupleTag<HopRow> tupleTag = new TupleTag<HopRow>(tupleId) {};
        targetTupleTags.add(tupleTag);
        if (targetTupleTagList == null) {
          targetTupleTagList = TupleTagList.of(tupleTag);
        } else {
          targetTupleTagList = targetTupleTagList.and(tupleTag);
        }
      }
      if (targetTupleTagList == null) {
        targetTupleTagList = TupleTagList.empty();
      }

      // Create a new transform function, initializes the transform
      //
      TransformFn transformFn =
          new TransformFn(
              variableValues,
              metastoreJson,
              transformPluginClasses,
              xpPluginClasses,
              transformName,
              transformPluginId,
              transformMetaInterfaceXml,
              inputRowMetaJson,
              inputTransform,
              targetTransforms,
              infoTransforms,
              infoRowMetaJsons,
              dataSamplersJson,
              runConfigName,
              parentLogChannelId);

      // The actual transform functionality
      //
      ParDo.SingleOutput<HopRow, HopRow> parDoTransformFn = ParDo.of(transformFn);

      // Add optional side inputs...
      //
      if (infoCollectionViews.size() > 0) {
        parDoTransformFn = parDoTransformFn.withSideInputs(infoCollectionViews);
      }

      // Specify the main output and targeted outputs
      //
      ParDo.MultiOutput<HopRow, HopRow> multiOutput =
          parDoTransformFn.withOutputTags(mainOutputTupleTag, targetTupleTagList);

      // Apply the multi output parallel do transform function to the main input stream
      //
      PCollectionTuple collectionTuple = input.apply(multiOutput);

      // In the tuple is everything we need to find.
      // Just make sure to retrieve the PCollections using the correct Tuple ID
      // Use HopBeamUtil.createTargetTupleId()... to make sure
      //
      return collectionTuple;
    } catch (Exception e) {
      numErrors.inc();
      LOG.error("Error transforming data in transform '" + transformName + "'", e);
      throw new RuntimeException("Error transforming data in transform", e);
    }
  }

  private class TransformFn extends TransformBaseFn {

    private static final long serialVersionUID = 95700000000000001L;

    public static final String INJECTOR_TRANSFORM_NAME = "_INJECTOR_";

    protected List<VariableValue> variableValues;
    protected String metastoreJson;
    protected List<String> transformPluginClasses;
    protected List<String> xpPluginClasses;
    protected String transformName;
    protected String transformPluginId;
    protected String transformMetaInterfaceXml;
    protected String inputRowMetaJson;
    protected String dataSamplersJson;
    protected List<String> targetTransforms;
    protected List<String> infoTransforms;
    protected List<String> infoRowMetaJsons;
    protected boolean inputTransform;
    protected boolean initialize;

    protected List<PCollection<HopRow>> infoCollections;

    // Log and count parse errors.
    private final Counter numErrors = Metrics.counter("main", "TransformProcessErrors");

    private transient PipelineMeta pipelineMeta;
    private transient TransformMeta transformMeta;
    private transient IRowMeta inputRowMeta;
    private transient List<TransformMetaDataCombi> transformCombis;
    private transient LocalPipelineEngine pipeline;
    private transient RowProducer rowProducer;
    private transient List<Object[]> resultRows;
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
        List<String> transformPluginClasses,
        List<String> xpPluginClasses,
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
        String parentLogChannelId) {
      super(parentLogChannelId, runConfigName, dataSamplersJson);
      this.variableValues = variableValues;
      this.metastoreJson = metastoreJson;
      this.transformPluginClasses = transformPluginClasses;
      this.xpPluginClasses = xpPluginClasses;
      this.transformName = transformName;
      this.transformPluginId = transformPluginId;
      this.transformMetaInterfaceXml = transformMetaInterfaceXml;
      this.inputRowMetaJson = inputRowMetaJson;
      this.inputTransform = inputTransform;
      this.targetTransforms = targetTransforms;
      this.infoTransforms = infoTransforms;
      this.infoRowMetaJsons = infoRowMetaJsons;
      this.dataSamplersJson = dataSamplersJson;
      this.initialize = true;
    }

    /**
     * Reset the row buffer every time we start a new bundle to prevent the output of double rows
     *
     * @param startBundleContext
     */
    @StartBundle
    public void startBundle(StartBundleContext startBundleContext) {
      Metrics.counter("startBundle", transformName).inc();
      if ("ScriptValueMod".equals(transformPluginId) && pipeline != null) {
        initialize = true;
      }
    }

    @Setup
    public void setup() {
      // Nothing
    }

    @Teardown
    public void tearDown() {
      try {
        if (executor != null) {
          executor.dispose();

          // Send last data from the data samplers over to the location (if any)
          //
          if (executionInfoLocation != null) {
            if (executionInfoTimer != null) {
              executionInfoTimer.cancel();
            }
            sendSamplesToLocation();
          }
        }
      } catch (Exception e) {
        LOG.error(
            "Error cleaning up single threaded pipeline executor in Beam transform "
                + transformName,
            e);
        throw new RuntimeException(
            "Error cleaning up single threaded pipeline executor in Beam transform "
                + transformName,
            e);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window) {

      try {
        if (initialize) {
          initialize = false;

          // Initialize Hop and load extra plugins as well
          //
          BeamHop.init(transformPluginClasses, xpPluginClasses);

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
                createInjectorTransform(
                    pipelineMeta, INJECTOR_TRANSFORM_NAME, inputRowMeta, 200, 200);
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
                createInjectorTransform(
                    pipelineMeta, infoTransform, infoRowMeta, 200, 350 + 150 * i);
            infoTransformMetas.add(infoTransformMeta);
          }

          transformCombis = new ArrayList<>();

          // The main transform inflated from XML metadata...
          //
          PluginRegistry registry = PluginRegistry.getInstance();
          ITransformMeta iTransformMeta =
              registry.loadClass(
                  TransformPluginType.class, transformPluginId, ITransformMeta.class);
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
            pipelineMeta.addPipelineHop(
                new PipelineHopMeta(mainInjectorTransformMeta, transformMeta));
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
          lookupExecutionInformation(metadataProvider);

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
          pipeline
              .getPipelineRunConfiguration()
              .setName("beam-transform-local (" + transformName + ")");

          pipeline.prepareExecution();

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
                    resultRows.add(row);
                  }
                };
            transformCombi.transform.addRowListener(rowListener);
          }

          // Create a list of TupleTag to direct the target rows
          //
          mainTupleTag =
              new TupleTag<HopRow>(HopBeamUtil.createMainOutputTupleId(transformName)) {};
          tupleTagList = new ArrayList<>();

          // The lists in here will contain all the rows that ended up in the various target
          // transforms (if any)
          //
          targetResultRowsList = new ArrayList<>();

          for (String targetTransform : targetTransforms) {
            TransformMetaDataCombi targetCombi = findCombi(pipeline, targetTransform);
            transformCombis.add(targetCombi);

            String tupleId = HopBeamUtil.createTargetTupleId(transformName, targetTransform);
            TupleTag<HopRow> tupleTag = new TupleTag<HopRow>(tupleId) {};
            tupleTagList.add(tupleTag);
            final List<Object[]> targetResultRows = new ArrayList<>();
            targetResultRowsList.add(targetResultRows);

            targetCombi.transform.addRowListener(
                new RowAdapter() {
                  @Override
                  public void rowReadEvent(IRowMeta rowMeta, Object[] row)
                      throws HopTransformException {
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

          executor = new SingleThreadedPipelineExecutor(pipeline);

          // Initialize the transforms...
          //
          executor.init();

          Counter initCounter = Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName);
          readCounter = Metrics.counter(Pipeline.METRIC_NAME_READ, transformName);
          writtenCounter = Metrics.counter(Pipeline.METRIC_NAME_WRITTEN, transformName);

          initCounter.inc();

          pipeline.setLogLevel(LogLevel.NOTHING);

          // Doesn't really start the threads in single threaded mode
          // Just sets some flags all over the place
          //
          pipeline.startThreads();

          pipeline.setLogLevel(LogLevel.BASIC);

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
        }

        // Get one row from the context main input and make a copy so we can change it.
        //
        HopRow originalInputRow = context.element();
        HopRow inputRow = HopBeamUtil.copyHopRow(originalInputRow, inputRowMeta);
        readCounter.inc();

        emptyRowBuffer(new TransformProcessContext(context), inputRow);
      } catch (Exception e) {
        numErrors.inc();
        LOG.info("Transform execution error :" + e.getMessage());
        throw new RuntimeException("Error executing TransformFn", e);
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
      try {
        // Nothing to do here
      } catch (Exception e) {
        numErrors.inc();
        LOG.info("Transform finishing bundle error :" + e.getMessage());
        throw new RuntimeException(
            "Error finalizing bundle of transform '" + transformName + "'", e);
      }
    }

    private transient int maxInputBufferSize = 0;
    private transient int minInputBufferSize = Integer.MAX_VALUE;

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
        rowProducer.putRow(inputRowMeta, inputRow.getRow());
      }

      // Execute all transforms in the transformation
      //
      executor.oneIteration();

      // Evaluate the results...
      //

      // Pass all rows in the output to the process context
      //
      for (Object[] resultRow : resultRows) {

        // Pass the row to the process context
        //
        context.output(mainTupleTag, new HopRow(resultRow));
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
  }

  private interface TupleOutputContext<T> {
    void output(TupleTag<T> tupleTag, T output);
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
}
