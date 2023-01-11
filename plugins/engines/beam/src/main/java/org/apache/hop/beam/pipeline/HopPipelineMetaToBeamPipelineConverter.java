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

package org.apache.hop.beam.pipeline;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.core.util.HopBeamUtil;
import org.apache.hop.beam.engines.HopPipelineExecutionOptions;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.engines.dataflow.BeamDataFlowPipelineRunConfiguration;
import org.apache.hop.beam.metadata.RunnerType;
import org.apache.hop.beam.pipeline.handler.BeamGenericTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamMergeJoinTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamRowGeneratorTransformHandler;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.groupby.GroupByMeta;
import org.apache.hop.pipeline.transforms.uniquerows.UniqueRowsMeta;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.IndexView;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HopPipelineMetaToBeamPipelineConverter {

  protected final String runConfigName;
  protected final PipelineRunConfiguration runConfiguration;

  protected IVariables variables;
  protected PipelineMeta pipelineMeta;
  protected SerializableMetadataProvider metadataProvider;
  protected String metaStoreJson;
  protected Map<String, IBeamPipelineTransformHandler> transformHandlers;
  protected IBeamPipelineTransformHandler genericTransformHandler;
  protected IBeamPipelineEngineRunConfiguration pipelineRunConfiguration;
  protected final String dataSamplersJson;
  protected final String parentLogChannelId;
  protected PipelineOptions pipelineOptions;

  public HopPipelineMetaToBeamPipelineConverter(
      IVariables variables,
      PipelineMeta pipelineMeta,
      IHopMetadataProvider metadataProvider,
      String runConfigName,
      List<IExecutionDataSampler<? extends IExecutionDataSamplerStore>> dataSamplers,
      String parentLogChannelId)
      throws HopException {
    this.transformHandlers = new HashMap<>();

    // Serialize the data samplers to JSON
    this.dataSamplersJson = serializeDataSamplers(dataSamplers);
    this.parentLogChannelId = parentLogChannelId;

    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.metadataProvider = new SerializableMetadataProvider(metadataProvider);
    this.metaStoreJson = this.metadataProvider.toJson();
    this.runConfigName = runConfigName;

    this.runConfiguration =
        metadataProvider.getSerializer(PipelineRunConfiguration.class).load(runConfigName);

    if (!(runConfiguration.getEngineRunConfiguration()
        instanceof IBeamPipelineEngineRunConfiguration)) {
      throw new HopException("You need to provide a Beam run configuration");
    }
    this.pipelineRunConfiguration =
        (IBeamPipelineEngineRunConfiguration) runConfiguration.getEngineRunConfiguration();
    this.pipelineOptions = pipelineRunConfiguration.getPipelineOptions();

    try {
      setAdditionalPipelineOption();
    } catch (Exception e) {
      throw new HopException("Could not set Additional pipeline options");
    }

    addDefaultTransformHandlers();
  }

  /**
   * Constructor used when options are provided via DataflowTemplate
   *
   * @param variables
   * @param pipelineMeta
   * @param metadataProvider
   * @param pipelineOptions
   * @param dataSamplers
   * @param parentLogChannelId
   * @throws HopException
   */
  public HopPipelineMetaToBeamPipelineConverter(
      IVariables variables,
      PipelineMeta pipelineMeta,
      IHopMetadataProvider metadataProvider,
      PipelineOptions pipelineOptions,
      List<IExecutionDataSampler<? extends IExecutionDataSamplerStore>> dataSamplers,
      String parentLogChannelId)
      throws HopException {
    this.transformHandlers = new HashMap<>();

    // Serialize the data samplers to JSON
    this.dataSamplersJson = serializeDataSamplers(dataSamplers);
    this.parentLogChannelId = parentLogChannelId;
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.metadataProvider = new SerializableMetadataProvider(metadataProvider);
    this.metaStoreJson = this.metadataProvider.toJson();
    this.runConfigName = "DataflowTemplate";

    // Create an empty default PipelineRunConfiguration for sane defaults
    BeamDataFlowPipelineRunConfiguration beamDataFlowPipelineRunConfiguration =
        new BeamDataFlowPipelineRunConfiguration();
    beamDataFlowPipelineRunConfiguration.setEnginePluginId("BeamDataFlowPipelineEngine");

    PipelineRunConfiguration newRunConfiguration =
        new PipelineRunConfiguration(
            "DataflowTemplate",
            "description",
            "",
            null,
            beamDataFlowPipelineRunConfiguration,
            null,
            false);

    // store temp run configuration
    metadataProvider.getSerializer(PipelineRunConfiguration.class).save(newRunConfiguration);

    this.runConfiguration = newRunConfiguration;
    this.pipelineRunConfiguration =
        (IBeamPipelineEngineRunConfiguration) newRunConfiguration.getEngineRunConfiguration();

    // Copy the provided pipelineOptions form the DataflowTemplate
    this.pipelineOptions = pipelineOptions;

    addDefaultTransformHandlers();
  }

  private String serializeDataSamplers(
      List<IExecutionDataSampler<? extends IExecutionDataSamplerStore>> dataSamplers)
      throws HopException {
    try {
      return HopJson.newMapper().writeValueAsString(dataSamplers);
    } catch (Exception e) {
      throw new HopException("Error serializing data samplers to JSON", e);
    }
  }

  protected List<String> splitPluginClasses(String transformPluginClasses) {
    List<String> list = new ArrayList<>();
    if (StringUtils.isNotEmpty(transformPluginClasses)) {
      list.addAll(Arrays.asList(transformPluginClasses.split(",")));
    }
    return list;
  }

  public void addDefaultTransformHandlers() {
    // Add the transform handlers for the special cases, functionality which Beams handles
    // specifically
    //
    transformHandlers.put(
        BeamConst.STRING_MERGE_JOIN_PLUGIN_ID, new BeamMergeJoinTransformHandler());
    transformHandlers.put(
        BeamConst.STRING_BEAM_ROW_GENERATOR_PLUGIN_ID, new BeamRowGeneratorTransformHandler());
    genericTransformHandler = new BeamGenericTransformHandler();
  }

  public static List<String> findAnnotatedClasses(String folder, String annotationClassName)
      throws HopException {
    JarCache jarCache = JarCache.getInstance();
    List<String> classNames = new ArrayList<>();

    // Scan only jar files with @Transform and @ExtensionPointPlugin annotations
    //
    File pluginFolder = new File("plugins/" + folder);

    try {
      // Get all the jar files in the plugin folder...
      //
      Set<File> files = jarCache.findJarFiles(pluginFolder);
      if (!files.isEmpty())
        for (File file : files) {

          // These are the jar files : find annotations in it...
          //
          IndexView index = jarCache.getIndex(file);

          // find annotations annotated with this meta-annotation
          for (AnnotationInstance instance : index.getAnnotations(annotationClassName)) {
            if (instance.target() instanceof ClassInfo) {
              ClassInfo classInfo = instance.target().asClass();
              classNames.add(classInfo.name().toString());
            }
          }
        }
      else {
        System.out.println("No jar files found in plugin folder " + pluginFolder);
      }
    } catch (Exception e) {
      throw new HopException("Unable to find annotated classes of class " + annotationClassName, e);
    }

    return classNames;
  }

  public void setAdditionalPipelineOption() throws Exception {
    // Create a new Pipeline
    //
    RunnerType runnerType = pipelineRunConfiguration.getRunnerType();
    Class<? extends PipelineRunner<?>> runnerClass = getPipelineRunnerClass(runnerType);
    // The generic options
    //
    pipelineOptions.setUserAgent(variables.resolve(pipelineRunConfiguration.getUserAgent()));
    pipelineOptions.setTempLocation(variables.resolve(pipelineRunConfiguration.getTempLocation()));

    pipelineOptions.setJobName(sanitizeJobName(pipelineMeta.getName()));

    pipelineOptions
        .as(HopPipelineExecutionOptions.class)
        .setLogLevel(
            LogLevel.getLogLevelForCode(
                Const.NVL(
                    pipelineRunConfiguration.getVariable(
                        BeamConst.STRING_LOCAL_PIPELINE_FLAG_LOG_LEVEL),
                    "MINIMAL")));

    pipelineOptions.setRunner(runnerClass);
  }

  public Pipeline createPipeline() throws Exception {
    try {
      ILogChannel log = LogChannel.GENERAL;

      Pipeline pipeline = Pipeline.create(pipelineOptions);

      pipeline.getCoderRegistry().registerCoderForClass(HopRow.class, new HopRowCoder());

      log.logBasic("Created Apache Beam pipeline with name '" + pipelineOptions.getJobName() + "'");

      // Keep track of which transform outputs which Collection
      //
      Map<String, PCollection<HopRow>> transformCollectionMap = new HashMap<>();

      // Handle input
      //
      handleBeamInputTransforms(log, transformCollectionMap, pipeline);

      // Transform all the other transforms...
      //
      handleGenericTransform(transformCollectionMap, pipeline);

      // Output handling
      //
      handleBeamOutputTransforms(log, transformCollectionMap, pipeline);

      return pipeline;
    } catch (Throwable e) {
      e.printStackTrace();
      throw new Exception("Error converting Hop pipeline to Beam", e);
    }
  }

  /**
   * Clean up the name for Dataflow and others...
   *
   * @param name The name of the job to sanitize for Beam
   * @return The sanitezed name without spaces and other special characters
   */
  private String sanitizeJobName(String name) {
    String newName = name.toLowerCase();
    if (name.matches("^[0-9].*")) {
      newName = "hop-" + newName;
    }
    StringBuilder builder = new StringBuilder(newName);
    for (int i = 0; i < builder.length(); i++) {
      String c = "" + builder.charAt(i);
      if (!c.matches("[-0-9a-z]")) {
        builder.setCharAt(i, '-');
      }
    }
    return builder.toString();
  }

  public static Class<? extends PipelineRunner<?>> getPipelineRunnerClass(RunnerType runnerType)
      throws HopException {
    if (runnerType == null) {
      throw new HopException("Please specify a valid runner type");
    }
    switch (runnerType) {
      case Direct:
        return DirectRunner.class;
      case Flink:
        return FlinkRunner.class;
      case Spark:
        return SparkRunner.class;
      case DataFlow:
        return DataflowRunner.class;
      default:
        throw new HopException("Unsupported runner type: " + runnerType.name());
    }
  }

  private void handleBeamInputTransforms(
      ILogChannel log, Map<String, PCollection<HopRow>> transformCollectionMap, Pipeline pipeline)
      throws HopException {

    List<TransformMeta> beamInputTransformMetas = findBeamInputs();
    for (TransformMeta transformMeta : beamInputTransformMetas) {
      IBeamPipelineTransformHandler transformHandler =
          transformHandlers.get(transformMeta.getTransformPluginId());
      if (transformHandler == null) {
        // See if the transform knows how to handle a Beam pipeline...
        //
        if (transformMeta.getTransform() instanceof IBeamPipelineTransformHandler) {
          transformHandler = (IBeamPipelineTransformHandler) transformMeta.getTransform();
        }
      }
      if (transformHandler == null) {
        throw new HopException(
            "Unable to find Beam pipeline transform handler for transform: "
                + transformMeta.getName());
      }

      transformHandler.handleTransform(
          log,
          variables,
          runConfigName,
          pipelineRunConfiguration,
          dataSamplersJson,
          metadataProvider,
          pipelineMeta,
          transformMeta,
          transformCollectionMap,
          pipeline,
          pipelineMeta.getTransformFields(variables, transformMeta),
          null,
          null,
          parentLogChannelId);
    }
  }

  private void handleBeamOutputTransforms(
      ILogChannel log, Map<String, PCollection<HopRow>> transformCollectionMap, Pipeline pipeline)
      throws HopException {
    List<TransformMeta> beamOutputTransformMetas = findBeamOutputs();
    for (TransformMeta transformMeta : beamOutputTransformMetas) {
      IBeamPipelineTransformHandler transformHandler =
          transformHandlers.get(transformMeta.getTransformPluginId());
      if (transformHandler == null) {
        // See if the transform knows how to handle a Beam pipeline...
        //
        if (transformMeta.getTransform() instanceof IBeamPipelineTransformHandler) {
          transformHandler = (IBeamPipelineTransformHandler) transformMeta.getTransform();
        }
      }
      if (transformHandler == null) {
        throw new HopException(
            "Unable to find Beam pipeline transform handler for transform: "
                + transformMeta.getName());
      }

      List<TransformMeta> previousTransforms =
          pipelineMeta.findPreviousTransforms(transformMeta, false);
      if (previousTransforms.size() > 1) {
        throw new HopException("Combining data from multiple transforms is not supported yet!");
      }
      TransformMeta previousTransform = previousTransforms.get(0);

      // See if this output transform isn't targeted specifically by the previous transform.
      //
      // Check in the map to see if previousTransform isn't targeting this one
      //
      String targetName =
          HopBeamUtil.createTargetTupleId(previousTransform.getName(), transformMeta.getName());
      PCollection<HopRow> input = transformCollectionMap.get(targetName);
      if (input == null) {
        input = transformCollectionMap.get(previousTransform.getName());
        if (input == null) {
          throw new HopException(
              "Previous PCollection for transform "
                  + previousTransform.getName()
                  + " could not be found");
        }
      } else {
        log.logBasic(
            "Transform "
                + transformMeta.getName()
                + " reading from previous transform targeting this one using : "
                + targetName);
      }

      // What fields are we getting from the previous transform(s)?
      //
      IRowMeta rowMeta = pipelineMeta.getTransformFields(variables, previousTransform);

      transformHandler.handleTransform(
          log,
          variables,
          runConfigName,
          pipelineRunConfiguration,
          dataSamplersJson,
          metadataProvider,
          pipelineMeta,
          transformMeta,
          transformCollectionMap,
          pipeline,
          rowMeta,
          previousTransforms,
          input,
          parentLogChannelId);
    }
  }

  private void handleGenericTransform(
      Map<String, PCollection<HopRow>> transformCollectionMap, Pipeline pipeline)
      throws HopException {

    ILogChannel log = LogChannel.GENERAL;

    // Perform topological sort
    //
    List<TransformMeta> transforms = getSortedTransformsList();

    for (TransformMeta transformMeta : transforms) {

      // Input and output transforms are handled else where.
      //
      IBeamPipelineTransformHandler transformHandler =
          transformHandlers.get(transformMeta.getTransformPluginId());
      if (transformHandler == null) {
        if (transformMeta.getTransform() instanceof IBeamPipelineTransformHandler) {
          // This metadata knows about Beam...
          //
          transformHandler = (IBeamPipelineTransformHandler) transformMeta.getTransform();
        }
      }

      if (transformHandler == null
          || (!transformHandler.isInput() && !transformHandler.isOutput())) {

        // Generic transform
        //
        validateTransformBeamUsage(transformMeta.getTransform());

        // Lookup all the previous transforms for this one, excluding info transforms like
        // StreamLookup...
        // So the usecase is : we read from multiple io transforms and join to one location...
        //
        List<TransformMeta> previousTransforms =
            pipelineMeta.findPreviousTransforms(transformMeta, false);

        TransformMeta firstPreviousTransform;
        IRowMeta rowMeta;
        PCollection<HopRow> input = null;

        // Transforms like Merge Join or Merge have no io, only info transforms reaching in
        //
        if (previousTransforms.isEmpty()) {
          rowMeta = new RowMeta();
        } else {

          // Lookup the previous collection to apply this transforms transform to.
          // We can take any of the inputs so the first one will do.
          //
          firstPreviousTransform = previousTransforms.get(0);

          // No fuss with info fields sneaking in, all previous transforms need to emit the same
          // layout anyway
          //
          rowMeta = pipelineMeta.getTransformFields(variables, firstPreviousTransform);

          // Check in the map to see if previousTransform isn't targeting this one
          //
          String targetName =
              HopBeamUtil.createTargetTupleId(
                  firstPreviousTransform.getName(), transformMeta.getName());
          input = transformCollectionMap.get(targetName);
          if (input == null) {
            input = transformCollectionMap.get(firstPreviousTransform.getName());
          } else {
            log.logBasic(
                "Transform "
                    + transformMeta.getName()
                    + " reading from previous transform targeting this one using : "
                    + targetName);
          }

          // If there are multiple io streams into this transform, flatten all the data sources by
          // default
          // This means to simply merge the data.
          //
          if (previousTransforms.size() > 1) {
            List<PCollection<HopRow>> extraInputs = new ArrayList<>();
            for (int i = 1; i < previousTransforms.size(); i++) {
              TransformMeta previousTransform = previousTransforms.get(i);
              PCollection<HopRow> previousPCollection;
              targetName =
                  HopBeamUtil.createTargetTupleId(
                      previousTransform.getName(), transformMeta.getName());
              previousPCollection = transformCollectionMap.get(targetName);
              if (previousPCollection == null) {
                previousPCollection = transformCollectionMap.get(previousTransform.getName());
              } else {
                log.logBasic(
                    "Transform "
                        + transformMeta.getName()
                        + " reading from previous transform targetting this one using : "
                        + targetName);
              }
              if (previousPCollection == null) {
                throw new HopException(
                    "Previous collection was not found for transform "
                        + previousTransform.getName()
                        + ", a previous transform to "
                        + transformMeta.getName());
              } else {
                extraInputs.add(previousPCollection);
              }
            }

            // Flatten the extra inputs...
            //
            PCollectionList<HopRow> inputList = PCollectionList.of(input);

            for (PCollection<HopRow> extraInput : extraInputs) {
              inputList = inputList.and(extraInput);
            }

            // Flatten all the collections.  It's business as usual behind this.
            //
            input = inputList.apply(transformMeta.getName() + " Flatten", Flatten.pCollections());
          }
        }

        if (transformHandler == null) {
          transformHandler = genericTransformHandler;
        }

        transformHandler.handleTransform(
            log,
            variables,
            runConfigName,
            pipelineRunConfiguration,
            dataSamplersJson,
            metadataProvider,
            pipelineMeta,
            transformMeta,
            transformCollectionMap,
            pipeline,
            rowMeta,
            previousTransforms,
            input,
            parentLogChannelId);
      }
    }
  }

  private void validateTransformBeamUsage(ITransformMeta meta) throws HopException {
    if (meta instanceof GroupByMeta) {
      throw new HopException(
          "Group By is not supported.  Use the Memory Group By transform instead.  It comes closest to Beam functionality.");
    }
    //    if (meta instanceof SortRowsMeta) {
    //      throw new HopException("Sort rows is not yet supported on Beam.");
    //    }
    if (meta instanceof UniqueRowsMeta) {
      throw new HopException(
          "The unique rows transform is not yet supported on Beam, for now use a Memory Group By to get distrinct rows");
    }
  }

  /** Find the Beam Input transforms, return them */
  private List<TransformMeta> findBeamInputs() {
    List<TransformMeta> transforms = new ArrayList<>();
    for (TransformMeta transformMeta : pipelineMeta.getPipelineHopTransforms(false)) {

      IBeamPipelineTransformHandler transformHandler =
          transformHandlers.get(transformMeta.getTransformPluginId());
      if (transformHandler != null && transformHandler.isInput()) {
        transforms.add(transformMeta);
      } else {
        if (transformMeta.getTransform() instanceof IBeamPipelineTransformHandler) {
          if (((IBeamPipelineTransformHandler) transformMeta.getTransform()).isInput()) {
            transforms.add(transformMeta);
          }
        }
      }
    }
    return transforms;
  }

  private List<TransformMeta> findBeamOutputs() {
    List<TransformMeta> transforms = new ArrayList<>();
    for (TransformMeta transformMeta : pipelineMeta.getPipelineHopTransforms(false)) {
      IBeamPipelineTransformHandler transformHandler =
          transformHandlers.get(transformMeta.getTransformPluginId());
      if (transformHandler != null && transformHandler.isOutput()) {
        transforms.add(transformMeta);
      } else {
        if (transformMeta.getTransform() instanceof IBeamPipelineTransformHandler) {
          if (((IBeamPipelineTransformHandler) transformMeta.getTransform()).isOutput()) {
            transforms.add(transformMeta);
          }
        }
      }
    }
    return transforms;
  }

  /** Sort the transforms from start to finish... */
  private List<TransformMeta> getSortedTransformsList() {

    // Create a copy of the transforms
    //
    List<TransformMeta> transforms = new ArrayList<>(pipelineMeta.getPipelineHopTransforms(false));

    // The bubble sort algorithm in contrast to the QuickSort or MergeSort
    // algorithms
    // does indeed cover all possibilities.
    // Sorting larger transformations with hundreds of transforms might be too slow
    // though.
    // We should consider caching PipelineMeta.findPrevious() results in that case.
    //
    pipelineMeta.clearCaches();

    //
    // Cocktail sort (bi-directional bubble sort)
    //
    // Original sort was taking 3ms for 30 transforms
    // cocktail sort takes about 8ms for the same 30, but it works :)

    // set these to true if you are working on this algorithm and don't like
    // flying blind.
    //

    int transformsMinSize = 0;
    int transformsSize = transforms.size();

    // Noticed a problem with an immediate shrinking iteration window
    // trapping rows that need to be sorted.
    // This threshold buys us some time to get the sorting close before
    // starting to decrease the window size.
    //
    // TODO: this could become much smarter by tracking row movement
    // and reacting to that each outer iteration verses
    // using a threshold.
    //
    // After this many iterations enable trimming inner iteration
    // window on no change being detected.
    //
    int windowShrinkThreshold = (int) Math.round(transformsSize * 0.75);

    // give ourselves some room to sort big lists. the window threshold should
    // stop us before reaching this anyway.
    //
    int totalIterations = transformsSize * 2;

    boolean isBefore;
    boolean forwardChange = false;
    boolean backwardChange = false;

    boolean lastForwardChange = true;
    boolean keepSortingForward = true;

    TransformMeta one;
    TransformMeta two;

    long startTime = System.currentTimeMillis();

    for (int x = 0; x < totalIterations; x++) {

      // Go forward through the list
      //
      if (keepSortingForward) {
        for (int y = transformsMinSize; y < transformsSize - 1; y++) {
          one = transforms.get(y);
          two = transforms.get(y + 1);
          isBefore = pipelineMeta.findPrevious(one, two);
          if (isBefore) {
            // two was found to be positioned BEFORE one so we need to
            // switch them...
            //
            transforms.set(y, two);
            transforms.set(y + 1, one);
            forwardChange = true;
          }
        }
      }

      // Go backward through the list
      //
      for (int z = transformsSize - 1; z > transformsMinSize; z--) {
        one = transforms.get(z);
        two = transforms.get(z - 1);

        isBefore = pipelineMeta.findPrevious(one, two);
        if (!isBefore) {
          // two was found NOT to be positioned BEFORE one so we need to
          // switch them...
          //
          transforms.set(z, two);
          transforms.set(z - 1, one);
          backwardChange = true;
        }
      }

      // Shrink transformsSize(max) if there was no forward change
      //
      if (x > windowShrinkThreshold && !forwardChange) {

        // should we keep going? check the window size
        //
        transformsSize--;
        if (transformsSize <= transformsMinSize) {
          break;
        }
      }

      // shrink transformsMinSize(min) if there was no backward change
      //
      if (x > windowShrinkThreshold && !backwardChange) {

        // should we keep going? check the window size
        //
        transformsMinSize++;
        if (transformsMinSize >= transformsSize) {
          break;
        }
      }

      // End of both forward and backward traversal.
      // Time to see if we should keep going.
      //
      if (!forwardChange && !backwardChange) {
        break;
      }

      //
      // if we are past the first iteration and there has been no change twice,
      // quit doing it!
      //
      if (keepSortingForward && x > 0 && !lastForwardChange && !forwardChange) {
        keepSortingForward = false;
      }
      lastForwardChange = forwardChange;
      forwardChange = false;
      backwardChange = false;
    } // finished sorting

    return transforms;
  }

  /**
   * Gets transformHandlers
   *
   * @return value of transformHandlers
   */
  public Map<String, IBeamPipelineTransformHandler> getTransformHandlers() {
    return transformHandlers;
  }

  /**
   * @param transformHandlers The transformHandlers to set
   */
  public void setTransformHandlers(Map<String, IBeamPipelineTransformHandler> transformHandlers) {
    this.transformHandlers = transformHandlers;
  }

  /**
   * Gets genericTransformHandler
   *
   * @return value of genericTransformHandler
   */
  public IBeamPipelineTransformHandler getGenericTransformHandler() {
    return genericTransformHandler;
  }

  /**
   * @param genericTransformHandler The genericTransformHandler to set
   */
  public void setGenericTransformHandler(IBeamPipelineTransformHandler genericTransformHandler) {
    this.genericTransformHandler = genericTransformHandler;
  }
}
