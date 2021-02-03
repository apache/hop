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
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.metadata.RunnerType;
import org.apache.hop.beam.pipeline.handler.BeamBigQueryInputTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamBigQueryOutputTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamGenericTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamGroupByTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamInputTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamKafkaInputTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamKafkaOutputTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamMergeJoinTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamOutputTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamPublisherTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamRowGeneratorTransformHandler;
import org.apache.hop.beam.pipeline.handler.IBeamTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamSubscriberTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamTimestampTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamWindowTransformHandler;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.groupby.GroupByMeta;
import org.apache.hop.pipeline.transforms.sort.SortRowsMeta;
import org.apache.hop.pipeline.transforms.uniquerows.UniqueRowsMeta;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HopPipelineMetaToBeamPipelineConverter<T extends IBeamPipelineEngineRunConfiguration> {

  protected IVariables variables;
  protected PipelineMeta pipelineMeta;
  protected SerializableMetadataProvider metadataProvider;
  protected String metaStoreJson;
  protected List<String> transformPluginClasses;
  protected List<String> xpPluginClasses;
  protected Map<String, IBeamTransformHandler> transformHandlers;
  protected IBeamTransformHandler genericTransformHandler;
  protected T pipelineRunConfiguration;

  public HopPipelineMetaToBeamPipelineConverter() {
    this.transformHandlers = new HashMap<>();
    this.transformPluginClasses = new ArrayList<>();
    this.xpPluginClasses = new ArrayList<>();
  }

  public HopPipelineMetaToBeamPipelineConverter( IVariables variables, PipelineMeta pipelineMeta, IHopMetadataProvider metadataProvider, T pipelineRunConfiguration ) throws HopException, HopException {
    this();
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.metadataProvider = new SerializableMetadataProvider( metadataProvider );
    this.metaStoreJson = this.metadataProvider.toJson();
    this.pipelineRunConfiguration = pipelineRunConfiguration;

    addClassesFromPluginsToStage( pipelineRunConfiguration.getPluginsToStage() );
    this.transformPluginClasses.addAll(splitPluginClasses(pipelineRunConfiguration.getTransformPluginClasses()));
    this.xpPluginClasses.addAll(splitPluginClasses(pipelineRunConfiguration.getXpPluginClasses()));

    addDefaultTransformHandlers();
  }

  protected List<String> splitPluginClasses( String transformPluginClasses ) {
    List<String> list = new ArrayList<>();
    if (StringUtils.isNotEmpty( transformPluginClasses )) {
      list.addAll( Arrays.asList( transformPluginClasses.split( "," ) ) );
    }
    return list;
  }

  public void addClassesFromPluginsToStage( String pluginsToStage ) throws HopException {
    // Find the plugins in the jar files in the plugin folders to stage...
    //
    if ( StringUtils.isNotEmpty( pluginsToStage ) ) {
      String[] pluginFolders = pluginsToStage.split( "," );
      // Scan only jar files with @Transform and @ExtensionPointPlugin annotations
      for ( String pluginFolder : pluginFolders ) {
        List<String> transformClasses = findAnnotatedClasses( pluginFolder, Transform.class.getName() );
        transformPluginClasses.addAll( transformClasses );
        List<String> xpClasses = findAnnotatedClasses( pluginFolder, ExtensionPoint.class.getName() );
        xpPluginClasses.addAll( xpClasses );
      }
    }
  }

  public void addDefaultTransformHandlers() throws HopException {
    // Add the transform handlers for the special cases, functionality which Beams handles specifically
    //
    transformHandlers.put( BeamConst.STRING_BEAM_INPUT_PLUGIN_ID, new BeamInputTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    transformHandlers.put( BeamConst.STRING_BEAM_OUTPUT_PLUGIN_ID, new BeamOutputTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    transformHandlers.put( BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID, new BeamPublisherTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    transformHandlers.put( BeamConst.STRING_BEAM_SUBSCRIBE_PLUGIN_ID, new BeamSubscriberTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    transformHandlers.put( BeamConst.STRING_MERGE_JOIN_PLUGIN_ID, new BeamMergeJoinTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    transformHandlers.put( BeamConst.STRING_MEMORY_GROUP_BY_PLUGIN_ID, new BeamGroupByTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    transformHandlers.put( BeamConst.STRING_BEAM_WINDOW_PLUGIN_ID, new BeamWindowTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    transformHandlers.put( BeamConst.STRING_BEAM_TIMESTAMP_PLUGIN_ID, new BeamTimestampTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    transformHandlers.put( BeamConst.STRING_BEAM_BIGQUERY_INPUT_PLUGIN_ID, new BeamBigQueryInputTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    transformHandlers.put( BeamConst.STRING_BEAM_BIGQUERY_OUTPUT_PLUGIN_ID, new BeamBigQueryOutputTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    transformHandlers.put( BeamConst.STRING_BEAM_KAFKA_CONSUME_PLUGIN_ID, new BeamKafkaInputTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    transformHandlers.put( BeamConst.STRING_BEAM_KAFKA_PRODUCE_PLUGIN_ID, new BeamKafkaOutputTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    transformHandlers.put( BeamConst.STRING_BEAM_ROW_GENERATOR_PLUGIN_ID, new BeamRowGeneratorTransformHandler( variables, pipelineRunConfiguration, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    genericTransformHandler = new BeamGenericTransformHandler( variables, pipelineRunConfiguration, metadataProvider, metaStoreJson, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  public static List<String> findAnnotatedClasses( String folder, String annotationClassName ) throws HopException {
    JarCache jarCache = JarCache.getInstance();
    List<String> classNames = new ArrayList<>();

    // Scan only jar files with @Transform and @ExtensionPointPlugin annotations
    //
    File pluginFolder = new File( "plugins/" + folder );

    try {
      // Get all the jar files in the plugin folder...
      //
      Set<File> files = jarCache.findJarFiles(pluginFolder);
      if ( !files.isEmpty() )
        for ( File file : files ) {

          // These are the jar files : find annotations in it...
          //
          IndexView index = jarCache.getIndex(file);

          // find annotations annotated with this meta-annotation
          for (AnnotationInstance instance : index.getAnnotations(DotName.createSimple(annotationClassName))) {
            if (instance.target() instanceof ClassInfo) {
                ClassInfo classInfo = (ClassInfo) instance.target();
                classNames.add( classInfo.name().toString() );             
            }
        }
      } else {
        System.out.println( "No jar files found in plugin folder " + pluginFolder );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to find annotated classes of class " + annotationClassName, e );
    }
    
    return classNames;
  }

  public Pipeline createPipeline() throws Exception {

    ILogChannel log = LogChannel.GENERAL;

    // Create a new Pipeline
    //
    RunnerType runnerType = pipelineRunConfiguration.getRunnerType();
    Class<? extends PipelineRunner<?>> runnerClass = getPipelineRunnerClass( runnerType );

    PipelineOptions pipelineOptions = pipelineRunConfiguration.getPipelineOptions();
    // The generic options
    //
    pipelineOptions.setUserAgent( pipelineRunConfiguration.resolve( pipelineRunConfiguration.getUserAgent() ) );
    pipelineOptions.setTempLocation( pipelineRunConfiguration.resolve( pipelineRunConfiguration.getTempLocation() ) );
    pipelineOptions.setJobName( pipelineMeta.getName() );

    pipelineOptions.setRunner( runnerClass );
    Pipeline pipeline = Pipeline.create( pipelineOptions );

    pipeline.getCoderRegistry().registerCoderForClass( HopRow.class, new HopRowCoder() );

    log.logBasic( "Created Apache Beam pipeline with name '" + pipelineOptions.getJobName() + "'" );

    // Keep track of which transform outputs which Collection
    //
    Map<String, PCollection<HopRow>> transformCollectionMap = new HashMap<>();

    // Handle io
    //
    handleBeamInputTransforms( log, transformCollectionMap, pipeline );

    // Transform all the other transforms...
    //
    handleGenericTransform( transformCollectionMap, pipeline );

    // Output handling
    //
    handleBeamOutputTransforms( log, transformCollectionMap, pipeline );

    return pipeline;
  }

  public static Class<? extends PipelineRunner<?>> getPipelineRunnerClass( RunnerType runnerType ) throws HopException {
    if ( runnerType == null ) {
      throw new HopException( "Please specify a valid runner type" );
    }
    switch ( runnerType ) {
      case Direct:
        return DirectRunner.class;
      case Flink:
        return FlinkRunner.class;
      case Spark:
        return SparkRunner.class;
      case DataFlow:
        return DataflowRunner.class;
      default:
        throw new HopException( "Unsupported runner type: " + runnerType.name() );
    }
  }

  private void handleBeamInputTransforms( ILogChannel log, Map<String, PCollection<HopRow>> transformCollectionMap, Pipeline pipeline ) throws HopException, IOException {

    List<TransformMeta> beamInputTransformMetas = findBeamInputs();
    for ( TransformMeta transformMeta : beamInputTransformMetas ) {
      IBeamTransformHandler transformHandler = transformHandlers.get( transformMeta.getTransformPluginId() );
      transformHandler.handleTransform( log, transformMeta, transformCollectionMap, pipeline, pipelineMeta.getTransformFields( variables, transformMeta ), null, null );
    }
  }

  private void handleBeamOutputTransforms( ILogChannel log, Map<String, PCollection<HopRow>> transformCollectionMap, Pipeline pipeline ) throws HopException, IOException {
    List<TransformMeta> beamOutputTransformMetas = findBeamOutputs();
    for ( TransformMeta transformMeta : beamOutputTransformMetas ) {
      IBeamTransformHandler transformHandler = transformHandlers.get( transformMeta.getTransformPluginId() );

      List<TransformMeta> previousTransforms = pipelineMeta.findPreviousTransforms( transformMeta, false );
      if ( previousTransforms.size() > 1 ) {
        throw new HopException( "Combining data from multiple transforms is not supported yet!" );
      }
      TransformMeta previousTransform = previousTransforms.get( 0 );

      PCollection<HopRow> input = transformCollectionMap.get( previousTransform.getName() );
      if ( input == null ) {
        throw new HopException( "Previous PCollection for transform " + previousTransform.getName() + " could not be found" );
      }

      // What fields are we getting from the previous transform(s)?
      //
      IRowMeta rowMeta = pipelineMeta.getTransformFields( variables, previousTransform );

      transformHandler.handleTransform( log, transformMeta, transformCollectionMap, pipeline, rowMeta, previousTransforms, input );
    }
  }

  private void handleGenericTransform( Map<String, PCollection<HopRow>> transformCollectionMap, Pipeline pipeline ) throws HopException, IOException {

    ILogChannel log = LogChannel.GENERAL;

    // Perform topological sort
    //
    List<TransformMeta> transforms = getSortedTransformsList();

    for ( TransformMeta transformMeta : transforms ) {

      // Input and output transforms are handled else where.
      //
      IBeamTransformHandler transformHandler = transformHandlers.get( transformMeta.getTransformPluginId() );
      if ( transformHandler == null || ( !transformHandler.isInput() && !transformHandler.isOutput() ) ) {

        // Generic transform
        //
        validateTransformBeamUsage( transformMeta.getTransform() );

        // Lookup all the previous transforms for this one, excluding info transforms like StreamLookup...
        // So the usecase is : we read from multiple io transforms and join to one location...
        //
        List<TransformMeta> previousTransforms = pipelineMeta.findPreviousTransforms( transformMeta, false );

        TransformMeta firstPreviousTransform;
        IRowMeta rowMeta;
        PCollection<HopRow> input = null;

        // Transforms like Merge Join or Merge have no io, only info transforms reaching in
        //
        if ( previousTransforms.isEmpty() ) {
          firstPreviousTransform = null;
          rowMeta = new RowMeta();
        } else {

          // Lookup the previous collection to apply this transforms transform to.
          // We can take any of the inputs so the first one will do.
          //
          firstPreviousTransform = previousTransforms.get( 0 );

          // No fuss with info fields sneaking in, all previous transforms need to emit the same layout anyway
          //
          rowMeta = pipelineMeta.getTransformFields( variables, firstPreviousTransform );

          // Check in the map to see if previousTransform isn't targeting this one
          //
          String targetName = HopBeamUtil.createTargetTupleId( firstPreviousTransform.getName(), transformMeta.getName() );
          input = transformCollectionMap.get( targetName );
          if ( input == null ) {
            input = transformCollectionMap.get( firstPreviousTransform.getName() );
          } else {
            log.logBasic( "Transform " + transformMeta.getName() + " reading from previous transform targeting this one using : " + targetName );
          }

          // If there are multiple io streams into this transform, flatten all the data sources by default
          // This means to simply merge the data.
          //
          if ( previousTransforms.size() > 1 ) {
            List<PCollection<HopRow>> extraInputs = new ArrayList<>();
            for ( int i = 1; i < previousTransforms.size(); i++ ) {
              TransformMeta previousTransform = previousTransforms.get( i );
              PCollection<HopRow> previousPCollection;
              targetName = HopBeamUtil.createTargetTupleId( previousTransform.getName(), transformMeta.getName() );
              previousPCollection = transformCollectionMap.get( targetName );
              if ( previousPCollection == null ) {
                previousPCollection = transformCollectionMap.get( previousTransform.getName() );
              } else {
                log.logBasic( "Transform " + transformMeta.getName() + " reading from previous transform targetting this one using : " + targetName );
              }
              if ( previousPCollection == null ) {
                throw new HopException( "Previous collection was not found for transform " + previousTransform.getName() + ", a previous transform to " + transformMeta.getName() );
              } else {
                extraInputs.add( previousPCollection );
              }
            }

            // Flatten the extra inputs...
            //
            PCollectionList<HopRow> inputList = PCollectionList.of( input );

            for ( PCollection<HopRow> extraInput : extraInputs ) {
              inputList = inputList.and( extraInput );
            }

            // Flatten all the collections.  It's business as usual behind this.
            //
            input = inputList.apply( transformMeta.getName() + " Flatten", Flatten.<HopRow>pCollections() );
          }
        }

        if ( transformHandler != null ) {

          transformHandler.handleTransform( log, transformMeta, transformCollectionMap, pipeline, rowMeta, previousTransforms, input );

        } else {

          genericTransformHandler.handleTransform( log, transformMeta, transformCollectionMap, pipeline, rowMeta, previousTransforms, input );

        }
      }
    }

  }

  private void validateTransformBeamUsage( ITransformMeta meta ) throws HopException {
    if ( meta instanceof GroupByMeta ) {
      throw new HopException( "Group By is not supported.  Use the Memory Group By transform instead.  It comes closest to Beam functionality." );
    }
    if ( meta instanceof SortRowsMeta ) {
      throw new HopException( "Sort rows is not yet supported on Beam." );
    }
    if ( meta instanceof UniqueRowsMeta ) {
      throw new HopException( "The unique rows transform is not yet supported on Beam, for now use a Memory Group By to get distrinct rows" );
    }
  }


  /**
   * Find the Beam Input transforms, return them
   *
   * @throws HopException
   */
  private List<TransformMeta> findBeamInputs() throws HopException {
    List<TransformMeta> transforms = new ArrayList<>();
    for ( TransformMeta transformMeta : pipelineMeta.getPipelineHopTransforms( false ) ) {

      IBeamTransformHandler transformHandler = transformHandlers.get( transformMeta.getTransformPluginId() );
      if ( transformHandler != null && transformHandler.isInput() ) {
        transforms.add( transformMeta );
      }
    }
    return transforms;
  }

  private List<TransformMeta> findBeamOutputs() throws HopException {
    List<TransformMeta> transforms = new ArrayList<>();
    for ( TransformMeta transformMeta : pipelineMeta.getPipelineHopTransforms( false ) ) {
      IBeamTransformHandler transformHandler = transformHandlers.get( transformMeta.getTransformPluginId() );
      if ( transformHandler != null && transformHandler.isOutput() ) {
        transforms.add( transformMeta );
      }
    }
    return transforms;
  }

  /**
   * Sort the transforms from start to finish...
   */
  private List<TransformMeta> getSortedTransformsList() {

    // Create a copy of the transforms
    //
    List<TransformMeta> transforms = new ArrayList<>( pipelineMeta.getPipelineHopTransforms( false ) );

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
    int windowShrinkThreshold = (int) Math.round( transformsSize * 0.75 );

    // give ourselves some room to sort big lists. the window threshold should
    // stop us before reaching this anyway.
    //
    int totalIterations = transformsSize * 2;

    boolean isBefore = false;
    boolean forwardChange = false;
    boolean backwardChange = false;

    boolean lastForwardChange = true;
    boolean keepSortingForward = true;

    TransformMeta one = null;
    TransformMeta two = null;

    long startTime = System.currentTimeMillis();

    for ( int x = 0; x < totalIterations; x++ ) {

      // Go forward through the list
      //
      if ( keepSortingForward ) {
        for ( int y = transformsMinSize; y < transformsSize - 1; y++ ) {
          one = transforms.get( y );
          two = transforms.get( y + 1 );
          isBefore = pipelineMeta.findPrevious( one, two );
          if ( isBefore ) {
            // two was found to be positioned BEFORE one so we need to
            // switch them...
            //
            transforms.set( y, two );
            transforms.set( y + 1, one );
            forwardChange = true;

          }
        }
      }

      // Go backward through the list
      //
      for ( int z = transformsSize - 1; z > transformsMinSize; z-- ) {
        one = transforms.get( z );
        two = transforms.get( z - 1 );

        isBefore = pipelineMeta.findPrevious( one, two );
        if ( !isBefore ) {
          // two was found NOT to be positioned BEFORE one so we need to
          // switch them...
          //
          transforms.set( z, two );
          transforms.set( z - 1, one );
          backwardChange = true;
        }
      }

      // Shrink transformsSize(max) if there was no forward change
      //
      if ( x > windowShrinkThreshold && !forwardChange ) {

        // should we keep going? check the window size
        //
        transformsSize--;
        if ( transformsSize <= transformsMinSize ) {
          break;
        }
      }

      // shrink transformsMinSize(min) if there was no backward change
      //
      if ( x > windowShrinkThreshold && !backwardChange ) {

        // should we keep going? check the window size
        //
        transformsMinSize++;
        if ( transformsMinSize >= transformsSize ) {
          break;
        }
      }

      // End of both forward and backward traversal.
      // Time to see if we should keep going.
      //
      if ( !forwardChange && !backwardChange ) {
        break;
      }

      //
      // if we are past the first iteration and there has been no change twice,
      // quit doing it!
      //
      if ( keepSortingForward && x > 0 && !lastForwardChange && !forwardChange ) {
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
  public Map<String, IBeamTransformHandler> getTransformHandlers() {
    return transformHandlers;
  }

  /**
   * @param transformHandlers The transformHandlers to set
   */
  public void setTransformHandlers( Map<String, IBeamTransformHandler> transformHandlers ) {
    this.transformHandlers = transformHandlers;
  }

  /**
   * Gets genericTransformHandler
   *
   * @return value of genericTransformHandler
   */
  public IBeamTransformHandler getGenericTransformHandler() {
    return genericTransformHandler;
  }

  /**
   * @param genericTransformHandler The genericTransformHandler to set
   */
  public void setGenericTransformHandler( IBeamTransformHandler genericTransformHandler ) {
    this.genericTransformHandler = genericTransformHandler;
  }
}
