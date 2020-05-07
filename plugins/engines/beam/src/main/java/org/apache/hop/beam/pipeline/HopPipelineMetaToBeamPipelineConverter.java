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
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.core.metastore.SerializableMetaStore;
import org.apache.hop.beam.core.util.KettleBeamUtil;
import org.apache.hop.beam.metastore.BeamJobConfig;
import org.apache.hop.beam.metastore.RunnerType;
import org.apache.hop.beam.pipeline.handler.BeamBigQueryInputStepHandler;
import org.apache.hop.beam.pipeline.handler.BeamBigQueryOutputStepHandler;
import org.apache.hop.beam.pipeline.handler.BeamGenericTransformHandler;
import org.apache.hop.beam.pipeline.handler.BeamGroupByStepHandler;
import org.apache.hop.beam.pipeline.handler.BeamInputStepHandler;
import org.apache.hop.beam.pipeline.handler.BeamKafkaInputStepHandler;
import org.apache.hop.beam.pipeline.handler.BeamKafkaOutputStepHandler;
import org.apache.hop.beam.pipeline.handler.BeamMergeJoinStepHandler;
import org.apache.hop.beam.pipeline.handler.BeamOutputStepHandler;
import org.apache.hop.beam.pipeline.handler.BeamPublisherStepHandler;
import org.apache.hop.beam.pipeline.handler.BeamStepHandler;
import org.apache.hop.beam.pipeline.handler.BeamSubscriberStepHandler;
import org.apache.hop.beam.pipeline.handler.BeamTimestampStepHandler;
import org.apache.hop.beam.pipeline.handler.BeamWindowStepHandler;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.JarFileCache;
import org.apache.hop.core.plugins.PluginFolder;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.groupby.GroupByMeta;
import org.apache.hop.pipeline.transforms.sort.SortRowsMeta;
import org.apache.hop.pipeline.transforms.uniquerows.UniqueRowsMeta;
import org.scannotation.AnnotationDB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HopPipelineMetaToBeamPipelineConverter {

  private PipelineMeta pipelineMeta;
  private SerializableMetaStore metaStore;
  private String metaStoreJson;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;
  private Map<String, BeamStepHandler> stepHandlers;
  private BeamStepHandler genericStepHandler;
  private BeamJobConfig beamJobConfig;

  public HopPipelineMetaToBeamPipelineConverter() {
    this.stepHandlers = new HashMap<>();
    this.transformPluginClasses = new ArrayList<>();
    this.xpPluginClasses = new ArrayList<>();
  }

  public HopPipelineMetaToBeamPipelineConverter( PipelineMeta pipelineMeta, IMetaStore metaStore, String pluginsToStage, BeamJobConfig beamJobConfig ) throws MetaStoreException, HopException {
    this();
    this.pipelineMeta = pipelineMeta;
    this.metaStore = new SerializableMetaStore( metaStore );
    this.metaStoreJson = this.metaStore.toJson();
    this.beamJobConfig = beamJobConfig;

    addClassesFromPluginsToStage( pluginsToStage );
    addDefaultTransformHandlers();
  }

  public HopPipelineMetaToBeamPipelineConverter( PipelineMeta pipelineMeta, IMetaStore metaStore, List<String> transformPluginClasses, List<String> xpPluginClasses, BeamJobConfig beamJobConfig )
    throws MetaStoreException {
    this();
    this.pipelineMeta = pipelineMeta;
    this.metaStore = new SerializableMetaStore( metaStore );
    this.metaStoreJson = this.metaStore.toJson();
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
    this.beamJobConfig = beamJobConfig;

    addDefaultTransformHandlers();
  }

  public void addClassesFromPluginsToStage( String pluginsToStage ) throws HopException {
    // Find the plugins in the jar files in the plugin folders to stage...
    //
    if ( StringUtils.isNotEmpty( pluginsToStage ) ) {
      String[] pluginFolders = pluginsToStage.split( "," );
      for ( String pluginFolder : pluginFolders ) {
        List<String> stepClasses = findAnnotatedClasses( pluginFolder, Transform.class.getName() );
        transformPluginClasses.addAll( stepClasses );
        List<String> xpClasses = findAnnotatedClasses( pluginFolder, ExtensionPoint.class.getName() );
        xpPluginClasses.addAll( xpClasses );
      }
    }
  }

  public void addDefaultTransformHandlers() throws MetaStoreException {
    // Add the transform handlers for the special cases, functionality which Beams handles specifically
    //
    stepHandlers.put( BeamConst.STRING_BEAM_INPUT_PLUGIN_ID, new BeamInputStepHandler( beamJobConfig, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamConst.STRING_BEAM_OUTPUT_PLUGIN_ID, new BeamOutputStepHandler( beamJobConfig, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID, new BeamPublisherStepHandler( beamJobConfig, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamConst.STRING_BEAM_SUBSCRIBE_PLUGIN_ID, new BeamSubscriberStepHandler( beamJobConfig, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamConst.STRING_MERGE_JOIN_PLUGIN_ID, new BeamMergeJoinStepHandler( beamJobConfig, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamConst.STRING_MEMORY_GROUP_BY_PLUGIN_ID, new BeamGroupByStepHandler( beamJobConfig, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamConst.STRING_BEAM_WINDOW_PLUGIN_ID, new BeamWindowStepHandler( beamJobConfig, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamConst.STRING_BEAM_TIMESTAMP_PLUGIN_ID, new BeamTimestampStepHandler( beamJobConfig, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamConst.STRING_BEAM_BIGQUERY_INPUT_PLUGIN_ID, new BeamBigQueryInputStepHandler( beamJobConfig, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamConst.STRING_BEAM_BIGQUERY_OUTPUT_PLUGIN_ID, new BeamBigQueryOutputStepHandler( beamJobConfig, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamConst.STRING_BEAM_KAFKA_CONSUME_PLUGIN_ID, new BeamKafkaInputStepHandler( beamJobConfig, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    stepHandlers.put( BeamConst.STRING_BEAM_KAFKA_PRODUCE_PLUGIN_ID, new BeamKafkaOutputStepHandler( beamJobConfig, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses ) );
    genericStepHandler = new BeamGenericTransformHandler( beamJobConfig, metaStore, metaStoreJson, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  public static List<String> findAnnotatedClasses( String folder, String annotationClassName ) throws HopException {
    JarFileCache jarFileCache = JarFileCache.getInstance();
    List<String> classnames = new ArrayList<>();

    // Scan only jar files with @Transform and @ExtensionPointPlugin annotations
    // No plugin.xml format supported for the moment
    //
    PluginFolder pluginFolder = new PluginFolder( "plugins/" + folder, false, true, false );

    try {
      // Get all the jar files in the plugin folder...
      //
      FileObject[] fileObjects = jarFileCache.getFileObjects( pluginFolder );
      if ( fileObjects != null ) {
        // System.out.println( "Found " + fileObjects.length + " jar files in folder " + pluginFolder.getFolder() );

        for ( FileObject fileObject : fileObjects ) {

          // These are the jar files : find annotations in it...
          //
          AnnotationDB annotationDB = jarFileCache.getAnnotationDB( fileObject );

          // These are the jar files : find annotations in it...
          //
          Set<String> impls = annotationDB.getAnnotationIndex().get( annotationClassName );
          if ( impls != null ) {

            for ( String fil : impls ) {
              classnames.add( fil );
            }
          }
        }
      } else {
        System.out.println( "No jar files found in plugin folder " + pluginFolder.getFolder() );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to find annotated classes of class " + annotationClassName, e );
    }

    return classnames;
  }

  public Pipeline createPipeline( PipelineOptions pipelineOptions ) throws Exception {

    ILogChannel log = LogChannel.GENERAL;

    // Create a new Pipeline
    //
    RunnerType runnerType = RunnerType.getRunnerTypeByName( beamJobConfig.getRunnerTypeName() );
    Class<? extends PipelineRunner<?>> runnerClass = getPipelineRunnerClass( runnerType );

    pipelineOptions.setRunner( runnerClass );
    Pipeline pipeline = Pipeline.create( pipelineOptions );

    pipeline.getCoderRegistry().registerCoderForClass( HopRow.class, new HopRowCoder() );

    log.logBasic( "Created pipeline job with name '" + pipelineOptions.getJobName() + "'" );

    // Keep track of which transform outputs which Collection
    //
    Map<String, PCollection<HopRow>> stepCollectionMap = new HashMap<>();

    // Handle io
    //
    handleBeamInputSteps( log, stepCollectionMap, pipeline );

    // Transform all the other transforms...
    //
    handleGenericStep( stepCollectionMap, pipeline );

    // Output handling
    //
    handleBeamOutputSteps( log, stepCollectionMap, pipeline );

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

  private void handleBeamInputSteps( ILogChannel log, Map<String, PCollection<HopRow>> stepCollectionMap, Pipeline pipeline ) throws HopException, IOException {

    List<TransformMeta> beamInputStepMetas = findBeamInputs();
    for ( TransformMeta transformMeta : beamInputStepMetas ) {
      BeamStepHandler stepHandler = stepHandlers.get( transformMeta.getTransformPluginId() );
      stepHandler.handleStep( log, transformMeta, stepCollectionMap, pipeline, pipelineMeta.getTransformFields( transformMeta ), null, null );
    }
  }

  private void handleBeamOutputSteps( ILogChannel log, Map<String, PCollection<HopRow>> stepCollectionMap, Pipeline pipeline ) throws HopException, IOException {
    List<TransformMeta> beamOutputStepMetas = findBeamOutputs();
    for ( TransformMeta transformMeta : beamOutputStepMetas ) {
      BeamStepHandler stepHandler = stepHandlers.get( transformMeta.getTransformPluginId() );

      List<TransformMeta> previousSteps = pipelineMeta.findPreviousTransforms( transformMeta, false );
      if ( previousSteps.size() > 1 ) {
        throw new HopException( "Combining data from multiple transforms is not supported yet!" );
      }
      TransformMeta previousStep = previousSteps.get( 0 );

      PCollection<HopRow> input = stepCollectionMap.get( previousStep.getName() );
      if ( input == null ) {
        throw new HopException( "Previous PCollection for transform " + previousStep.getName() + " could not be found" );
      }

      // What fields are we getting from the previous transform(s)?
      //
      IRowMeta rowMeta = pipelineMeta.getTransformFields( previousStep );

      stepHandler.handleStep( log, transformMeta, stepCollectionMap, pipeline, rowMeta, previousSteps, input );
    }
  }

  private void handleGenericStep( Map<String, PCollection<HopRow>> stepCollectionMap, Pipeline pipeline ) throws HopException, IOException {

    ILogChannel log = LogChannel.GENERAL;

    // Perform topological sort
    //
    List<TransformMeta> transforms = getSortedTransformsList();

    for ( TransformMeta transformMeta : transforms ) {

      // Input and output transforms are handled else where.
      //
      BeamStepHandler stepHandler = stepHandlers.get( transformMeta.getTransformPluginId() );
      if ( stepHandler == null || ( !stepHandler.isInput() && !stepHandler.isOutput() ) ) {

        // Generic transform
        //
        validateStepBeamUsage( transformMeta.getTransform() );

        // Lookup all the previous transforms for this one, excluding info transforms like StreamLookup...
        // So the usecase is : we read from multiple io transforms and join to one location...
        //
        List<TransformMeta> previousSteps = pipelineMeta.findPreviousTransforms( transformMeta, false );

        TransformMeta firstPreviousStep;
        IRowMeta rowMeta;
        PCollection<HopRow> input = null;

        // Steps like Merge Join or Merge have no io, only info transforms reaching in
        //
        if ( previousSteps.isEmpty() ) {
          firstPreviousStep = null;
          rowMeta = new RowMeta();
        } else {

          // Lookup the previous collection to apply this transforms transform to.
          // We can take any of the inputs so the first one will do.
          //
          firstPreviousStep = previousSteps.get( 0 );

          // No fuss with info fields sneaking in, all previous transforms need to emit the same layout anyway
          //
          rowMeta = pipelineMeta.getTransformFields( firstPreviousStep );
          // System.out.println("STEP FIELDS for '"+firstPreviousStep.getName()+"' : "+rowMeta);

          // Check in the map to see if previousStep isn't targeting this one
          //
          String targetName = KettleBeamUtil.createTargetTupleId( firstPreviousStep.getName(), transformMeta.getName() );
          input = stepCollectionMap.get( targetName );
          if ( input == null ) {
            input = stepCollectionMap.get( firstPreviousStep.getName() );
          } else {
            log.logBasic( "Transform " + transformMeta.getName() + " reading from previous transform targeting this one using : " + targetName );
          }

          // If there are multiple io streams into this transform, flatten all the data sources by default
          // This means to simply merge the data.
          //
          if ( previousSteps.size() > 1 ) {
            List<PCollection<HopRow>> extraInputs = new ArrayList<>();
            for ( int i = 1; i < previousSteps.size(); i++ ) {
              TransformMeta previousStep = previousSteps.get( i );
              PCollection<HopRow> previousPCollection;
              targetName = KettleBeamUtil.createTargetTupleId( previousStep.getName(), transformMeta.getName() );
              previousPCollection = stepCollectionMap.get( targetName );
              if ( previousPCollection == null ) {
                previousPCollection = stepCollectionMap.get( previousStep.getName() );
              } else {
                log.logBasic( "Transform " + transformMeta.getName() + " reading from previous transform targetting this one using : " + targetName );
              }
              if ( previousPCollection == null ) {
                throw new HopException( "Previous collection was not found for transform " + previousStep.getName() + ", a previous transform to " + transformMeta.getName() );
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

        if ( stepHandler != null ) {

          stepHandler.handleStep( log, transformMeta, stepCollectionMap, pipeline, rowMeta, previousSteps, input );

        } else {

          genericStepHandler.handleStep( log, transformMeta, stepCollectionMap, pipeline, rowMeta, previousSteps, input );

        }
      }
    }

  }

  private void validateStepBeamUsage( ITransformMeta meta ) throws HopException {
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

      BeamStepHandler stepHandler = stepHandlers.get( transformMeta.getTransformPluginId() );
      if ( stepHandler != null && stepHandler.isInput() ) {
        transforms.add( transformMeta );
      }
    }
    return transforms;
  }

  private List<TransformMeta> findBeamOutputs() throws HopException {
    List<TransformMeta> transforms = new ArrayList<>();
    for ( TransformMeta transformMeta : pipelineMeta.getPipelineHopTransforms( false ) ) {
      BeamStepHandler stepHandler = stepHandlers.get( transformMeta.getTransformPluginId() );
      if ( stepHandler != null && stepHandler.isOutput() ) {
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

    int stepsMinSize = 0;
    int stepsSize = transforms.size();

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
    int windowShrinkThreshold = (int) Math.round( stepsSize * 0.75 );

    // give ourselves some room to sort big lists. the window threshold should
    // stop us before reaching this anyway.
    //
    int totalIterations = stepsSize * 2;

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
        for ( int y = stepsMinSize; y < stepsSize - 1; y++ ) {
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
      for ( int z = stepsSize - 1; z > stepsMinSize; z-- ) {
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

      // Shrink stepsSize(max) if there was no forward change
      //
      if ( x > windowShrinkThreshold && !forwardChange ) {

        // should we keep going? check the window size
        //
        stepsSize--;
        if ( stepsSize <= stepsMinSize ) {
          break;
        }
      }

      // shrink stepsMinSize(min) if there was no backward change
      //
      if ( x > windowShrinkThreshold && !backwardChange ) {

        // should we keep going? check the window size
        //
        stepsMinSize++;
        if ( stepsMinSize >= stepsSize ) {
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
   * Gets stepHandlers
   *
   * @return value of stepHandlers
   */
  public Map<String, BeamStepHandler> getStepHandlers() {
    return stepHandlers;
  }

  /**
   * @param stepHandlers The stepHandlers to set
   */
  public void setStepHandlers( Map<String, BeamStepHandler> stepHandlers ) {
    this.stepHandlers = stepHandlers;
  }

  /**
   * Gets genericStepHandler
   *
   * @return value of genericStepHandler
   */
  public BeamStepHandler getGenericStepHandler() {
    return genericStepHandler;
  }

  /**
   * @param genericStepHandler The genericStepHandler to set
   */
  public void setGenericStepHandler( BeamStepHandler genericStepHandler ) {
    this.genericStepHandler = genericStepHandler;
  }
}