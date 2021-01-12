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

package org.apache.hop.beam.core.transform;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.joda.time.Instant;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.beam.core.shared.VariableValue;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.core.util.HopBeamUtil;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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

  // Used in the private TransformFn class below
  //
  protected List<PCollectionView<List<HopRow>>> infoCollectionViews;

  // Log and count errors.
  protected static final Logger LOG = LoggerFactory.getLogger( TransformTransform.class );
  protected static final Counter numErrors = Metrics.counter( "main", "TransformErrors" );

  public TransformTransform() {
    variableValues = new ArrayList<>();
  }

  public TransformTransform( List<VariableValue> variableValues, String metastoreJson, List<String> transformPluginClasses, List<String> xpPluginClasses,
                             int batchSize, int flushIntervalMs, String transformName, String transformPluginId, String transformMetaInterfaceXml, String inputRowMetaJson, boolean inputTransform,
                             List<String> targetTransforms, List<String> infoTransforms, List<String> infoRowMetaJsons, List<PCollectionView<List<HopRow>>> infoCollectionViews ) {
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
  }

  @Override public PCollectionTuple expand( PCollection<HopRow> input ) {
    try {
      // Only initialize once on this node/vm
      //
      BeamHop.init( transformPluginClasses, xpPluginClasses );

      // Similar for the output : treate a TupleTag list for the target transforms...
      //
      TupleTag<HopRow> mainOutputTupleTag = new TupleTag<HopRow>( HopBeamUtil.createMainOutputTupleId( transformName ) ) {
      };
      List<TupleTag<HopRow>> targetTupleTags = new ArrayList<>();
      TupleTagList targetTupleTagList = null;
      for ( String targetTransform : targetTransforms ) {
        String tupleId = HopBeamUtil.createTargetTupleId( transformName, targetTransform );
        TupleTag<HopRow> tupleTag = new TupleTag<HopRow>( tupleId ) {
        };
        targetTupleTags.add( tupleTag );
        if ( targetTupleTagList == null ) {
          targetTupleTagList = TupleTagList.of( tupleTag );
        } else {
          targetTupleTagList = targetTupleTagList.and( tupleTag );
        }
      }
      if ( targetTupleTagList == null ) {
        targetTupleTagList = TupleTagList.empty();
      }

      // Create a new transform function, initializes the transform
      //
      TransformFn transformFn = new TransformFn( variableValues, metastoreJson, transformPluginClasses, xpPluginClasses,
        transformName, transformPluginId, transformMetaInterfaceXml, inputRowMetaJson, inputTransform,
        targetTransforms, infoTransforms, infoRowMetaJsons );

      // The actual transform functionality
      //
      ParDo.SingleOutput<HopRow, HopRow> parDoTransformFn = ParDo.of( transformFn );

      // Add optional side inputs...
      //
      if ( infoCollectionViews.size() > 0 ) {
        parDoTransformFn = parDoTransformFn.withSideInputs( infoCollectionViews );
      }

      // Specify the main output and targeted outputs
      //
      ParDo.MultiOutput<HopRow, HopRow> multiOutput = parDoTransformFn.withOutputTags( mainOutputTupleTag, targetTupleTagList );

      // Apply the multi output parallel do transform function to the main input stream
      //
      PCollectionTuple collectionTuple = input.apply( multiOutput );

      // In the tuple is everything we need to find.
      // Just make sure to retrieve the PCollections using the correct Tuple ID
      // Use HopBeamUtil.createTargetTupleId()... to make sure
      //
      return collectionTuple;
    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error transforming data in transform '" + transformName + "'", e );
      throw new RuntimeException( "Error transforming data in transform", e );
    }

  }

  private class TransformFn extends DoFn<HopRow, HopRow> {

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
    protected List<String> targetTransforms;
    protected List<String> infoTransforms;
    protected List<String> infoRowMetaJsons;
    protected boolean inputTransform;
    protected boolean initialize;

    protected List<PCollection<HopRow>> infoCollections;

    // Log and count parse errors.
    private final Counter numErrors = Metrics.counter( "main", "TransformProcessErrors" );

    private transient PipelineMeta pipelineMeta;
    private transient TransformMeta transformMeta;
    private transient IRowMeta inputRowMeta;
    private transient IRowMeta outputRowMeta;
    private transient List<TransformMetaDataCombi> transformCombis;
    private transient LocalPipelineEngine pipeline;
    private transient RowProducer rowProducer;
    private transient IRowListener rowListener;
    private transient List<Object[]> resultRows;
    private transient List<List<Object[]>> targetResultRowsList;
    private transient List<IRowMeta> targetRowMetas;
    private transient List<IRowMeta> infoRowMetas;
    private transient List<RowProducer> infoRowProducers;

    private transient TupleTag<HopRow> mainTupleTag;
    private transient List<TupleTag<HopRow>> tupleTagList;

    private transient Counter initCounter;
    private transient Counter readCounter;
    private transient Counter writtenCounter;

    private transient SingleThreadedPipelineExecutor executor;

    public TransformFn() {
    }


    // I created a private class because instances of this one need access to infoCollectionViews
    //

    public TransformFn( List<VariableValue> variableValues, String metastoreJson, List<String> transformPluginClasses, List<String> xpPluginClasses, String transformName, String transformPluginId,
                   String transformMetaInterfaceXml, String inputRowMetaJson, boolean inputTransform,
                   List<String> targetTransforms, List<String> infoTransforms, List<String> infoRowMetaJsons ) {
      this();
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
      this.initialize = true;
    }

    /**
     * Reset the row buffer every time we start a new bundle to prevent the output of double rows
     *
     * @param startBundleContext
     */
    @StartBundle
    public void startBundle( StartBundleContext startBundleContext ) {
      Metrics.counter( "startBundle", transformName ).inc();
      if ( "ScriptValueMod".equals( transformPluginId ) && pipeline != null ) {
        initialize = true;
      }
    }

    @Setup
    public void setup() {
      // Nothing
    }

    @Teardown
    public void tearDown() {
      // Nothing
    }

    @ProcessElement
    public void processElement( ProcessContext context, BoundedWindow window ) {

      try {
        if ( initialize ) {
          initialize = false;

          // Initialize Hop and load extra plugins as well
          //
          BeamHop.init( transformPluginClasses, xpPluginClasses );

          // The content of the metadata is JSON serialized and inflated below.
          //
          IHopMetadataProvider metadataProvider = new SerializableMetadataProvider( metastoreJson );

          // Create a very simple new transformation to run single threaded...
          // Single threaded...
          //
          pipelineMeta = new PipelineMeta();
          pipelineMeta.setPipelineType( PipelineMeta.PipelineType.SingleThreaded );
          pipelineMeta.setMetadataProvider( metadataProvider );

          // Input row metadata...
          //
          inputRowMeta = JsonRowMeta.fromJson( inputRowMetaJson );
          infoRowMetas = new ArrayList<>();
          for ( String infoRowMetaJson : infoRowMetaJsons ) {
            IRowMeta infoRowMeta = JsonRowMeta.fromJson( infoRowMetaJson );
            infoRowMetas.add( infoRowMeta );
          }

          // Create an Injector transform with the right row layout...
          // This will help all transforms see the row layout statically...
          //
          TransformMeta mainInjectorTransformMeta = null;
          if ( !inputTransform ) {
            mainInjectorTransformMeta = createInjectorTransform( pipelineMeta, INJECTOR_TRANSFORM_NAME, inputRowMeta, 200, 200 );
          }

          // Our main transform writes to a bunch of targets
          // Add a dummy transform for each one so the transform can target them
          //
          int targetLocationY = 200;
          List<TransformMeta> targetTransformMetas = new ArrayList<>();
          for ( String targetTransform : targetTransforms ) {
            DummyMeta dummyMeta = new DummyMeta();
            TransformMeta targetTransformMeta = new TransformMeta( targetTransform, dummyMeta );
            targetTransformMeta.setLocation( 600, targetLocationY );
            targetLocationY += 150;

            targetTransformMetas.add( targetTransformMeta );
            pipelineMeta.addTransform( targetTransformMeta );
          }

          // The transform might read information from info transforms
          // Transforms like "Stream Lookup" or "Validator"
          // They read all the data on input from a side input
          //
          List<List<HopRow>> infoDataSets = new ArrayList<>();
          List<TransformMeta> infoTransformMetas = new ArrayList<>();
          for ( int i = 0; i < infoTransforms.size(); i++ ) {
            String infoTransform = infoTransforms.get( i );
            PCollectionView<List<HopRow>> cv = infoCollectionViews.get( i );

            // Get the data from the side input, from the info transform(s)
            //
            List<HopRow> infoDataSet = context.sideInput( cv );
            infoDataSets.add( infoDataSet );

            IRowMeta infoRowMeta = infoRowMetas.get( i );

            // Add an Injector transform for every info transform so the transform can read from it
            //
            TransformMeta infoTransformMeta = createInjectorTransform( pipelineMeta, infoTransform, infoRowMeta, 200, 350 + 150 * i );
            infoTransformMetas.add( infoTransformMeta );
          }

          transformCombis = new ArrayList<>();

          // The main transform inflated from XML metadata...
          //
          PluginRegistry registry = PluginRegistry.getInstance();
          ITransformMeta iTransformMeta = registry.loadClass( TransformPluginType.class, transformPluginId, ITransformMeta.class );
          if ( iTransformMeta == null ) {
            throw new HopException( "Unable to load transform plugin with ID " + transformPluginId + ", this plugin isn't in the plugin registry or classpath" );
          }

          HopBeamUtil.loadTransformMetadataFromXml( transformName, iTransformMeta, transformMetaInterfaceXml, pipelineMeta.getMetadataProvider() );

          transformMeta = new TransformMeta( transformName, iTransformMeta );
          transformMeta.setTransformPluginId( transformPluginId );
          transformMeta.setLocation( 400, 200 );
          pipelineMeta.addTransform( transformMeta );
          if ( !inputTransform ) {
            pipelineMeta.addPipelineHop( new PipelineHopMeta( mainInjectorTransformMeta, transformMeta ) );
          }
          // The target hops as well
          //
          for ( TransformMeta targetTransformMeta : targetTransformMetas ) {
            pipelineMeta.addPipelineHop( new PipelineHopMeta( transformMeta, targetTransformMeta ) );
          }

          // And the info hops...
          //
          for ( TransformMeta infoTransformMeta : infoTransformMetas ) {
            pipelineMeta.addPipelineHop( new PipelineHopMeta( infoTransformMeta, transformMeta ) );
          }

          iTransformMeta.searchInfoAndTargetTransforms( pipelineMeta.getTransforms() );

          // Create the transformation...
          //
          pipeline = new LocalPipelineEngine( pipelineMeta );
          pipeline.setLogLevel( LogLevel.ERROR );
          pipeline.setMetadataProvider( pipelineMeta.getMetadataProvider() );
          pipeline.prepareExecution();

          // Give transforms variables from above
          //
          for ( VariableValue variableValue : variableValues ) {
            if ( StringUtils.isNotEmpty( variableValue.getVariable() ) ) {
              pipeline.setVariable( variableValue.getVariable(), variableValue.getValue() );
            }
          }

          // Create producers so we can efficiently pass data
          //
          rowProducer = null;
          if ( !inputTransform ) {
            rowProducer = pipeline.addRowProducer( INJECTOR_TRANSFORM_NAME, 0 );
          }
          infoRowProducers = new ArrayList<>();
          for ( String infoTransform : infoTransforms ) {
            RowProducer infoRowProducer = pipeline.addRowProducer( infoTransform, 0 );
            infoRowProducers.add( infoRowProducer );
          }

          // Find the right interfaces for execution later...
          //
          if ( !inputTransform ) {
            TransformMetaDataCombi injectorCombi = findCombi( pipeline, INJECTOR_TRANSFORM_NAME );
            transformCombis.add( injectorCombi );
          }

          TransformMetaDataCombi transformCombi = findCombi( pipeline, transformName );
          transformCombis.add( transformCombi );
          outputRowMeta = pipelineMeta.getTransformFields( pipeline, transformName );

          if ( targetTransforms.isEmpty() ) {
            rowListener = new RowAdapter() {
              @Override public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
                resultRows.add( row );
              }
            };
            transformCombi.transform.addRowListener( rowListener );
          }

          // Create a list of TupleTag to direct the target rows
          //
          mainTupleTag = new TupleTag<HopRow>( HopBeamUtil.createMainOutputTupleId( transformName ) ) {
          };
          tupleTagList = new ArrayList<>();

          // The lists in here will contain all the rows that ended up in the various target transforms (if any)
          //
          targetRowMetas = new ArrayList<>();
          targetResultRowsList = new ArrayList<>();

          for ( String targetTransform : targetTransforms ) {
            TransformMetaDataCombi targetCombi = findCombi( pipeline, targetTransform );
            transformCombis.add( targetCombi );
            targetRowMetas.add( pipelineMeta.getTransformFields( pipeline, transformCombi.transformName ) );

            String tupleId = HopBeamUtil.createTargetTupleId( transformName, targetTransform );
            TupleTag<HopRow> tupleTag = new TupleTag<HopRow>( tupleId ) {
            };
            tupleTagList.add( tupleTag );
            final List<Object[]> targetResultRows = new ArrayList<>();
            targetResultRowsList.add( targetResultRows );

            targetCombi.transform.addRowListener( new RowAdapter() {
              @Override public void rowReadEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
                // We send the target row to a specific list...
                //
                targetResultRows.add( row );
              }
            } );
          }

          executor = new SingleThreadedPipelineExecutor( pipeline );

          // Initialize the transforms...
          //
          executor.init();

          initCounter = Metrics.counter( Pipeline.METRIC_NAME_INIT, transformName );
          readCounter = Metrics.counter( Pipeline.METRIC_NAME_READ, transformName );
          writtenCounter = Metrics.counter( Pipeline.METRIC_NAME_WRITTEN, transformName );

          initCounter.inc();

          // Doesn't really start the threads in single threaded mode
          // Just sets some flags all over the place
          //
          pipeline.startThreads();

          resultRows = new ArrayList<>();

          // Copy the info data sets to the info transforms...
          // We do this only once so all subsequent rows can use this.
          //
          for ( int i = 0; i < infoTransforms.size(); i++ ) {
            RowProducer infoRowProducer = infoRowProducers.get( i );
            List<HopRow> infoDataSet = infoDataSets.get( i );
            TransformMetaDataCombi combi = findCombi( pipeline, infoTransforms.get( i ) );
            IRowMeta infoRowMeta = infoRowMetas.get( i );

            // Pass and process the rows in the info transforms
            //
            for ( HopRow infoRowData : infoDataSet ) {
              infoRowProducer.putRow( infoRowMeta, infoRowData.getRow() );
              combi.transform.processRow();
            }

            // By calling finished() transforms like Stream Lookup know no more rows are going to come
            // and they can start to work with the info data set
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
        HopRow inputRow = HopBeamUtil.copyHopRow( originalInputRow, inputRowMeta );
        readCounter.inc();

        emptyRowBuffer( new TransformProcessContext( context ), inputRow );
      } catch ( Exception e ) {
        numErrors.inc();
        LOG.info( "Transform execution error :" + e.getMessage() );
        throw new RuntimeException( "Error executing TransformFn", e );
      }
    }

    @FinishBundle
    public void finishBundle( FinishBundleContext context ) {
      try {
        // Nothing to do here
      } catch ( Exception e ) {
        numErrors.inc();
        LOG.info( "Transform finishing bundle error :" + e.getMessage() );
        throw new RuntimeException( "Error finalizing bundle of transform '" + transformName + "'", e );
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
    private synchronized void emptyRowBuffer( TupleOutputContext<HopRow> context, HopRow inputRow ) throws HopException {
      // Empty all the row buffers for another iteration
      //
      resultRows.clear();
      for ( int t = 0; t < targetTransforms.size(); t++ ) {
        targetResultRowsList.get( t ).clear();
      }

      // Pass the rows in the rowBuffer to the input RowSet
      //
      if ( !inputTransform ) {
        rowProducer.putRow( inputRowMeta, inputRow.getRow() );
      }

      // Execute all transforms in the transformation
      //
      executor.oneIteration();

      // Evaluate the results...
      //

      // Pass all rows in the output to the process context
      //
      for ( Object[] resultRow : resultRows ) {

        // Pass the row to the process context
        //
        context.output( mainTupleTag, new HopRow( resultRow ) );
        writtenCounter.inc();
      }

      // Pass whatever ended up on the target nodes
      //
      for ( int t = 0; t < targetResultRowsList.size(); t++ ) {
        List<Object[]> targetRowsList = targetResultRowsList.get( t );
        TupleTag<HopRow> tupleTag = tupleTagList.get( t );

        for ( Object[] targetRow : targetRowsList ) {
          context.output( tupleTag, new HopRow( targetRow ) );
        }
      }
    }

    private TransformMeta createInjectorTransform( PipelineMeta pipelineMeta, String injectorTransformName, IRowMeta injectorRowMeta, int x, int y ) {
      InjectorMeta injectorMeta = new InjectorMeta();
      injectorMeta.allocate( injectorRowMeta.size() );
      for ( int i = 0; i < injectorRowMeta.size(); i++ ) {
        IValueMeta valueMeta = injectorRowMeta.getValueMeta( i );
        injectorMeta.getFieldname()[ i ] = valueMeta.getName();
        injectorMeta.getType()[ i ] = valueMeta.getType();
        injectorMeta.getLength()[ i ] = valueMeta.getLength();
        injectorMeta.getPrecision()[ i ] = valueMeta.getPrecision();
      }
      TransformMeta injectorTransformMeta = new TransformMeta( injectorTransformName, injectorMeta );
      injectorTransformMeta.setLocation( x, y );
      pipelineMeta.addTransform( injectorTransformMeta );

      return injectorTransformMeta;
    }

    private TransformMetaDataCombi findCombi( Pipeline pipeline, String transformName ) {
      for ( TransformMetaDataCombi combi : pipeline.getTransforms() ) {
        if ( combi.transformName.equals( transformName ) ) {
          return combi;
        }
      }
      throw new RuntimeException( "Configuration error, transform '" + transformName + "' not found in transformation" );
    }
  }

  private interface TupleOutputContext<T> {
    void output( TupleTag<T> tupleTag, T output );
  }

  private class TransformProcessContext implements TupleOutputContext<HopRow> {

    private DoFn.ProcessContext context;

    public TransformProcessContext( DoFn.ProcessContext processContext ) {
      this.context = processContext;
    }

    @Override public void output( TupleTag<HopRow> tupleTag, HopRow output ) {
      context.output( tupleTag, output );
    }
  }

  private class TransformFinishBundleContext implements TupleOutputContext<HopRow> {

    private DoFn.FinishBundleContext context;
    private BoundedWindow batchWindow;

    public TransformFinishBundleContext( DoFn.FinishBundleContext context, BoundedWindow batchWindow ) {
      this.context = context;
      this.batchWindow = batchWindow;
    }

    @Override public void output( TupleTag<HopRow> tupleTag, HopRow output ) {
      context.output( tupleTag, output, Instant.now(), batchWindow );
    }
  }
}
