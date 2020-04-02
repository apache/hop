/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * We can use this factory to create pipelines with a source and target transform.<br>
 * The source transform is an Injector transform.<br>
 * The target transform is a dummy transform.<br>
 * The middle transform is the transform specified.<br>
 *
 * @author Matt Casters
 */
public class PipelineTestFactory {
  public static final String INJECTOR_TRANSFORMNAME = "injector";
  public static final String DUMMY_TRANSFORMNAME = "dummy";
  public static final String ERROR_TRANSFORMNAME = "dummyError";

  public static final String NUMBER_ERRORS_FIELD = "NumberErrors";
  public static final String ERROR_DESC_FIELD = "ErrorDescription";
  public static final String ERROR_FIELD_VALUE = "ErrorFieldValue";
  public static final String ERROR_CODE_VALUE = "ErrorCodeValue";

  static PluginRegistry registry = PluginRegistry.getInstance();

  public static PipelineMeta generateTestPipeline( IVariables parent, ITransformMeta oneMeta,
                                                   String oneTransformName ) {
    return generateTestPipeline( parent, oneMeta, oneTransformName, null );
  }

  public static PipelineMeta generateTestPipeline( IVariables parent, ITransformMeta oneMeta,
                                                   String oneTransformName, IRowMeta injectorRowMeta ) {
    PipelineMeta previewMeta = new PipelineMeta( parent );

    // First the injector transform...
    TransformMeta zero = getInjectorTransformMeta( injectorRowMeta );
    previewMeta.addTransform( zero );

    // Then the middle transform to test...
    //
    TransformMeta one = new TransformMeta( registry.getPluginId( TransformPluginType.class, oneMeta ), oneTransformName, oneMeta );
    one.setLocation( 150, 50 );
    previewMeta.addTransform( one );

    // Then we add the dummy transform to read the results from
    TransformMeta two = getReadTransformMeta();
    previewMeta.addTransform( two );

    // Add the hops between the 3 transforms.
    PipelineHopMeta zeroOne = new PipelineHopMeta( zero, one );
    previewMeta.addPipelineHop( zeroOne );
    PipelineHopMeta oneTwo = new PipelineHopMeta( one, two );
    previewMeta.addPipelineHop( oneTwo );

    return previewMeta;
  }

  public static PipelineMeta generateTestPipelineError( IVariables parent, ITransformMeta oneMeta,
                                                        String oneTransformName ) {
    PipelineMeta previewMeta = new PipelineMeta( parent );

    if ( parent == null ) {
      parent = new Variables();
    }

    // First the injector transform...
    TransformMeta zero = getInjectorTransformMeta();
    previewMeta.addTransform( zero );

    // Then the middle transform to test...
    //
    TransformMeta one = new TransformMeta( registry.getPluginId( TransformPluginType.class, oneMeta ), oneTransformName, oneMeta );
    one.setLocation( 150, 50 );
    previewMeta.addTransform( one );

    // Then we add the dummy transform to read the results from
    TransformMeta two = getReadTransformMeta();
    previewMeta.addTransform( two );

    // error handling transform
    TransformMeta err = getReadTransformMeta( ERROR_TRANSFORMNAME );
    previewMeta.addTransform( err );

    // Add the hops between the 3 transforms.
    PipelineHopMeta zeroOne = new PipelineHopMeta( zero, one );
    previewMeta.addPipelineHop( zeroOne );
    PipelineHopMeta oneTwo = new PipelineHopMeta( one, two );
    previewMeta.addPipelineHop( oneTwo );

    TransformErrorMeta errMeta = new TransformErrorMeta( parent, one, err );
    errMeta.setEnabled( true );

    errMeta.setNrErrorsValuename( NUMBER_ERRORS_FIELD );
    errMeta.setErrorDescriptionsValuename( ERROR_DESC_FIELD );
    errMeta.setErrorFieldsValuename( ERROR_FIELD_VALUE );
    errMeta.setErrorCodesValuename( ERROR_CODE_VALUE );

    one.setTransformErrorMeta( errMeta );
    PipelineHopMeta oneErr = new PipelineHopMeta( one, err );
    previewMeta.addPipelineHop( oneErr );

    return previewMeta;
  }

  public static List<RowMetaAndData> executeTestPipeline( PipelineMeta pipelineMeta,
                                                          String testTransformName, List<RowMetaAndData> inputData ) throws HopException {
    return executeTestPipeline( pipelineMeta, INJECTOR_TRANSFORMNAME, testTransformName, DUMMY_TRANSFORMNAME, inputData );
  }

  public static List<RowMetaAndData> executeTestPipeline( PipelineMeta pipelineMeta, String injectorTransformName,
                                                          String testTransformName, String dummyTransformname, List<RowMetaAndData> inputData ) throws HopException {
    return executeTestPipeline( pipelineMeta, injectorTransformName, testTransformName,
      dummyTransformname, inputData, null, null );
  }

  public static List<RowMetaAndData> executeTestPipeline( PipelineMeta pipelineMeta, String injectorTransformName,
                                                          String testTransformName, String dummyTransformname, List<RowMetaAndData> inputData,
                                                          IVariables runTimeVariables, IVariables runTimeParameters ) throws HopException {
    // Now execute the pipeline...
    Pipeline pipeline = new Pipeline( pipelineMeta );

    pipeline.initializeVariablesFrom( runTimeVariables );
    if ( runTimeParameters != null ) {
      for ( String param : pipeline.listParameters() ) {
        String value = runTimeParameters.getVariable( param );
        if ( value != null ) {
          pipeline.setParameterValue( param, value );
          pipelineMeta.setParameterValue( param, value );
        }
      }
    }
    pipeline.prepareExecution();

    // Capture the rows that come out of the dummy transform...
    //
    ITransform si = pipeline.getTransformInterface( dummyTransformname, 0 );
    PipelineTransformCollector dummyRc = new PipelineTransformCollector();
    si.addRowListener( dummyRc );

    // Add a row producer...
    //
    RowProducer rp = pipeline.addRowProducer( injectorTransformName, 0 );

    // Start the transforms...
    //
    pipeline.startThreads();

    // Inject the actual test rows...
    //
    List<RowMetaAndData> inputList = inputData;
    Iterator<RowMetaAndData> it = inputList.iterator();
    while ( it.hasNext() ) {
      RowMetaAndData rm = it.next();
      rp.putRow( rm.getRowMeta(), rm.getData() );
    }
    rp.finished();

    // Wait until the pipeline is finished...
    //
    pipeline.waitUntilFinished();

    // If there is an error in the result, throw an exception here...
    //
    if ( pipeline.getResult().getNrErrors() > 0 ) {
      throw new HopException( "Test pipeline finished with errors. Check the log." );
    }

    // Return the result from the dummy transform...
    //
    return dummyRc.getRowsRead();
  }

  public static Map<String, PipelineTransformCollector> executeTestPipelineError( PipelineMeta pipelineMeta, String testTransformName,
                                                                                  List<RowMetaAndData> inputData ) throws HopException {
    return executeTestPipelineError( pipelineMeta, INJECTOR_TRANSFORMNAME, testTransformName, DUMMY_TRANSFORMNAME, ERROR_TRANSFORMNAME,
      inputData );
  }

  public static Map<String, PipelineTransformCollector> executeTestPipelineError( PipelineMeta pipelineMeta,
                                                                                  String injectorTransformName, String testTransformName, String dummyTransformname, String errorTransformName,
                                                                                  List<RowMetaAndData> inputData ) throws HopException {
    // Now execute the pipeline...
    Pipeline pipeline = new Pipeline( pipelineMeta );

    pipeline.prepareExecution();

    // Capture the rows that come out of the dummy transform...
    //
    ITransform si = pipeline.getTransformInterface( dummyTransformname, 0 );
    PipelineTransformCollector dummyRc = new PipelineTransformCollector();
    si.addRowListener( dummyRc );

    ITransform junit = pipeline.getTransformInterface( testTransformName, 0 );
    PipelineTransformCollector dummyJu = new PipelineTransformCollector();
    junit.addRowListener( dummyJu );

    // add error handler
    ITransform er = pipeline.getTransformInterface( errorTransformName, 0 );
    PipelineTransformCollector erColl = new PipelineTransformCollector();
    er.addRowListener( erColl );

    // Add a row producer...
    //
    RowProducer rp = pipeline.addRowProducer( injectorTransformName, 0 );

    // Start the transforms...
    //
    pipeline.startThreads();

    // Inject the actual test rows...
    //
    List<RowMetaAndData> inputList = inputData;
    Iterator<RowMetaAndData> it = inputList.iterator();
    while ( it.hasNext() ) {
      RowMetaAndData rm = it.next();
      rp.putRow( rm.getRowMeta(), rm.getData() );
    }
    rp.finished();

    // Wait until the pipeline is finished...
    //
    pipeline.waitUntilFinished();

    // If there is an error in the result, throw an exception here...
    //
    if ( pipeline.getResult().getNrErrors() > 0 ) {
      throw new HopException( "Test pipeline finished with errors. Check the log." );
    }

    // Return the result from the dummy transform...
    Map<String, PipelineTransformCollector> ret = new HashMap<String, PipelineTransformCollector>();
    ret.put( dummyTransformname, dummyRc );
    ret.put( errorTransformName, erColl );
    ret.put( testTransformName, dummyJu );
    return ret;
  }

  static TransformMeta getInjectorTransformMeta() {
    return getInjectorTransformMeta( null );
  }

  static TransformMeta getInjectorTransformMeta( IRowMeta outputRowMeta ) {
    InjectorMeta zeroMeta = new InjectorMeta();

    // Sets output fields for cases when no rows are sent to the test transform, but metadata is still needed
    if ( outputRowMeta != null && outputRowMeta.size() > 0 ) {
      String[] fieldName = new String[ outputRowMeta.size() ];
      int[] fieldLength = new int[ outputRowMeta.size() ];
      int[] fieldPrecision = new int[ outputRowMeta.size() ];
      int[] fieldType = new int[ outputRowMeta.size() ];
      for ( int i = 0; i < outputRowMeta.size(); i++ ) {
        IValueMeta field = outputRowMeta.getValueMeta( i );
        fieldName[ i ] = field.getName();
        fieldLength[ i ] = field.getLength();
        fieldPrecision[ i ] = field.getPrecision();
        fieldType[ i ] = field.getType();
      }
      zeroMeta.setFieldname( fieldName );
      zeroMeta.setLength( fieldLength );
      zeroMeta.setPrecision( fieldPrecision );
      zeroMeta.setType( fieldType );
    }

    TransformMeta zero = new TransformMeta( registry.getPluginId( TransformPluginType.class, zeroMeta ), INJECTOR_TRANSFORMNAME, zeroMeta );
    zero.setLocation( 50, 50 );

    return zero;
  }

  static TransformMeta getReadTransformMeta( String name ) {
    DummyMeta twoMeta = new DummyMeta();
    TransformMeta two = new TransformMeta( registry.getPluginId( TransformPluginType.class, twoMeta ), name, twoMeta );
    two.setLocation( 250, 50 );
    return two;
  }

  static TransformMeta getReadTransformMeta() {
    return getReadTransformMeta( DUMMY_TRANSFORMNAME );
  }

}
