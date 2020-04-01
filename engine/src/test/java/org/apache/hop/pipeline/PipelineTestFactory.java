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
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.step.StepErrorMeta;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.steps.dummy.DummyMeta;
import org.apache.hop.pipeline.steps.injector.InjectorMeta;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * We can use this factory to create pipelines with a source and target step.<br>
 * The source step is an Injector step.<br>
 * The target step is a dummy step.<br>
 * The middle step is the step specified.<br>
 *
 * @author Matt Casters
 */
public class PipelineTestFactory {
  public static final String INJECTOR_STEPNAME = "injector";
  public static final String DUMMY_STEPNAME = "dummy";
  public static final String ERROR_STEPNAME = "dummyError";

  public static final String NUMBER_ERRORS_FIELD = "NumberErrors";
  public static final String ERROR_DESC_FIELD = "ErrorDescription";
  public static final String ERROR_FIELD_VALUE = "ErrorFieldValue";
  public static final String ERROR_CODE_VALUE = "ErrorCodeValue";

  static PluginRegistry registry = PluginRegistry.getInstance();

  public static PipelineMeta generateTestPipeline( VariableSpace parent, StepMetaInterface oneMeta,
                                                   String oneStepname ) {
    return generateTestPipeline( parent, oneMeta, oneStepname, null );
  }

  public static PipelineMeta generateTestPipeline( VariableSpace parent, StepMetaInterface oneMeta,
                                                   String oneStepname, RowMetaInterface injectorRowMeta ) {
    PipelineMeta previewMeta = new PipelineMeta( parent );

    // First the injector step...
    StepMeta zero = getInjectorStepMeta( injectorRowMeta );
    previewMeta.addStep( zero );

    // Then the middle step to test...
    //
    StepMeta one = new StepMeta( registry.getPluginId( StepPluginType.class, oneMeta ), oneStepname, oneMeta );
    one.setLocation( 150, 50 );
    previewMeta.addStep( one );

    // Then we add the dummy step to read the results from
    StepMeta two = getReadStepMeta();
    previewMeta.addStep( two );

    // Add the hops between the 3 steps.
    PipelineHopMeta zeroOne = new PipelineHopMeta( zero, one );
    previewMeta.addPipelineHop( zeroOne );
    PipelineHopMeta oneTwo = new PipelineHopMeta( one, two );
    previewMeta.addPipelineHop( oneTwo );

    return previewMeta;
  }

  public static PipelineMeta generateTestPipelineError( VariableSpace parent, StepMetaInterface oneMeta,
                                                        String oneStepname ) {
    PipelineMeta previewMeta = new PipelineMeta( parent );

    if ( parent == null ) {
      parent = new Variables();
    }

    // First the injector step...
    StepMeta zero = getInjectorStepMeta();
    previewMeta.addStep( zero );

    // Then the middle step to test...
    //
    StepMeta one = new StepMeta( registry.getPluginId( StepPluginType.class, oneMeta ), oneStepname, oneMeta );
    one.setLocation( 150, 50 );
    previewMeta.addStep( one );

    // Then we add the dummy step to read the results from
    StepMeta two = getReadStepMeta();
    previewMeta.addStep( two );

    // error handling step
    StepMeta err = getReadStepMeta( ERROR_STEPNAME );
    previewMeta.addStep( err );

    // Add the hops between the 3 steps.
    PipelineHopMeta zeroOne = new PipelineHopMeta( zero, one );
    previewMeta.addPipelineHop( zeroOne );
    PipelineHopMeta oneTwo = new PipelineHopMeta( one, two );
    previewMeta.addPipelineHop( oneTwo );

    StepErrorMeta errMeta = new StepErrorMeta( parent, one, err );
    errMeta.setEnabled( true );

    errMeta.setNrErrorsValuename( NUMBER_ERRORS_FIELD );
    errMeta.setErrorDescriptionsValuename( ERROR_DESC_FIELD );
    errMeta.setErrorFieldsValuename( ERROR_FIELD_VALUE );
    errMeta.setErrorCodesValuename( ERROR_CODE_VALUE );

    one.setStepErrorMeta( errMeta );
    PipelineHopMeta oneErr = new PipelineHopMeta( one, err );
    previewMeta.addPipelineHop( oneErr );

    return previewMeta;
  }

  public static List<RowMetaAndData> executeTestPipeline( PipelineMeta pipelineMeta,
                                                          String testStepname, List<RowMetaAndData> inputData ) throws HopException {
    return executeTestPipeline( pipelineMeta, INJECTOR_STEPNAME, testStepname, DUMMY_STEPNAME, inputData );
  }

  public static List<RowMetaAndData> executeTestPipeline( PipelineMeta pipelineMeta, String injectorStepname,
                                                          String testStepname, String dummyStepname, List<RowMetaAndData> inputData ) throws HopException {
    return executeTestPipeline( pipelineMeta, injectorStepname, testStepname,
      dummyStepname, inputData, null, null );
  }

  public static List<RowMetaAndData> executeTestPipeline( PipelineMeta pipelineMeta, String injectorStepname,
                                                          String testStepname, String dummyStepname, List<RowMetaAndData> inputData,
                                                          VariableSpace runTimeVariables, VariableSpace runTimeParameters ) throws HopException {
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

    // Capture the rows that come out of the dummy step...
    //
    StepInterface si = pipeline.getStepInterface( dummyStepname, 0 );
    RowStepCollector dummyRc = new RowStepCollector();
    si.addRowListener( dummyRc );

    // Add a row producer...
    //
    RowProducer rp = pipeline.addRowProducer( injectorStepname, 0 );

    // Start the steps...
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

    // Return the result from the dummy step...
    //
    return dummyRc.getRowsRead();
  }

  public static Map<String, RowStepCollector> executeTestPipelineError( PipelineMeta pipelineMeta, String testStepname,
                                                                        List<RowMetaAndData> inputData ) throws HopException {
    return executeTestPipelineError( pipelineMeta, INJECTOR_STEPNAME, testStepname, DUMMY_STEPNAME, ERROR_STEPNAME,
      inputData );
  }

  public static Map<String, RowStepCollector> executeTestPipelineError( PipelineMeta pipelineMeta,
                                                                        String injectorStepname, String testStepname, String dummyStepname, String errorStepName,
                                                                        List<RowMetaAndData> inputData ) throws HopException {
    // Now execute the pipeline...
    Pipeline pipeline = new Pipeline( pipelineMeta );

    pipeline.prepareExecution();

    // Capture the rows that come out of the dummy step...
    //
    StepInterface si = pipeline.getStepInterface( dummyStepname, 0 );
    RowStepCollector dummyRc = new RowStepCollector();
    si.addRowListener( dummyRc );

    StepInterface junit = pipeline.getStepInterface( testStepname, 0 );
    RowStepCollector dummyJu = new RowStepCollector();
    junit.addRowListener( dummyJu );

    // add error handler
    StepInterface er = pipeline.getStepInterface( errorStepName, 0 );
    RowStepCollector erColl = new RowStepCollector();
    er.addRowListener( erColl );

    // Add a row producer...
    //
    RowProducer rp = pipeline.addRowProducer( injectorStepname, 0 );

    // Start the steps...
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

    // Return the result from the dummy step...
    Map<String, RowStepCollector> ret = new HashMap<String, RowStepCollector>();
    ret.put( dummyStepname, dummyRc );
    ret.put( errorStepName, erColl );
    ret.put( testStepname, dummyJu );
    return ret;
  }

  static StepMeta getInjectorStepMeta() {
    return getInjectorStepMeta( null );
  }

  static StepMeta getInjectorStepMeta( RowMetaInterface outputRowMeta ) {
    InjectorMeta zeroMeta = new InjectorMeta();

    // Sets output fields for cases when no rows are sent to the test step, but metadata is still needed
    if ( outputRowMeta != null && outputRowMeta.size() > 0 ) {
      String[] fieldName = new String[ outputRowMeta.size() ];
      int[] fieldLength = new int[ outputRowMeta.size() ];
      int[] fieldPrecision = new int[ outputRowMeta.size() ];
      int[] fieldType = new int[ outputRowMeta.size() ];
      for ( int i = 0; i < outputRowMeta.size(); i++ ) {
        ValueMetaInterface field = outputRowMeta.getValueMeta( i );
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

    StepMeta zero = new StepMeta( registry.getPluginId( StepPluginType.class, zeroMeta ), INJECTOR_STEPNAME, zeroMeta );
    zero.setLocation( 50, 50 );

    return zero;
  }

  static StepMeta getReadStepMeta( String name ) {
    DummyMeta twoMeta = new DummyMeta();
    StepMeta two = new StepMeta( registry.getPluginId( StepPluginType.class, twoMeta ), name, twoMeta );
    two.setLocation( 250, 50 );
    return two;
  }

  static StepMeta getReadStepMeta() {
    return getReadStepMeta( DUMMY_STEPNAME );
  }

}
