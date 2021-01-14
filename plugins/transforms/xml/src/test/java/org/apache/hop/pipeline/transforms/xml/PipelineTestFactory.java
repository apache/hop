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

package org.apache.hop.pipeline.transforms.xml;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
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
 * We can use this factory to create transformations with a source and target transform.<br>
 * The source transform is an Injector transform.<br>
 * The target transform is a dummy transform.<br>
 * The middle transform is the transform specified.<br>
 * 
 *
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

  public static PipelineMeta generateTestTransformation(IVariables parent, ITransformMeta oneMeta,
                                                        String oneTransformName ) {
    PipelineMeta previewMeta = new PipelineMeta();

    // First the injector transform...
    TransformMeta zero = getInjectorTransformMeta();
    previewMeta.addTransform( zero );

    // Then the middle transform to test...
    //
    TransformMeta one = new TransformMeta( registry.getPluginId( TransformPluginType.class, oneMeta ), oneTransformName, oneMeta );
    one.setLocation( 150, 50 );
//    one.setDraw( true );
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

  public static PipelineMeta generateTestTransformationError( IVariables parent, ITransformMeta oneMeta,
      String oneTransformName ) {
    PipelineMeta previewMeta = new PipelineMeta();

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
//    one.setDraw( true );
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

    TransformErrorMeta errMeta = new TransformErrorMeta( one, err );
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

  public static List<RowMetaAndData> executeTestTransformation(PipelineMeta pipelineMeta, String injectorTransformName,
                                                               String testTransformName, String dummyTransformName, List<RowMetaAndData> inputData ) throws HopException {
    // Now execute the transformation...
    Pipeline trans = new LocalPipelineEngine( pipelineMeta );

    trans.prepareExecution(  );

    // Capture the rows that come out of the dummy transform...
    //
    ITransform si = trans.getTransformInterface( dummyTransformName, 0 );
    RowTransformCollector dummyRc = new RowTransformCollector();
    si.addRowListener( dummyRc );

    // Add a row producer...
    //
    RowProducer rp = trans.addRowProducer( injectorTransformName, 0 );

    // Start the transforms...
    //
    trans.startThreads();

    // Inject the actual test rows...
    //
    List<RowMetaAndData> inputList = inputData;
    Iterator<RowMetaAndData> it = inputList.iterator();
    while ( it.hasNext() ) {
      RowMetaAndData rm = it.next();
      rp.putRow( rm.getRowMeta(), rm.getData() );
    }
    rp.finished();

    // Wait until the transformation is finished...
    //
    trans.waitUntilFinished();

    // If there is an error in the result, throw an exception here...
    //
    if ( trans.getResult().getNrErrors() > 0 ) {
      throw new HopException( "Test transformation finished with errors. Check the log." );
    }

    // Return the result from the dummy transform...
    //
    return dummyRc.getRowsRead();
  }

  public static Map<String, RowTransformCollector> executeTestTransformationError( PipelineMeta pipelineMeta, String testTransformName,
      List<RowMetaAndData> inputData ) throws HopException {
    return executeTestTransformationError( pipelineMeta, INJECTOR_TRANSFORMNAME, testTransformName, DUMMY_TRANSFORMNAME, ERROR_TRANSFORMNAME,
        inputData );
  }

  public static Map<String, RowTransformCollector> executeTestTransformationError( PipelineMeta pipelineMeta,
      String injectorTransformName, String testTransformName, String dummyTransformName, String errorTransformName,
      List<RowMetaAndData> inputData ) throws HopException {
    // Now execute the transformation...
    Pipeline trans = new LocalPipelineEngine( pipelineMeta );

    trans.prepareExecution(  );

    // Capture the rows that come out of the dummy transform...
    //
    ITransform si = trans.getTransformInterface( dummyTransformName, 0 );
    RowTransformCollector dummyRc = new RowTransformCollector();
    si.addRowListener( dummyRc );

    ITransform junit = trans.getTransformInterface( testTransformName, 0 );
    RowTransformCollector dummyJu = new RowTransformCollector();
    junit.addRowListener( dummyJu );

    // add error handler
    ITransform er = trans.getTransformInterface( errorTransformName, 0 );
    RowTransformCollector erColl = new RowTransformCollector();
    er.addRowListener( erColl );

    // Add a row producer...
    //
    RowProducer rp = trans.addRowProducer( injectorTransformName, 0 );

    // Start the transforms...
    //
    trans.startThreads();

    // Inject the actual test rows...
    //
    List<RowMetaAndData> inputList = inputData;
    Iterator<RowMetaAndData> it = inputList.iterator();
    while ( it.hasNext() ) {
      RowMetaAndData rm = it.next();
      rp.putRow( rm.getRowMeta(), rm.getData() );
    }
    rp.finished();

    // Wait until the transformation is finished...
    //
    trans.waitUntilFinished();

    // If there is an error in the result, throw an exception here...
    //
    if ( trans.getResult().getNrErrors() > 0 ) {
      throw new HopException( "Test transformation finished with errors. Check the log." );
    }

    // Return the result from the dummy transform...
    Map<String, RowTransformCollector> ret = new HashMap<>();
    ret.put( dummyTransformName, dummyRc );
    ret.put( errorTransformName, erColl );
    ret.put( testTransformName, dummyJu );
    return ret;
  }

  static TransformMeta getInjectorTransformMeta() {
    InjectorMeta zeroMeta = new InjectorMeta();
    TransformMeta zero = new TransformMeta( registry.getPluginId( TransformPluginType.class, zeroMeta ), INJECTOR_TRANSFORMNAME, zeroMeta );
    zero.setLocation( 50, 50 );
    //zero.setDraw( true );
    return zero;
  }

  static TransformMeta getReadTransformMeta( String name ) {
    DummyMeta twoMeta = new DummyMeta();
    TransformMeta two = new TransformMeta( registry.getPluginId( TransformPluginType.class, twoMeta ), name, twoMeta );
    two.setLocation( 250, 50 );
//    two.setDraw( true );
    return two;
  }

  static TransformMeta getReadTransformMeta() {
    return getReadTransformMeta( DUMMY_TRANSFORMNAME );
  }

}
