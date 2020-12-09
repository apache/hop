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

package org.apache.hop.pipeline.transforms.jsonoutput;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.hop.TestUtilities;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.rowgenerator.RowGeneratorMeta;
import org.apache.hop.pipeline.transforms.TransformRowsCollector;
import org.json.simple.JSONObject;
import org.junit.Assert;

import java.io.File;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This class was a "copy and modification" of Hop's JsonOutputTests.
 * 
 * @author Hendy Irawan <hendy@soluvas.com> Modified by Sean Flatley, removing dependency on external text file to hold
 *         expected results and modifying code to handle "Compatibility Mode".
 */
public class JsonOutputTest extends TestCase {

  private static final String EXPECTED_NON_COMPATIBILITY_JSON =
      "{\"data\":[{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"},"
          + "{\"id\":1,\"state\":\"Florida\",\"city\":\"Orlando\"}]}";

  private static final String EXPECTED_COMPATIBILITY_MODE_JSON =
      "{\"data\":[{\"id\":1},{\"state\":\"Florida\"},{\"city\":\"Orlando\"},{\"id\":1},{\"state\":\"Florida\"},"
          + "{\"city\":\"Orlando\"},{\"id\":1},{\"state\":\"Florida\"},{\"city\":\"Orlando\"},{\"id\":1},"
          + "{\"state\":\"Florida\"},{\"city\":\"Orlando\"},{\"id\":1},{\"state\":\"Florida\"},"
          + "{\"city\":\"Orlando\"},{\"id\":1},{\"state\":\"Florida\"},{\"city\":\"Orlando\"},{\"id\":1},"
          + "{\"state\":\"Florida\"},{\"city\":\"Orlando\"},{\"id\":1},{\"state\":\"Florida\"},"
          + "{\"city\":\"Orlando\"},{\"id\":1},{\"state\":\"Florida\"},{\"city\":\"Orlando\"},{\"id\":1},"
          + "{\"state\":\"Florida\"},{\"city\":\"Orlando\"}]}";

  /**
   * Creates a row generator transform for this class..
   * 
   * @param name
   * @param registry
   * @return
   */
  private TransformMeta createRowGeneratorTransform( String name, PluginRegistry registry ) {

    // Default the name if it is empty
    String testFileOutputName = ( Utils.isEmpty( name ) ? "generate rows" : name );

    // create the RowGenerator and Transform Meta
    RowGeneratorMeta rowGeneratorMeta = new RowGeneratorMeta();
    String rowGeneratorPid = registry.getPluginId( TransformPluginType.class, rowGeneratorMeta );
    TransformMeta generateRowsTransform = new TransformMeta( rowGeneratorPid, testFileOutputName, rowGeneratorMeta );

    // Set the field names, types and values
    rowGeneratorMeta.setFieldName( new String[] { "Id", "State", "City" } );
    rowGeneratorMeta.setFieldType( new String[] { "Integer", "String", "String" } );
    rowGeneratorMeta.setValue( new String[] { "1", "Florida", "Orlando" } );
    rowGeneratorMeta.setFieldLength( new int[] { -1, -1, -1 } );
    rowGeneratorMeta.setFieldPrecision( new int[] { -1, -1, -1 } );
    rowGeneratorMeta.setGroup( new String[] { "", "", "" } );
    rowGeneratorMeta.setDecimal( new String[] { "", "", "" } );
    rowGeneratorMeta.setCurrency( new String[] { "", "", "" } );
    rowGeneratorMeta.setFieldFormat( new String[] { "", "", "" } );
    rowGeneratorMeta.setRowLimit( "10" );

    // return the transform meta
    return generateRowsTransform;
  }

  /**
   * Create a dummy transform for this class.
   * 
   * @param name
   * @param registry
   * @return
   */
  private TransformMeta createDummyTransform( String name, PluginRegistry registry ) {
    // Create a dummy transform 1 and add it to the tranMeta
    String dummyTransformName = "dummy transform";
    DummyMeta dm1 = new DummyMeta();
    String dummyPid1 = registry.getPluginId( TransformPluginType.class, dm1 );
    TransformMeta dummyTransform = new TransformMeta( dummyPid1, dummyTransformName, dm1 );

    return dummyTransform;
  }

  /**
   * Create result data for test case 1. Each Object array in element in list should mirror the data written by the row
   * generator created by the createRowGenerator method.
   * 
   * @return list of metadata/data couples of how the result should look like.
   */
  public List<RowMetaAndData> createResultData1() {
    List<RowMetaAndData> list = new ArrayList<>();

    IRowMeta rowMetaInterface = createResultRowMeta();

    Object[] r1 = new Object[] { new Long( 1L ), "Orlando", "Florida" };
    Object[] r2 = new Object[] { new Long( 1L ), "Orlando", "Florida" };
    Object[] r3 = new Object[] { new Long( 1L ), "Orlando", "Florida" };
    Object[] r4 = new Object[] { new Long( 1L ), "Orlando", "Florida" };
    Object[] r5 = new Object[] { new Long( 1L ), "Orlando", "Florida" };
    Object[] r6 = new Object[] { new Long( 1L ), "Orlando", "Florida" };
    Object[] r7 = new Object[] { new Long( 1L ), "Orlando", "Florida" };
    Object[] r8 = new Object[] { new Long( 1L ), "Orlando", "Florida" };
    Object[] r9 = new Object[] { new Long( 1L ), "Orlando", "Florida" };
    Object[] r10 = new Object[] { new Long( 1L ), "Orlando", "Florida" };

    list.add( new RowMetaAndData( rowMetaInterface, r1 ) );
    list.add( new RowMetaAndData( rowMetaInterface, r2 ) );
    list.add( new RowMetaAndData( rowMetaInterface, r3 ) );
    list.add( new RowMetaAndData( rowMetaInterface, r4 ) );
    list.add( new RowMetaAndData( rowMetaInterface, r5 ) );
    list.add( new RowMetaAndData( rowMetaInterface, r6 ) );
    list.add( new RowMetaAndData( rowMetaInterface, r7 ) );
    list.add( new RowMetaAndData( rowMetaInterface, r8 ) );
    list.add( new RowMetaAndData( rowMetaInterface, r9 ) );
    list.add( new RowMetaAndData( rowMetaInterface, r10 ) );
    return list;
  }

  /**
   * Creates a IRowMeta with a IValueMeta with the name "filename".
   * 
   * @return
   */
  public IRowMeta createIRowMeta() {
    IRowMeta rowMetaInterface = new RowMeta();

    IValueMeta[] valuesMeta = { new ValueMetaString( "filename" ), };
    for ( int i = 0; i < valuesMeta.length; i++ ) {
      rowMetaInterface.addValueMeta( valuesMeta[i] );
    }

    return rowMetaInterface;
  }

  /**
   * Creates data... Will add more as I figure what the data is.
   * 
   * @return
   */
  public List<RowMetaAndData> createData() {
    List<RowMetaAndData> list = new ArrayList<>();
    IRowMeta rowMetaInterface = createIRowMeta();
    Object[] r1 = new Object[] {};
    list.add( new RowMetaAndData( rowMetaInterface, r1 ) );
    return list;
  }

  /**
   * Creates a row meta interface for the fields that are defined by performing a getFields and by checking "Result
   * filenames - Add filenames to result from "Text File Input" dialog.
   * 
   * @return
   */
  public IRowMeta createResultRowMeta() {
    IRowMeta rowMetaInterface = new RowMeta();

    IValueMeta[] valuesMeta =
      { new ValueMetaInteger( "Id" ), new ValueMetaString( "State" ), new ValueMetaString( "City" ) };

    for ( int i = 0; i < valuesMeta.length; i++ ) {
      rowMetaInterface.addValueMeta( valuesMeta[i] );
    }

    return rowMetaInterface;
  }

  private TransformMeta createJsonOutputTransform( String name, String jsonFileName, PluginRegistry registry ) {

    // Create a Text File Output transform
    String testFileOutputName = name;
    JsonOutputMeta jsonOutputMeta = new JsonOutputMeta();
    String textFileInputPid = registry.getPluginId( TransformPluginType.class, jsonOutputMeta );
    TransformMeta jsonOutputTransform = new TransformMeta( textFileInputPid, testFileOutputName, jsonOutputMeta );

    // initialize the fields
    JsonOutputField[] fields = new JsonOutputField[3];
    for ( int idx = 0; idx < fields.length; idx++ ) {
      fields[idx] = new JsonOutputField();
    }

    // populate the fields
    // it is important that the setPosition(int)
    // is invoked with the correct position as
    // we are testing the reading of a delimited file.
    fields[0].setFieldName( "id" );
    fields[0].setElementName( "id" );

    fields[1].setFieldName( "state" );
    fields[1].setElementName( "state" );

    fields[2].setFieldName( "city" );
    fields[2].setElementName( "city" );

    // call this to allocate the number of fields
    jsonOutputMeta.allocate( fields.length );
    jsonOutputMeta.setOutputFields( fields );

    // set meta properties- these were determined by running Spoon
    // and setting up the transformation we are setting up here.
    // i.e. - the dialog told me what I had to set to avoid
    // NPEs during the transformation.

    // We need a file name so we will generate a temp file
    jsonOutputMeta.setOperationType( JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE );
    jsonOutputMeta.setOutputValue( "data" );
    jsonOutputMeta.setFileName( jsonFileName );
    jsonOutputMeta.setExtension( "js" );
    jsonOutputMeta.setNrRowsInBloc( "0" ); // a single "data" contains an array of all records
    jsonOutputMeta.setJsonBloc( "data" );

    return jsonOutputTransform;
  }

  public String test( boolean compatibilityMode ) throws Exception {
    HopEnvironment.init();

    // Create a new transformation...
    //
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "testJsonOutput" );
    PluginRegistry registry = PluginRegistry.getInstance();

    // create an injector transform
    String injectorTransformName = "injector transform";
    TransformMeta injectorTransform = TestUtilities.createInjectorTransform( injectorTransformName, registry );
    pipelineMeta.addTransform( injectorTransform );

    // create a row generator transform
    TransformMeta rowGeneratorTransform = createRowGeneratorTransform( "Create rows for testJsonOutput1", registry );
    pipelineMeta.addTransform( rowGeneratorTransform );

    // create a PipelineHopMeta for injector and add it to the pipelineMeta
    PipelineHopMeta hop_injectoryRowGenerator = new PipelineHopMeta( injectorTransform, rowGeneratorTransform );
    pipelineMeta.addPipelineHop( hop_injectoryRowGenerator );

    // create the json output transform
    // but first lets get a filename
    String jsonFileName = TestUtilities.createEmptyTempFile( "testJsonOutput1_" );
    TransformMeta jsonOutputTransform = createJsonOutputTransform( "json output transform", jsonFileName, registry );
    ( (JsonOutputMeta) jsonOutputTransform.getTransform() ).setCompatibilityMode( compatibilityMode );
    pipelineMeta.addTransform( jsonOutputTransform );

    // create a PipelineHopMeta for jsonOutputTransform and add it to the pipelineMeta
    PipelineHopMeta hop_RowGeneratorOutputTextFile = new PipelineHopMeta( rowGeneratorTransform, jsonOutputTransform );
    pipelineMeta.addPipelineHop( hop_RowGeneratorOutputTextFile );

    // Create a dummy transform and add it to the tranMeta
    String dummyTransformName = "dummy transform";
    TransformMeta dummyTransform = createDummyTransform( dummyTransformName, registry );
    pipelineMeta.addTransform( dummyTransform );

    // create a PipelineHopMeta for the
    PipelineHopMeta hopOutputJson_dummyTransform = new PipelineHopMeta( jsonOutputTransform, dummyTransform );
    pipelineMeta.addPipelineHop( hopOutputJson_dummyTransform );

    // Now execute the transformation...
    Pipeline pipeline = new LocalPipelineEngine( pipelineMeta );
    pipeline.prepareExecution();

    // Create a row collector and add it to the dummy transform interface
    IEngineComponent dummyITransform = pipeline.findComponent( dummyTransformName, 0 );
    TransformRowsCollector dummyRowCollector = new TransformRowsCollector();
    dummyITransform.addRowListener( dummyRowCollector );

    // RowProducer rowProducer = pipeline.addRowProducer(injectorTransformName, 0);
    pipeline.startThreads();
    pipeline.waitUntilFinished();

    // get the results and return it
    File outputFile = new File( jsonFileName + ".js" );
    String jsonStructure = FileUtils.readFileToString( outputFile );

    return jsonStructure;
  }

  // The actual tests

  public void testNonCompatibilityMode() throws Exception {
    String jsonStructure = test( false );
    Assert.assertTrue( jsonEquals( EXPECTED_NON_COMPATIBILITY_JSON, jsonStructure ) );
  }

  public void testCompatibilityMode() throws Exception {
    String jsonStructure = test( true );
    Assert.assertEquals( EXPECTED_COMPATIBILITY_MODE_JSON, jsonStructure );
  }

  /* PDI-7243 */
  public void testNpeIsNotThrownOnNullInput() throws Exception {
    TransformMockHelper<JsonOutputMeta, JsonOutputData> mockHelper =
      new TransformMockHelper<>( "jsonOutput", JsonOutputMeta.class, JsonOutputData.class );
    when( mockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
        mockHelper.iLogChannel );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );
    when( mockHelper.transformMeta.getTransform() ).thenReturn( new JsonOutputMeta() );

    JsonOutput transform =
        new JsonOutput( mockHelper.transformMeta, mockHelper.iTransformMeta, mockHelper.iTransformData, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    transform = spy( transform );

    doReturn( null ).when( transform ).getRow();

    transform.processRow();
  }

  public void testEmptyDoesntWriteToFile() throws Exception {
    TransformMockHelper<JsonOutputMeta, JsonOutputData> mockHelper =
      new TransformMockHelper<>( "jsonOutput", JsonOutputMeta.class, JsonOutputData.class );
    when( mockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
            mockHelper.iLogChannel );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );
    when( mockHelper.transformMeta.getTransform() ).thenReturn( new JsonOutputMeta() );

    JsonOutputData transformData = new JsonOutputData();
    transformData.writeToFile = true;
    JsonOutput transform =
            new JsonOutput( mockHelper.transformMeta, mockHelper.iTransformMeta, transformData, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    transform = spy( transform );

    doReturn( null ).when( transform ).getRow();
    doReturn( true ).when( transform ).openNewFile();
    doReturn( true ).when( transform ).closeFile();

    transform.processRow();
    verify( transform, times( 0 ) ).openNewFile();
    verify( transform, times( 0 ) ).closeFile();
  }

  @SuppressWarnings( "unchecked" )
  public void testWriteToFile() throws Exception {
    TransformMockHelper<JsonOutputMeta, JsonOutputData> mockHelper =
      new TransformMockHelper<>( "jsonOutput", JsonOutputMeta.class, JsonOutputData.class );
    when( mockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
            mockHelper.iLogChannel );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );
    when( mockHelper.transformMeta.getTransform() ).thenReturn( new JsonOutputMeta() );

    JsonOutputData transformData = new JsonOutputData();
    transformData.writeToFile = true;
    JSONObject jsonObject = new JSONObject();
    jsonObject.put( "key", "value" );
    transformData.ja.add( jsonObject );
    transformData.writer = mock( Writer.class );

    JsonOutput transform = new JsonOutput( mockHelper.transformMeta, mockHelper.iTransformMeta, transformData, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    transform = spy( transform );

    doReturn( null ).when( transform ).getRow();
    doReturn( true ).when( transform ).openNewFile();
    doReturn( true ).when( transform ).closeFile();
    doNothing().when( transformData.writer ).write( anyString() );

    transform.processRow();
    verify( transform ).openNewFile();
    verify( transform ).closeFile();
  }

  /**
   * compare json (deep equals ignoring order)
   */
  protected boolean jsonEquals( String json1, String json2 ) throws Exception {
    ObjectMapper om = new ObjectMapper();
    JsonNode parsedJson1 = om.readTree( json1 );
    JsonNode parsedJson2 = om.readTree( json2 );
    return parsedJson1.equals( parsedJson2 );
  }
}
