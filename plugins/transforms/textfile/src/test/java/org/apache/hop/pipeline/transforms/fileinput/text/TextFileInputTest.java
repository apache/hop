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

package org.apache.hop.pipeline.transforms.fileinput.text;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.playlist.FilePlayListAll;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Assert;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.AbstractFileErrorHandler;
import org.apache.hop.pipeline.transform.errorhandling.IFileErrorHandler;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.apache.hop.pipeline.transforms.file.IBaseFileInputReader;
import org.apache.hop.pipeline.transforms.file.IBaseFileInputTransformControl;
import org.apache.hop.ui.pipeline.transform.common.TextFileLineUtil;
import org.apache.hop.utils.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TextFileInputTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  private static InputStreamReader getInputStreamReader( String data ) throws UnsupportedEncodingException {
    return new InputStreamReader( new ByteArrayInputStream( data.getBytes( ( "UTF-8" ) ) ) );
  }

  @Test
  public void testGetLineDOS() throws HopFileException, UnsupportedEncodingException {
    String input = "col1\tcol2\tcol3\r\ndata1\tdata2\tdata3\r\n";
    String expected = "col1\tcol2\tcol3";
    String output = TextFileLineUtil.getLine( null, getInputStreamReader( input ), TextFileLineUtil.FILE_FORMAT_DOS, new StringBuilder( 1000 ) );
    assertEquals( expected, output );
  }

  @Test
  public void testGetLineUnix() throws HopFileException, UnsupportedEncodingException {
    String input = "col1\tcol2\tcol3\ndata1\tdata2\tdata3\n";
    String expected = "col1\tcol2\tcol3";
    String output = TextFileLineUtil.getLine( null, getInputStreamReader( input ), TextFileLineUtil.FILE_FORMAT_UNIX, new StringBuilder( 1000 ) );
    assertEquals( expected, output );
  }

  @Test
  public void testGetLineOSX() throws HopFileException, UnsupportedEncodingException {
    String input = "col1\tcol2\tcol3\rdata1\tdata2\tdata3\r";
    String expected = "col1\tcol2\tcol3";
    String output = TextFileLineUtil.getLine( null, getInputStreamReader( input ), TextFileLineUtil.FILE_FORMAT_UNIX, new StringBuilder( 1000 ) );
    assertEquals( expected, output );
  }

  @Test
  public void testGetLineMixed() throws HopFileException, UnsupportedEncodingException {
    String input = "col1\tcol2\tcol3\r\ndata1\tdata2\tdata3\r";
    String expected = "col1\tcol2\tcol3";
    String output = TextFileLineUtil.getLine( null, getInputStreamReader( input ), TextFileLineUtil.FILE_FORMAT_MIXED, new StringBuilder( 1000 ) );
    assertEquals( expected, output );
  }

  @Test( timeout = 100 )
  public void test_PDI695() throws HopFileException, UnsupportedEncodingException {
    String inputDOS = "col1\tcol2\tcol3\r\ndata1\tdata2\tdata3\r\n";
    String inputUnix = "col1\tcol2\tcol3\ndata1\tdata2\tdata3\n";
    String inputOSX = "col1\tcol2\tcol3\rdata1\tdata2\tdata3\r";
    String expected = "col1\tcol2\tcol3";

    assertEquals( expected, TextFileLineUtil.getLine( null, getInputStreamReader( inputDOS ),
      TextFileLineUtil.FILE_FORMAT_UNIX, new StringBuilder( 1000 ) ) );
    assertEquals( expected, TextFileLineUtil.getLine( null, getInputStreamReader( inputUnix ),
      TextFileLineUtil.FILE_FORMAT_UNIX, new StringBuilder( 1000 ) ) );
    assertEquals( expected, TextFileLineUtil.getLine( null, getInputStreamReader( inputOSX ),
      TextFileLineUtil.FILE_FORMAT_UNIX, new StringBuilder( 1000 ) ) );
  }

  @Test
  public void readWrappedInputWithoutHeaders() throws Exception {
    final String content = new StringBuilder()
      .append( "r1c1" ).append( '\n' ).append( ";r1c2\n" )
      .append( "r2c1" ).append( '\n' ).append( ";r2c2" )
      .toString();
    final String virtualFile = createVirtualFile( "pdi-2607.txt", content );

    TextFileInputMeta meta = createMetaObject( field( "col1" ), field( "col2" ) );
    meta.content.lineWrapped = true;
    meta.content.nrWraps = 1;

    TextFileInputData data = createDataObject( virtualFile, ";", "col1", "col2" );

    TextFileInput input = TransformMockUtil.getTransform( TextFileInput.class, meta, data, TextFileInputMeta.class, TextFileInputData.class, "test" );
    List<Object[]> output = PipelineTestingUtil.execute( input, 2, false );
    PipelineTestingUtil.assertResult( new Object[] { "r1c1", "r1c2" }, output.get( 0 ) );
    PipelineTestingUtil.assertResult( new Object[] { "r2c1", "r2c2" }, output.get( 1 ) );

    deleteVfsFile( virtualFile );
  }

  @Test
  public void readInputWithMissedValues() throws Exception {
    final String virtualFile = createVirtualFile( "pdi-14172.txt", "1,1,1\n", "2,,2\n" );

    BaseFileField field2 = field( "col2" );
    field2.setRepeated( true );

    TextFileInputMeta meta = createMetaObject( field( "col1" ), field2, field( "col3" ) );
    TextFileInputData data = createDataObject( virtualFile, ",", "col1", "col2", "col3" );

    TextFileInput input = TransformMockUtil.getTransform( TextFileInput.class, meta, data, TextFileInputMeta.class, TextFileInputData.class, "test" );
    List<Object[]> output = PipelineTestingUtil.execute( input, 2, false );
    PipelineTestingUtil.assertResult( new Object[] { "1", "1", "1" }, output.get( 0 ) );
    PipelineTestingUtil.assertResult( new Object[] { "2", "1", "2" }, output.get( 1 ) );

    deleteVfsFile( virtualFile );
  }

  @Test
  public void readInputWithNonEmptyNullif() throws Exception {
    final String virtualFile = createVirtualFile( "pdi-14358.txt", "-,-\n" );

    BaseFileField col2 = field( "col2" );
    col2.setNullString( "-" );

    TextFileInputMeta meta = createMetaObject( field( "col1" ), col2 );
    TextFileInputData data = createDataObject( virtualFile, ",", "col1", "col2" );

    TextFileInput input = TransformMockUtil.getTransform( TextFileInput.class, meta, data, TextFileInputMeta.class, TextFileInputData.class, "test" );

    List<Object[]> output = PipelineTestingUtil.execute( input, 1, false );
    PipelineTestingUtil.assertResult( new Object[] { "-" }, output.get( 0 ) );

    deleteVfsFile( virtualFile );
  }

  @Test
  public void readInputWithDefaultValues() throws Exception {
    final String virtualFile = createVirtualFile( "pdi-14832.txt", "1,\n" );

    BaseFileField col2 = field( "col2" );
    col2.setIfNullValue( "DEFAULT" );

    TextFileInputMeta meta = createMetaObject( field( "col1" ), col2 );
    TextFileInputData data = createDataObject( virtualFile, ",", "col1", "col2" );

    TextFileInput input = TransformMockUtil.getTransform( TextFileInput.class, meta, data, TextFileInputMeta.class, TextFileInputData.class, "test" );

    List<Object[]> output = PipelineTestingUtil.execute( input, 1, false );
    PipelineTestingUtil.assertResult( new Object[] { "1", "DEFAULT" }, output.get( 0 ) );

    deleteVfsFile( virtualFile );
  }

  @Test
  public void testErrorHandlerLineNumber() throws Exception {
    final String content = new StringBuilder()
      .append( "123" ).append( '\n' ).append( "333\n" )
      .append( "345" ).append( '\n' ).append( "773\n" )
      .append( "aaa" ).append( '\n' ).append( "444" )
      .toString();
    final String virtualFile = createVirtualFile( "pdi-2607.txt", content );

    TextFileInputMeta meta = createMetaObject( field( "col1" ) );

    meta.inputFields[ 0 ].setType( 1 );
    meta.content.lineWrapped = false;
    meta.content.nrWraps = 1;
    meta.errorHandling.errorIgnored = true;
    TextFileInputData data = createDataObject( virtualFile, ";", "col1" );
    data.dataErrorLineHandler = Mockito.mock( IFileErrorHandler.class );

    TextFileInput input = TransformMockUtil.getTransform( TextFileInput.class, meta, data, TextFileInputMeta.class, TextFileInputData.class, "test" );

    List<Object[]> output = PipelineTestingUtil.execute( input, 4, false );

    Mockito.verify( data.dataErrorLineHandler ).handleLineError( 4, AbstractFileErrorHandler.NO_PARTS );
    input.dispose(); // close file
    deleteVfsFile( virtualFile );
  }

  @Test
  public void testHandleOpenFileException() throws Exception {
    final String content = new StringBuilder()
      .append( "123" ).append( '\n' ).append( "333\n" ).toString();
    final String virtualFile = createVirtualFile( "pdi-16697.txt", content );

    TextFileInputMeta meta = createMetaObject( field( "col1" ) );

    meta.inputFields[ 0 ].setType( 1 );
    meta.errorHandling.errorIgnored = true;
    meta.errorHandling.skipBadFiles = true;

    TextFileInputData data = createDataObject( virtualFile, ";", "col1" );
    data.dataErrorLineHandler = Mockito.mock( IFileErrorHandler.class );

    TestTextFileInput textFileInput = Mockito.spy( TransformMockUtil.getTransform( TestTextFileInput.class, meta, data, TextFileInputMeta.class, TextFileInputData.class, "test" ) );
    TransformMeta transformMeta = textFileInput.getTransformMeta();
    Mockito.doReturn( true ).when( transformMeta ).isDoingErrorHandling();

    List<Object[]> output = PipelineTestingUtil.execute( textFileInput, 0, false );

    deleteVfsFile( virtualFile );

    assertEquals( 1, data.rejectedFiles.size() );
    assertEquals( 0, textFileInput.getErrors() );
  }

  @Test
  public void test_PDI17117() throws Exception {
    final String virtualFile = createVirtualFile( "pdi-14832.txt", "1,\n" );

    BaseFileField col2 = field( "col2" );
    col2.setIfNullValue( "DEFAULT" );

    TextFileInputMeta meta = createMetaObject( field( "col1" ), col2 );

    meta.inputFiles.passingThruFields = true;
    meta.inputFiles.acceptingFilenames = true;
    TextFileInputData data = createDataObject( virtualFile, ",", "col1", "col2" );

    TextFileInput input = Mockito.spy( TransformMockUtil.getTransform( TextFileInput.class, meta, data, TextFileInputMeta.class, TextFileInputData.class, "test" ) );

    IRowSet rowset = Mockito.mock( IRowSet.class );
    IRowMeta rwi = Mockito.mock( IRowMeta.class );
    Object[] obj1 = new Object[ 2 ];
    Object[] obj2 = new Object[ 2 ];
    Mockito.doReturn( rowset ).when( input ).findInputRowSet( null );
    Mockito.doReturn( null ).when( input ).getRowFrom( rowset );
    Mockito.when( input.getRowFrom( rowset ) ).thenReturn( obj1, obj2, null );
    Mockito.doReturn( rwi ).when( rowset ).getRowMeta();
    Mockito.when( rwi.getString( obj2, 0 ) ).thenReturn( "filename1", "filename2" );
    List<Object[]> output = PipelineTestingUtil.execute( input, 0, false );

    List<String> passThroughKeys = new ArrayList<>( data.passThruFields.keySet() );
    Assert.assertNotNull( passThroughKeys );
    // set order is not guaranteed - order alphabetically
    passThroughKeys.sort( String.CASE_INSENSITIVE_ORDER );
    assertEquals( 2, passThroughKeys.size() );

    Assert.assertNotNull( passThroughKeys.get( 0 ) );
    Assert.assertTrue( passThroughKeys.get( 0 ).startsWith( "0_file" ) );
    Assert.assertTrue( passThroughKeys.get( 0 ).endsWith( "filename1" ) );

    Assert.assertNotNull( passThroughKeys.get( 1 ) );
    Assert.assertTrue( passThroughKeys.get( 1 ).startsWith( "1_file" ) );
    Assert.assertTrue( passThroughKeys.get( 1 ).endsWith( "filename2" ) );

    deleteVfsFile( virtualFile );
  }

  @Test
  public void testClose() throws Exception {

    TextFileInputMeta mockTFIM = createMetaObject( null );
    String virtualFile = createVirtualFile( "pdi-17267.txt", null );
    TextFileInputData mockTFID = createDataObject( virtualFile, ";", null );
    mockTFID.lineBuffer = new ArrayList<>();
    mockTFID.lineBuffer.add( new TextFileLine( null, 0l, null ) );
    mockTFID.lineBuffer.add( new TextFileLine( null, 0l, null ) );
    mockTFID.lineBuffer.add( new TextFileLine( null, 0l, null ) );
    mockTFID.filename = "";

    FileContent mockFileContent = mock( FileContent.class );
    InputStream mockInputStream = mock( InputStream.class );
    when( mockFileContent.getInputStream() ).thenReturn( mockInputStream );
    FileObject mockFO = mock( FileObject.class );
    when( mockFO.getContent() ).thenReturn( mockFileContent );

    TextFileInputReader tFIR = new TextFileInputReader( mock( IBaseFileInputTransformControl.class ),
      mockTFIM, mockTFID, mockFO, mock( ILogChannel.class ) );

    assertEquals( 3, mockTFID.lineBuffer.size() );
    tFIR.close();
    // After closing the file, the buffer must be empty!
    assertEquals( 0, mockTFID.lineBuffer.size() );
  }

  private TextFileInputMeta createMetaObject( BaseFileField... fields ) {
    TextFileInputMeta meta = new TextFileInputMeta();
    meta.content.fileCompression = "None";
    meta.content.fileType = "CSV";
    meta.content.header = false;
    meta.content.nrHeaderLines = -1;
    meta.content.footer = false;
    meta.content.nrFooterLines = -1;

    meta.inputFields = fields;
    return meta;
  }

  private TextFileInputData createDataObject( String file,
                                              String separator,
                                              String... outputFields ) throws Exception {
    TextFileInputData data = new TextFileInputData();
    data.files = new FileInputList();
    data.files.addFile( HopVfs.getFileObject( file ) );

    data.separator = separator;

    data.outputRowMeta = new RowMeta();
    if ( outputFields != null ) {
      for ( String field : outputFields ) {
        data.outputRowMeta.addValueMeta( new ValueMetaString( field ) );
      }
    }

    data.dataErrorLineHandler = mock( IFileErrorHandler.class );
    data.fileFormatType = TextFileLineUtil.FILE_FORMAT_UNIX;
    data.filterProcessor = new TextFileFilterProcessor( new TextFileFilter[ 0 ], new Variables() );
    data.filePlayList = new FilePlayListAll();
    return data;
  }

  private static String createVirtualFile( String filename, String... rows ) throws Exception {
    String virtualFile = TestUtils.createRamFile( filename );

    StringBuilder content = new StringBuilder();
    if ( rows != null ) {
      for ( String row : rows ) {
        content.append( row );
      }
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    bos.write( content.toString().getBytes() );

    try ( OutputStream os = HopVfs.getFileObject( virtualFile ).getContent().getOutputStream() ) {
      IOUtils.copy( new ByteArrayInputStream( bos.toByteArray() ), os );
    }

    return virtualFile;
  }

  private static void deleteVfsFile( String path ) throws Exception {
    TestUtils.getFileObject( path ).delete();
  }

  private static BaseFileField field( String name ) {
    return new BaseFileField( name, -1, -1 );
  }

  public static class TestTextFileInput extends TextFileInput {
    public TestTextFileInput( TransformMeta transformMeta, TextFileInputMeta meta, TextFileInputData data, int copyNr, PipelineMeta pipelineMeta,
                              Pipeline pipeline ) {
      super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
    }

    @Override
    protected IBaseFileInputReader createReader( TextFileInputMeta meta, TextFileInputData data, FileObject file )
      throws Exception {
      throw new Exception( "Can not create reader for the file object " + file );
    }
  }
}
