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

package org.apache.hop.pipeline.transforms.textfileinput;

import org.apache.commons.io.IOUtils;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.file.IInputFileMeta;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.playlist.FilePlayListAll;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transform.errorhandling.IFileErrorHandler;
import org.apache.hop.pipeline.transforms.fileinput.*;
import org.apache.hop.utils.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @deprecated replaced by implementation in the ...transforms.fileinput.text package
 */
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
    String output = TextFileInput.getLine( null, getInputStreamReader( input ),
      TextFileInputMeta.FILE_FORMAT_DOS, new StringBuilder( 1000 ) );
    assertEquals( expected, output );
  }

  @Test
  public void testGetLineUnix() throws HopFileException, UnsupportedEncodingException {
    String input = "col1\tcol2\tcol3\ndata1\tdata2\tdata3\n";
    String expected = "col1\tcol2\tcol3";
    String output = TextFileInput.getLine( null, getInputStreamReader( input ),
      TextFileInputMeta.FILE_FORMAT_UNIX, new StringBuilder( 1000 ) );
    assertEquals( expected, output );
  }

  @Test
  public void testGetLineOSX() throws HopFileException, UnsupportedEncodingException {
    String input = "col1\tcol2\tcol3\rdata1\tdata2\tdata3\r";
    String expected = "col1\tcol2\tcol3";
    String output = TextFileInput.getLine( null, getInputStreamReader( input ),
      TextFileInputMeta.FILE_FORMAT_UNIX, new StringBuilder( 1000 ) );
    assertEquals( expected, output );
  }

  @Test
  public void testGetLineMixed() throws HopFileException, UnsupportedEncodingException {
    String input = "col1\tcol2\tcol3\r\ndata1\tdata2\tdata3\r";
    String expected = "col1\tcol2\tcol3";
    String output = TextFileInput.getLine( null, getInputStreamReader( input ),
      TextFileInputMeta.FILE_FORMAT_MIXED, new StringBuilder( 1000 ) );
    assertEquals( expected, output );
  }

  @Test( timeout = 100 )
  public void test_PDI695() throws HopFileException, UnsupportedEncodingException {
    String inputDOS = "col1\tcol2\tcol3\r\ndata1\tdata2\tdata3\r\n";
    String inputUnix = "col1\tcol2\tcol3\ndata1\tdata2\tdata3\n";
    String inputOSX = "col1\tcol2\tcol3\rdata1\tdata2\tdata3\r";
    String expected = "col1\tcol2\tcol3";

    assertEquals( expected, TextFileInput.getLine( null, getInputStreamReader( inputDOS ),
      TextFileInputMeta.FILE_FORMAT_UNIX, new StringBuilder( 1000 ) ) );
    assertEquals( expected, TextFileInput.getLine( null, getInputStreamReader( inputUnix ),
      TextFileInputMeta.FILE_FORMAT_UNIX, new StringBuilder( 1000 ) ) );
    assertEquals( expected, TextFileInput.getLine( null, getInputStreamReader( inputOSX ),
      TextFileInputMeta.FILE_FORMAT_UNIX, new StringBuilder( 1000 ) ) );
  }

  @Test
  public void readWrappedInputWithoutHeaders() throws Exception {
    final String content = new StringBuilder()
      .append( "r1c1" ).append( '\n' ).append( ";r1c2\n" )
      .append( "r2c1" ).append( '\n' ).append( ";r2c2" )
      .toString();
    final String virtualFile = createVirtualFile( "pdi-2607.txt", content );

    TextFileInputMeta meta = new TextFileInputMeta();
    meta.setLineWrapped( true );
    meta.setNrWraps( 1 );
    meta.setInputFields( new TextFileInputField[] { field( "col1" ), field( "col2" ) } );
    meta.setFileCompression( "None" );
    meta.setFileType( "CSV" );
    meta.setHeader( false );
    meta.setNrHeaderLines( -1 );
    meta.setFooter( false );
    meta.setNrFooterLines( -1 );

    TextFileInputData data = new TextFileInputData();
    data.setFiles( new FileInputList() );
    data.getFiles().addFile( HopVfs.getFileObject( virtualFile ) );

    data.outputRowMeta = new RowMeta();
    data.outputRowMeta.addValueMeta( new ValueMetaString( "col1" ) );
    data.outputRowMeta.addValueMeta( new ValueMetaString( "col2" ) );

    data.dataErrorLineHandler = Mockito.mock( IFileErrorHandler.class );
    data.fileFormatType = TextFileInputMeta.FILE_FORMAT_UNIX;
    data.separator = ";";
    data.filterProcessor = new TextFileFilterProcessor( new TextFileFilter[ 0 ] );
    data.filePlayList = new FilePlayListAll();

    TextFileInput input = TransformMockUtil.getTransform( TextFileInput.class, meta, data, TextFileInputMeta.class, TextFileInputData.class, "test" );
    List<Object[]> output = PipelineTestingUtil.execute( input, 2, false );
    PipelineTestingUtil.assertResult( new Object[] { "r1c1", "r1c2" }, output.get( 0 ) );
    PipelineTestingUtil.assertResult( new Object[] { "r2c1", "r2c2" }, output.get( 1 ) );

    deleteVfsFile( virtualFile );
  }

  @Test
  public void readInputWithMissedValues() throws Exception {
    final String virtualFile = createVirtualFile( "pdi-14172.txt", "1,1,1\n", "2,,2\n" );

    TextFileInputMeta meta = new TextFileInputMeta();
    TextFileInputField field2 = field( "col2" );
    field2.setRepeated( true );
    meta.setInputFields( new TextFileInputField[] {
      field( "col1" ), field2, field( "col3" )
    } );
    meta.setFileCompression( "None" );
    meta.setFileType( "CSV" );
    meta.setHeader( false );
    meta.setNrHeaderLines( -1 );
    meta.setFooter( false );
    meta.setNrFooterLines( -1 );

    TextFileInputData data = new TextFileInputData();
    data.setFiles( new FileInputList() );
    data.getFiles().addFile( HopVfs.getFileObject( virtualFile ) );

    data.outputRowMeta = new RowMeta();
    data.outputRowMeta.addValueMeta( new ValueMetaString( "col1" ) );
    data.outputRowMeta.addValueMeta( new ValueMetaString( "col2" ) );
    data.outputRowMeta.addValueMeta( new ValueMetaString( "col3" ) );

    data.dataErrorLineHandler = Mockito.mock( IFileErrorHandler.class );
    data.fileFormatType = TextFileInputMeta.FILE_FORMAT_UNIX;
    data.separator = ",";
    data.filterProcessor = new TextFileFilterProcessor( new TextFileFilter[ 0 ] );
    data.filePlayList = new FilePlayListAll();


    TextFileInput input = TransformMockUtil.getTransform( TextFileInput.class, meta, data, TextFileInputMeta.class, TextFileInputData.class, "test" );
    List<Object[]> output = PipelineTestingUtil.execute( input, 2, false );
    PipelineTestingUtil.assertResult( new Object[] { "1", "1", "1" }, output.get( 0 ) );
    PipelineTestingUtil.assertResult( new Object[] { "2", "1", "2" }, output.get( 1 ) );

    deleteVfsFile( virtualFile );
  }

  @Test
  public void readInputWithDefaultValues() throws Exception {
    final String virtualFile = createVirtualFile( "pdi-14832.txt", "1,\n" );

    TextFileInputMeta meta = new TextFileInputMeta();
    TextFileInputField field2 = field( "col2" );
    field2.setIfNullValue( "DEFAULT" );
    meta.setInputFields( new TextFileInputField[] { field( "col1" ), field2 } );
    meta.setFileCompression( "None" );
    meta.setFileType( "CSV" );
    meta.setHeader( false );
    meta.setNrHeaderLines( -1 );
    meta.setFooter( false );
    meta.setNrFooterLines( -1 );

    TextFileInputData data = new TextFileInputData();
    data.setFiles( new FileInputList() );
    data.getFiles().addFile( HopVfs.getFileObject( virtualFile ) );

    data.outputRowMeta = new RowMeta();
    data.outputRowMeta.addValueMeta( new ValueMetaString( "col1" ) );
    data.outputRowMeta.addValueMeta( new ValueMetaString( "col2" ) );

    data.dataErrorLineHandler = Mockito.mock( IFileErrorHandler.class );
    data.fileFormatType = TextFileInputMeta.FILE_FORMAT_UNIX;
    data.separator = ",";
    data.filterProcessor = new TextFileFilterProcessor( new TextFileFilter[ 0 ] );
    data.filePlayList = new FilePlayListAll();

    TextFileInput input = TransformMockUtil.getTransform( TextFileInput.class, meta, data, TextFileInputMeta.class, TextFileInputData.class, "test" );
    List<Object[]> output = PipelineTestingUtil.execute( input, 1, false );
    PipelineTestingUtil.assertResult( new Object[] { "1", "DEFAULT" }, output.get( 0 ) );

    deleteVfsFile( virtualFile );
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

    OutputStream os = HopVfs.getFileObject( virtualFile ).getContent().getOutputStream();
    try {
      IOUtils.copy( new ByteArrayInputStream( bos.toByteArray() ), os );
    } finally {
      os.close();
    }

    return virtualFile;
  }

  private static void deleteVfsFile( String path ) throws Exception {
    TestUtils.getFileObject( path ).delete();
  }

  private static TextFileInputField field( String name ) {
    return new TextFileInputField( name, -1, -1 );
  }

  /**
   * PDI-14390 Text file input throws NPE if skipping error rows and passing through incoming fieds
   *
   * @throws Exception
   */
  @Test
  public void convertLineToRowTest() throws Exception {
    ILogChannel log = Mockito.mock( ILogChannel.class );
    TextFileLine textFileLine = Mockito.mock( TextFileLine.class );
    textFileLine.line = "testData1;testData2;testData3";
    IInputFileMeta info = Mockito.mock( IInputFileMeta.class );
    TextFileInputField[] textFileInputFields = { new TextFileInputField(), new TextFileInputField(), new TextFileInputField() };
    Mockito.doReturn( textFileInputFields ).when( info ).getInputFields();
    Mockito.doReturn( "CSV" ).when( info ).getFileType();
    Mockito.doReturn( "/" ).when( info ).getEscapeCharacter();
    Mockito.doReturn( true ).when( info ).isErrorIgnored();
    Mockito.doReturn( true ).when( info ).isErrorLineSkipped();

    IRowMeta outputRowMeta = Mockito.mock( IRowMeta.class );
    Mockito.doReturn( 15 ).when( outputRowMeta ).size();

    IValueMeta valueMetaWithError = Mockito.mock( IValueMeta.class );
    Mockito.doThrow( new HopValueException( "Error converting" ) ).when( valueMetaWithError ).convertDataFromString( Mockito.anyString(),
      Mockito.any( IValueMeta.class ), Mockito.anyString(), Mockito.anyString(), Mockito.anyInt() );
    Mockito.doReturn( valueMetaWithError ).when( outputRowMeta ).getValueMeta( Mockito.anyInt() );

    //it should run without NPE
    TextFileInput.convertLineToRow( log, textFileLine, info, new Object[ 3 ], 1, outputRowMeta,
      Mockito.mock( IRowMeta.class ), null, 1L, ";", null, "/", Mockito.mock( IFileErrorHandler.class ),
      false, false, false, false, false, false, false, false, null, null, false, new Date(), null, null, null, 1L );
  }

}
