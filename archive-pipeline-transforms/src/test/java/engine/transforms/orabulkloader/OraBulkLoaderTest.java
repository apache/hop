/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.apache.hop.pipeline.transforms.orabulkloader;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.poi.util.TempFile;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * User: Dzmitry Stsiapanau Date: 4/8/14 Time: 1:44 PM
 */
public class OraBulkLoaderTest {
  private static final String expectedDataContents1 = "OPTIONS(" + Const.CR + "  ERRORS='null'" + Const.CR + "  , "
    + "ROWS='null'" + Const.CR + ")" + Const.CR + "LOAD DATA" + Const.CR + "INFILE '";
  private static final String expectedDataContents2 = "'" + Const.CR + "INTO TABLE null" + Const.CR + "null"
    + Const.CR + "FIELDS TERMINATED BY ',' " + "ENCLOSED BY '\"'" + Const.CR + "(null, " + Const.CR + "null CHAR)";
  private TransformMockHelper<OraBulkLoaderMeta, OraBulkLoaderData> transformMockHelper;
  private OraBulkLoader oraBulkLoader;
  private File tempControlFile;
  private File tempDataFile;
  private String tempControlFilepath;
  private String tempDataFilepath;
  private String tempControlVfsFilepath;
  private String tempDataVfsFilepath;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setupBeforeClass() throws HopException {
    HopClientEnvironment.init();
  }

  @Before
  public void setUp() throws Exception {
    transformMockHelper = new TransformMockHelper<OraBulkLoaderMeta, OraBulkLoaderData>( "TEST_CREATE_COMMANDLINE",
      OraBulkLoaderMeta.class, OraBulkLoaderData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      transformMockHelper.logChannelInterface );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
    oraBulkLoader = spy( new OraBulkLoader( transformMockHelper.transformMeta, transformMockHelper.iTransformData, 0,
      transformMockHelper.pipelineMeta, transformMockHelper.pipeline ) );

    tempControlFile = TempFile.createTempFile( "control", "test" );
    tempControlFile.deleteOnExit();
    tempDataFile = TempFile.createTempFile( "data", "test" );
    tempDataFile.deleteOnExit();
    tempControlFilepath = tempControlFile.getAbsolutePath();
    tempDataFilepath = tempDataFile.getAbsolutePath();
    tempControlVfsFilepath = "file:///" + tempControlFilepath;
    tempDataVfsFilepath = "file:///" + tempDataFilepath;
  }

  @After
  public void cleanUp() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testGetControlFileContents() throws Exception {
    String[] streamFields = { "id", "name" };
    String[] streamTable = { "id", "name" };
    String[] dateMask = { "", "" };
    DatabaseMeta databaseMeta = mock( DatabaseMeta.class );
    Object[] rowData = { 1, "rowdata", new Date() };
    IRowMeta iRowMeta = mock( IRowMeta.class );
    PipelineMeta pipelineMeta = spy( new PipelineMeta() );
    IValueMeta idVmi = new ValueMetaNumber( "id" );
    IValueMeta nameVmi = new ValueMetaString( "name", 20, -1 );

    OraBulkLoaderMeta oraBulkLoaderMeta = spy( new OraBulkLoaderMeta() );
    oraBulkLoaderMeta.setDatabaseMeta( databaseMeta );
    oraBulkLoaderMeta.setControlFile( tempControlVfsFilepath );
    oraBulkLoaderMeta.setDataFile( tempDataVfsFilepath );

    doReturn( pipelineMeta ).when( oraBulkLoader ).getPipelineMeta();
    doReturn( streamFields ).when( oraBulkLoaderMeta ).getFieldStream();
    doReturn( streamTable ).when( oraBulkLoaderMeta ).getFieldTable();
    doReturn( dateMask ).when( oraBulkLoaderMeta ).getDateMask();
    doReturn( 0 ).when( iRowMeta ).indexOfValue( "id" );
    doReturn( idVmi ).when( iRowMeta ).getValueMeta( 0 );
    doReturn( 1 ).when( iRowMeta ).indexOfValue( "name" );
    doReturn( nameVmi ).when( iRowMeta ).getValueMeta( 1 );

    String expectedDataContents = expectedDataContents1 + tempDataFilepath + expectedDataContents2;
    String actualDataContents = oraBulkLoader.getControlFileContents( oraBulkLoaderMeta, iRowMeta, rowData );
    assertEquals( "The Expected Control File Contents do not match Actual Contents", expectedDataContents,
      actualDataContents );
  }

  @Test
  public void testCreateControlFile() throws Exception {
    // Create a tempfile, so we can use the temp file path when we run the createControlFile method
    String tempTrueControlFilepath = tempControlFile.getAbsolutePath() + "A.txt";
    String expectedControlContents = "test";
    OraBulkLoaderMeta oraBulkLoaderMeta = mock( OraBulkLoaderMeta.class );
    IRowMeta iRowMeta = mock( IRowMeta.class );
    Object[] objectRow = {};

    doReturn( iRowMeta ).when( oraBulkLoader ).getInputRowMeta();
    doReturn( expectedControlContents ).when( oraBulkLoader ).getControlFileContents( oraBulkLoaderMeta, iRowMeta, objectRow );
    oraBulkLoader.createControlFile( tempTrueControlFilepath, objectRow, oraBulkLoaderMeta );

    assertTrue( Files.exists( Paths.get( tempTrueControlFilepath ) ) );

    File tempTrueControlFile = new File( tempTrueControlFilepath );
    String tempTrueControlFileContents = new String( Files.readAllBytes( tempTrueControlFile.toPath() ) );
    assertEquals( expectedControlContents, tempTrueControlFileContents );
    tempTrueControlFile.delete();
  }

  @Test
  public void testCreateCommandLine() throws Exception {
    File tmp = File.createTempFile( "testCreateCOmmandLine", "tmp" );
    tmp.deleteOnExit();
    OraBulkLoaderMeta meta = new OraBulkLoaderMeta();
    meta.setSqlldr( tmp.getAbsolutePath() );
    meta.setControlFile( tmp.getAbsolutePath() );
    DatabaseMeta dm = mock( DatabaseMeta.class );
    when( dm.getUsername() ).thenReturn( "user" );
    when( dm.getPassword() ).thenReturn( "Encrypted 2be98afc86aa7f2e4cb298b5eeab387f5" );
    meta.setDatabaseMeta( dm );
    String cmd = oraBulkLoader.createCommandLine( meta, true );
    String expected = tmp.getAbsolutePath() + " control='" + tmp.getAbsolutePath() + "' userid=user/PENTAHO@";
    assertEquals( "Comandline for oracle bulkloader is not as expected", expected, cmd );
  }

  @Test
  public void testDispose() throws Exception {
    PipelineMeta pipelineMeta = spy( new PipelineMeta() );
    OraBulkLoaderData oraBulkLoaderData = new OraBulkLoaderData();
    OraBulkLoaderMeta oraBulkLoaderMeta = new OraBulkLoaderMeta();
    oraBulkLoaderMeta.setDataFile( tempDataVfsFilepath );
    oraBulkLoaderMeta.setControlFile( tempControlVfsFilepath );
    oraBulkLoaderMeta.setEraseFiles( true );
    oraBulkLoaderMeta.setLoadMethod( "AUTO_END" );

    assertTrue( Files.exists( Paths.get( tempControlFilepath ) ) );
    assertTrue( Files.exists( Paths.get( tempDataFilepath ) ) );

    doReturn( pipelineMeta ).when( oraBulkLoader ).getPipelineMeta();
    oraBulkLoader.dispose();

    assertFalse( Files.exists( Paths.get( tempControlFilepath ) ) );
    assertFalse( Files.exists( Paths.get( tempDataFilepath ) ) );
  }
}
