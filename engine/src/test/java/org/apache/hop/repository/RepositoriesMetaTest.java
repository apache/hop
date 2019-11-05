/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.repository;

import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.test.util.XXEUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RepositoriesMetaTest {
  private RepositoriesMeta repoMeta;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpClass() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    repoMeta = spy( new RepositoriesMeta() );
  }

  @Test
  public void testToString() throws Exception {
    RepositoriesMeta repositoriesMeta = new RepositoriesMeta();
    assertEquals( "RepositoriesMeta", repositoriesMeta.toString() );
  }

  @Test
  @Ignore
  public void testReadData_closeInput() throws Exception {
    String repositoriesFile = getClass().getResource( "repositories.xml" ).getPath();
    
    LogChannel log = mock( LogChannel.class );
    when( repoMeta.getHopUserRepositoriesFile() ).thenReturn( repositoriesFile );
    when( repoMeta.newLogChannel() ).thenReturn( log );
    repoMeta.readData();
    
    RandomAccessFile fos = null;
    try {
      File file = new File( repositoriesFile );
      if ( file.exists() ) {
        fos = new RandomAccessFile( file, "rw" );
      }
    } catch ( FileNotFoundException | SecurityException e ) {
      fail( "the file with properties should be unallocated" );
    } finally {
      if ( fos != null ) {
        fos.close();
      }
    }
  }
  
  @Test
  @Ignore
  public void testReadData() throws Exception {

    LogChannel log = mock( LogChannel.class );
    doReturn( getClass().getResource( "repositories.xml" ).getPath() ).when( repoMeta ).getHopUserRepositoriesFile();
    doReturn( log ).when( repoMeta ).newLogChannel();
    repoMeta.readData();

    String repositoriesXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + Const.CR
      + "<repositories>" + Const.CR
      + "  <connection>" + Const.CR
      + "    <name>local postgres</name>" + Const.CR
      + "    <server>localhost</server>" + Const.CR
      + "    <type>POSTGRESQL</type>" + Const.CR
      + "    <access>Native</access>" + Const.CR
      + "    <database>hibernate</database>" + Const.CR
      + "    <port>5432</port>" + Const.CR
      + "    <username>auser</username>" + Const.CR
      + "    <password>Encrypted 2be98afc86aa7f285bb18bd63c99dbdde</password>" + Const.CR
      + "    <servername/>" + Const.CR
      + "    <data_tablespace/>" + Const.CR
      + "    <index_tablespace/>" + Const.CR
      + "    <attributes>" + Const.CR
      + "      <attribute><code>FORCE_IDENTIFIERS_TO_LOWERCASE</code><attribute>N</attribute></attribute>" + Const.CR
      + "      <attribute><code>FORCE_IDENTIFIERS_TO_UPPERCASE</code><attribute>N</attribute></attribute>" + Const.CR
      + "      <attribute><code>IS_CLUSTERED</code><attribute>N</attribute></attribute>" + Const.CR
      + "      <attribute><code>PORT_NUMBER</code><attribute>5432</attribute></attribute>" + Const.CR
      + "      <attribute><code>PRESERVE_RESERVED_WORD_CASE</code><attribute>N</attribute></attribute>" + Const.CR
      + "      <attribute><code>QUOTE_ALL_FIELDS</code><attribute>N</attribute></attribute>" + Const.CR
      + "      <attribute><code>SUPPORTS_BOOLEAN_DATA_TYPE</code><attribute>Y</attribute></attribute>" + Const.CR
      + "      <attribute><code>SUPPORTS_TIMESTAMP_DATA_TYPE</code><attribute>Y</attribute></attribute>" + Const.CR
      + "      <attribute><code>USE_POOLING</code><attribute>N</attribute></attribute>" + Const.CR
      + "    </attributes>" + Const.CR
      + "  </connection>" + Const.CR
      + "  <repository>    <id>HopFileRepository</id>" + Const.CR
      + "    <name>Test Repository</name>" + Const.CR
      + "    <description>Test Repository Description</description>" + Const.CR
      + "    <is_default>false</is_default>" + Const.CR
      + "    <base_directory>test-repository</base_directory>" + Const.CR
      + "    <read_only>N</read_only>" + Const.CR
      + "    <hides_hidden_files>N</hides_hidden_files>" + Const.CR
      + "  </repository>  </repositories>" + Const.CR;
    assertEquals( repositoriesXml, repoMeta.getXML() );
    RepositoriesMeta clone = repoMeta.clone();
    assertEquals( repositoriesXml, repoMeta.getXML() );
    assertNotSame( clone, repoMeta );

    assertEquals( 1, repoMeta.nrRepositories() );
    RepositoryMeta repository = repoMeta.getRepository( 0 );
    assertEquals( "Test Repository", repository.getName() );
    assertEquals( "Test Repository Description", repository.getDescription() );
    assertEquals( "  <repository>    <id>HopFileRepository</id>" + Const.CR
      + "    <name>Test Repository</name>" + Const.CR
      + "    <description>Test Repository Description</description>" + Const.CR
      + "    <is_default>false</is_default>" + Const.CR
      + "    <base_directory>test-repository</base_directory>" + Const.CR
      + "    <read_only>N</read_only>" + Const.CR
      + "    <hides_hidden_files>N</hides_hidden_files>" + Const.CR
      + "  </repository>", repository.getXML() );
    assertSame( repository, repoMeta.searchRepository( "Test Repository" ) );
    assertSame( repository, repoMeta.findRepositoryById( "HopFileRepository" ) );
    assertSame( repository, repoMeta.findRepository( "Test Repository" ) );
    assertNull( repoMeta.findRepository( "not found" ) );
    assertNull( repoMeta.findRepositoryById( "not found" ) );
    assertEquals( 0, repoMeta.indexOfRepository( repository ) );
    repoMeta.removeRepository( 0 );
    assertEquals( 0, repoMeta.nrRepositories() );
    assertNull( repoMeta.searchRepository( "Test Repository" ) );
    repoMeta.addRepository( 0, repository );
    assertEquals( 1, repoMeta.nrRepositories() );
    repoMeta.removeRepository( 1 );
    assertEquals( 1, repoMeta.nrRepositories() );


    assertEquals( 1, repoMeta.nrDatabases() );
    assertEquals( "local postgres", repoMeta.getDatabase( 0 ).getName() );
    DatabaseMeta searchDatabase = repoMeta.searchDatabase( "local postgres" );
    assertSame( searchDatabase, repoMeta.getDatabase( 0 ) );
    assertEquals( 0, repoMeta.indexOfDatabase( searchDatabase ) );
    repoMeta.removeDatabase( 0 );
    assertEquals( 0, repoMeta.nrDatabases() );
    assertNull( repoMeta.searchDatabase( "local postgres" ) );
    repoMeta.addDatabase( 0, searchDatabase );
    assertEquals( 1, repoMeta.nrDatabases() );
    repoMeta.removeDatabase( 1 );
    assertEquals( 1, repoMeta.nrDatabases() );

    assertEquals( "Unable to read repository with id [junk]. RepositoryMeta is not available.", repoMeta.getErrorMessage() );
  }

  @Test
  public void testNothingToRead() throws Exception {
    doReturn( "filedoesnotexist.xml" ).when( repoMeta ).getHopUserRepositoriesFile();

    assertTrue( repoMeta.readData() );
    assertEquals( 0, repoMeta.nrDatabases() );
    assertEquals( 0, repoMeta.nrRepositories() );
  }

  @Test
  @Ignore
  public void testReadDataFromInputStream() throws Exception {
    InputStream inputStream = getClass().getResourceAsStream( "repositories.xml" );
    repoMeta.readDataFromInputStream( inputStream );

    assertEquals( 1, repoMeta.nrDatabases() );
    assertEquals( 1, repoMeta.nrRepositories() );
  }

  @Test
  public void testErrorReadingInputStream() throws Exception {
    try {
      repoMeta.readDataFromInputStream(  getClass().getResourceAsStream( "filedoesnotexist.xml" ) );
    } catch ( HopException e ) {
      assertEquals( Const.CR
        + "Error reading information from file:" + Const.CR
        + "InputStream cannot be null" + Const.CR, e.getMessage() );
    }
  }

  @Test
  public void testErrorReadingFile() throws Exception {
    when( repoMeta.getHopUserRepositoriesFile() ).thenReturn( getClass().getResource( "bad-repositories.xml" ).getPath() );
    try {
      repoMeta.readData();
    } catch ( HopException e ) {
      assertEquals( Const.CR
        + "Error reading information from file:" + Const.CR
        + "The element type \"repositories\" must be terminated by the matching end-tag \"</repositories>\"."
        + Const.CR, e.getMessage() );
    }
  }

  @Test
  public void testWriteFile() throws Exception {
    String path = getClass().getResource( "repositories.xml" ).getPath().replace( "repositories.xml", "new-repositories.xml" );
    doReturn( path ).when( repoMeta ).getHopUserRepositoriesFile();
    repoMeta.writeData();
    InputStream resourceAsStream = getClass().getResourceAsStream( "new-repositories.xml" );
    assertEquals(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + Const.CR
        + "<repositories>" + Const.CR
        + "  </repositories>" + Const.CR, IOUtils.toString( resourceAsStream ) );
    new File( path ).delete();
  }

  @Test
  public void testErrorWritingFile() throws Exception {
    when( repoMeta.getHopUserRepositoriesFile() ).thenReturn( null );
    try {
      repoMeta.writeData();
    } catch ( HopException e ) {
      assertTrue( e.getMessage().startsWith( Const.CR + "Error writing repositories metadata" ) );
    }
  }



  @Test( expected = HopException.class )
  public void exceptionThrownWhenParsingXmlWithBigAmountOfExternalEntitiesFromInputStream() throws Exception {

    repoMeta.readDataFromInputStream( new ByteArrayInputStream( XXEUtils.MALICIOUS_XML.getBytes() ) );
  }
}
