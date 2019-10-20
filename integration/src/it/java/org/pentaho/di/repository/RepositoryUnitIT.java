/*
 * ! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 * ******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * ****************************************************************************
 */

package org.apache.hop.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;

import junit.framework.TestSuite;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.repository.kdr.HopDatabaseRepository;
import org.apache.hop.repository.kdr.HopDatabaseRepositoryCreationHelper;
import org.apache.hop.repository.kdr.HopDatabaseRepositoryMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.steps.getxmldata.GetXMLDataField;
import org.apache.hop.trans.steps.getxmldata.GetXMLDataMeta;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;

/**
 * This class serves as a collection of tests for transformation steps and other Hop capabilities
 * that need to interact with a database repository. It offers performance benefits over putting
 * repo-related tests into the individual unit test classes, as it is a test suite that establishes
 * a connection to the test database repository once before all internal test cases are executed,
 * and disconnects after all tests have been run.
 *
 * @author Matt Burgess
 *
 */
public class RepositoryUnitIT extends TestSuite {

  protected static HopDatabaseRepositoryMeta repositoryMeta;
  protected static HopDatabaseRepository repository;
  protected static DatabaseMeta connection;
  protected static PluginRegistry registry;
  protected static String filename; // The H2 database backing file

  /**
   * setUpBeforeClass is a method called once before all tests are run. For this test suite, it is
   * used to set up and connect to the test database repository, to increase performance and reduce
   * unnecessary initialization, connects/disconnects from each test case. If repository
   * initialization and/or connection/disconnection
   *
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    HopEnvironment.init();

    registry = PluginRegistry.getInstance();

    filename = File.createTempFile( "kdrtest", "" ).getAbsolutePath();

    System.out.println( "Using file '" + filename + "' as a H2 database repository" );

    try {
      DatabaseMeta databaseMeta = new DatabaseMeta( "H2Repo", "H2", "JDBC", null, filename, null, null, null );
      repositoryMeta =
        new HopDatabaseRepositoryMeta( "HopDatabaseRepository", "H2Repo", "H2 Repository", databaseMeta );
      repository = new HopDatabaseRepository();
      repository.init( repositoryMeta );
      repository.connectionDelegate.connect( true, true );
      HopDatabaseRepositoryCreationHelper helper = new HopDatabaseRepositoryCreationHelper( repository );
      helper.createRepositorySchema( null, false, new ArrayList<String>(), false );

      // Reconnect as admin
      repository.disconnect();
      repository.connect( "admin", "admin" );

    } catch ( Exception e ) {
      e.printStackTrace();
      throw new HopException( "Error during database repository unit testing", e );
    }
  }

  /**
   * This method is called once after all test cases have been run, and is used to perform
   * suite-level cleanup such as disconnecting from the test repository.
   *
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if ( repository != null ) {

      // Disconnect and remove the H2 database file
      repository.disconnect();

      new File( filename + ".h2.db" ).delete();
      new File( filename + ".trace.db" ).delete();
    }
  }

  /**
   * This method is called once before each test case is executed.
   *
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * This method is called once after each test case is executed.
   *
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  /**
   * This test is to ensure that the metadata for the GetXMLData step is preserved when saving to a
   * repository. The test creates a GetXMLData step and saves it to the repository. Then the local
   * data is changed and the step is read back in from the repository. It is then asserted that the
   * field value(s) are equal to what was saved.
   *
   * Test method for
   * {@link org.apache.hop.trans.steps.getxmldata.GetXMLDataMeta#readRep(org.apache.hop.repository.Repository,
   * org.apache.hop.repository.ObjectId, java.util.List, java.util.Map)}
   * . Test method for
   * {@link org.apache.hop.trans.steps.getxmldata.GetXMLDataMeta#saveRep(org.apache.hop.repository.Repository,
   * org.apache.hop.repository.ObjectId, jorg.apache.hop.repository.ObjectId)}
   * .
   */
  @Test
  public void testGetXMLDataMetaSaveAndReadRep() {

    //
    // Create a new transformation...
    //
    TransMeta transMeta = new TransMeta();
    transMeta.setName( "getxmldata1" );

    //
    // Create a Get XML Data step
    //
    String getXMLDataName = "get xml data step";
    GetXMLDataMeta gxdm = new GetXMLDataMeta();

    String getXMLDataPid = registry.getPluginId( StepPluginType.class, gxdm );
    StepMeta getXMLDataStep = new StepMeta( getXMLDataPid, getXMLDataName, gxdm );
    transMeta.addStep( getXMLDataStep );

    GetXMLDataField[] fields = new GetXMLDataField[1];

    for ( int idx = 0; idx < fields.length; idx++ ) {
      fields[idx] = new GetXMLDataField();
    }

    fields[0].setName( "objectid" );
    fields[0].setXPath( "ObjectID" );
    fields[0].setElementType( GetXMLDataField.ELEMENT_TYPE_NODE );
    fields[0].setResultType( GetXMLDataField.RESULT_TYPE_TYPE_SINGLE_NODE );
    fields[0].setType( ValueMetaInterface.TYPE_STRING );
    fields[0].setFormat( "" );
    fields[0].setLength( -1 );
    fields[0].setPrecision( -1 );
    fields[0].setCurrencySymbol( "" );
    fields[0].setDecimalSymbol( "" );
    fields[0].setGroupSymbol( "" );
    fields[0].setTrimType( GetXMLDataField.TYPE_TRIM_NONE );

    gxdm.setDefault();
    gxdm.setEncoding( "UTF-8" );
    gxdm.setIsAFile( false );
    gxdm.setInFields( true );
    gxdm.setLoopXPath( "/" );
    gxdm.setXMLField( "field1" );
    gxdm.setInputFields( fields );

    try {
      // Now save the transformation and then read it back in
      transMeta.setRepository( repository );
      RepositoryDirectoryInterface repositoryDirectory = repository.findDirectory( "/" );
      transMeta.setRepositoryDirectory( repositoryDirectory );
      repository.transDelegate.saveTransformation( transMeta, "None", null, true );

      // Create a new placeholder meta and set the result type to something different than what was
      // saved,
      // to ensure the saveRep code is working correctly.
      GetXMLDataMeta newMeta = (GetXMLDataMeta) gxdm.clone();
      for ( GetXMLDataField f : newMeta.getInputFields() ) {
        f.setResultType( GetXMLDataField.RESULT_TYPE_VALUE_OF );
      }
      newMeta.readRep( repository, new MemoryMetaStore(), getXMLDataStep.getObjectId(), repository.getDatabases() );

      // Check that the value of Result Type is what was saved in the repo
      assertEquals( newMeta.getInputFields()[0].getResultTypeCode(), "singlenode" );

    } catch ( HopException e ) {
      fail( "Test failed due to exception: " + e.getLocalizedMessage() );
    }
  }
}
