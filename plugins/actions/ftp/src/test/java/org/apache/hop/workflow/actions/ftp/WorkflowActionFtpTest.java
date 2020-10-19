/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.workflow.actions.ftp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.utils.TestUtils;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WorkflowActionFtpTest {
  private IWorkflowEngine<WorkflowMeta> workflow;
  private ActionFtp action;
  private String existingDir;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Before
  public void setUp() throws Exception {
    workflow = new LocalWorkflowEngine( new WorkflowMeta() );
    action = new MockedActionFtp();

    workflow.getWorkflowMeta().addAction( new ActionMeta( action ) );
    action.setParentWorkflow( workflow );

    workflow.setStopped( false );

    action.setServerName( "some.server" );
    action.setUserName( "anonymous" );
    action.setFtpDirectory( "." );
    action.setWildcard( "robots.txt" );
    action.setBinaryMode( false );
    action.setSuccessCondition( "success_if_no_errors" );

    existingDir = TestUtils.createTempDir();
  }

  @After
  public void tearDown() throws Exception {
    File fl = new File( existingDir );
    if ( !fl.exists() ) {
      return;
    }
    File[] fls = fl.listFiles();
    if ( fls == null || fls.length == 0 ) {
      return;
    }
    fls[ 0 ].delete();
    fl.delete();
  }

  @Test
  public void testFixedExistingTargetDir() throws Exception {
    action.setTargetDirectory( existingDir );

    Result result = action.execute( new Result(), 0 );

    assertTrue( "For existing folder should be true", result.getResult() );
    assertEquals( "There should be no errors", 0, result.getNrErrors() );
  }

  @Test
  public void testFixedNonExistingTargetDir() throws Exception {
    action.setTargetDirectory( existingDir + File.separator + "sub" );

    Result result = action.execute( new Result(), 0 );

    assertFalse( "For non existing folder should be false", result.getResult() );
    assertTrue( "There should be errors", 0 != result.getNrErrors() );
  }

  @Test
  public void testVariableExistingTargetDir() throws Exception {
    action.setTargetDirectory( "${Internal.Workflow.Filename.Directory}" );
    action.setVariable( "Internal.Workflow.Filename.Directory", existingDir );

    Result result = action.execute( new Result(), 0 );

    assertTrue( "For existing folder should be true", result.getResult() );
    assertEquals( "There should be no errors", 0, result.getNrErrors() );
  }

  @Test
  public void testVariableNonExistingTargetDir() throws Exception {
    action.setTargetDirectory( "${Internal.Workflow.Filename.Directory}/Worg" );
    action.setVariable( "Internal.Workflow.Filename.Directory", existingDir + File.separator + "sub" );

    Result result = action.execute( new Result(), 0 );

    assertFalse( "For non existing folder should be false", result.getResult() );
    assertTrue( "There should be errors", 0 != result.getNrErrors() );
  }

  @Test
  public void testProtocolVariableExistingTargetDir() throws Exception {
    action.setTargetDirectory( "${Internal.Workflow.Filename.Directory}" );
    action.setVariable( "Internal.Workflow.Filename.Directory", "file://" + existingDir );

    Result result = action.execute( new Result(), 0 );

    assertTrue( "For existing folder should be true", result.getResult() );
    assertEquals( "There should be no errors", 0, result.getNrErrors() );
  }

  @Test
  public void testPtotocolVariableNonExistingTargetDir() throws Exception {
    action.setTargetDirectory( "${Internal.Workflow.Filename.Directory}/Worg" );
    action.setVariable( "Internal.Workflow.Filename.Directory", "file://" + existingDir + File.separator + "sub" );

    Result result = action.execute( new Result(), 0 );

    assertFalse( "For non existing folder should be false", result.getResult() );
    assertTrue( "There should be errors", 0 != result.getNrErrors() );
  }

  @Test
  public void testTargetFilenameNoDateTime() throws Exception {
    File destFolder = tempFolder.newFolder( "pdi5558" );
    destFolder.deleteOnExit();
    ActionFtp entry = new ActionFtp();
    entry.setTargetDirectory( destFolder.getAbsolutePath() );
    entry.setAddDateBeforeExtension( false );

    assertNull( entry.returnTargetFilename( null ) );
    assertEquals( destFolder.getAbsolutePath() + Const.FILE_SEPARATOR + "testFile",
      entry.returnTargetFilename( "testFile" ) );
    assertEquals( destFolder.getAbsolutePath() + Const.FILE_SEPARATOR + "testFile.txt",
      entry.returnTargetFilename( "testFile.txt" ) );
  }

  @Test
  public void testTargetFilenameWithDateTime() throws Exception {
    SimpleDateFormat yyyyMMdd = new SimpleDateFormat( "yyyyMMdd" );
    SimpleDateFormat HHmmssSSS = new SimpleDateFormat( "HHmmssSSS" );
    SimpleDateFormat yyyyMMddHHmmssSSS = new SimpleDateFormat( "yyyyMMdd_HHmmssSSS" );
    File destFolder = tempFolder.newFolder( "pdi5558" );
    destFolder.deleteOnExit();
    String destFolderName = destFolder.getAbsolutePath();
    ActionFtp entry = new ActionFtp();
    entry.setTargetDirectory( destFolderName );
    entry.setAddDateBeforeExtension( true );

    //Test Date-Only
    entry.setDateInFilename( true );
    assertNull( entry.returnTargetFilename( null ) );
    assertEquals( "Test Add Date without file extension",
      destFolderName + Const.FILE_SEPARATOR + "testFile_" + yyyyMMdd.format( new Date() ),
      entry.returnTargetFilename( "testFile" ) );
    assertEquals( "Test Add Date with file extension",
      destFolderName + Const.FILE_SEPARATOR + "testFile_" + yyyyMMdd.format( new Date() ) + ".txt",
      entry.returnTargetFilename( "testFile.txt" ) );

    //Test Date-and-Time
    entry.setTimeInFilename( true );
    String beforeString = destFolderName + Const.FILE_SEPARATOR + "testFile_" + yyyyMMddHHmmssSSS.format( new Date() ) + ".txt";
    String actualValue = entry.returnTargetFilename( "testFile.txt" );
    String afterString = destFolderName + Const.FILE_SEPARATOR + "testFile_" + yyyyMMddHHmmssSSS.format( new Date() ) + ".txt";

    Pattern expectedFormat = Pattern.compile(
      Pattern.quote( destFolderName + Const.FILE_SEPARATOR + "testFile_" + yyyyMMdd.format( new Date() ) + "_" )
        + "([\\d]{9})\\.txt" );
    assertTrue( "Output file matches expected format", expectedFormat.matcher( actualValue ).matches() );
    assertTrue( "The actual time is not too early for test run", actualValue.compareTo( beforeString ) >= 0 );
    assertTrue( "The actual time is not too late for test run", actualValue.compareTo( afterString ) <= 0 );

    //Test Time-Only
    entry.setDateInFilename( false );
    beforeString = destFolderName + Const.FILE_SEPARATOR + "testFile_" + HHmmssSSS.format( new Date() ) + ".txt";
    actualValue = entry.returnTargetFilename( "testFile.txt" );
    afterString = destFolderName + Const.FILE_SEPARATOR + "testFile_" + HHmmssSSS.format( new Date() ) + ".txt";

    expectedFormat = Pattern.compile(
      Pattern.quote( destFolderName + Const.FILE_SEPARATOR + "testFile_" ) + "([\\d]{9})\\.txt" );
    assertTrue( "Output file matches expected format", expectedFormat.matcher( actualValue ).matches() );
    assertTrue( "The actual time is not too early for test run", actualValue.compareTo( beforeString ) >= 0 );
    assertTrue( "The actual time is not too late for test run", actualValue.compareTo( afterString ) <= 0 );
  }
}
