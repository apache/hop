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

package org.apache.hop.ui.hopui;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static junit.framework.Assert.*;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.hop.ui.hopui.delegates.HopUiTabsDelegate;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.LastUsedFile;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.ObjectRevision;
import org.apache.hop.repository.RepositoryDirectory;
import org.apache.hop.repository.RepositoryDirectoryInterface;
import org.apache.hop.repository.RepositoryObject;
import org.apache.hop.repository.RepositoryObjectType;
import org.apache.hop.repository.RepositorySecurityProvider;
import org.apache.hop.repository.Repository;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepErrorMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.steps.csvinput.CsvInputMeta;
import org.apache.hop.trans.steps.dummytrans.DummyTransMeta;
import org.apache.hop.ui.core.FileDialogOperation;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.hopui.delegates.HopUiDelegates;
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import org.apache.xul.swt.tab.TabItem;
import org.apache.xul.swt.tab.TabSet;

/**
 * HopUI tests
 *
 * @author Pavel Sakun
 * @see HopUi
 */
public class HopUiTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private final HopUi hopUi = mock( HopUi.class );
  private final LogChannelInterface log = mock( LogChannelInterface.class );
  private static HopUiPerspective mockHopUiPerspective = mock( HopUiPerspective.class );
  private static HopUiPerspectiveManager perspective = HopUiPerspectiveManager.getInstance();

  @BeforeClass
  public static void setUpClass() {
    perspective.addPerspective( mockHopUiPerspective );
  }

  @Before
  public void setUp() throws HopException {
    doCallRealMethod().when( hopUi ).copySelected( any( TransMeta.class ), anyListOf( StepMeta.class ),
        anyListOf( NotePadMeta.class ) );
    doCallRealMethod().when( hopUi ).pasteXML( any( TransMeta.class ), anyString(), any( Point.class ) );
    doCallRealMethod().when( hopUi ).delHop( any( TransMeta.class ), any( TransHopMeta.class ) );
    when( hopUi.getLog() ).thenReturn( log );

    HopEnvironment.init();
  }

  /**
   * test two steps
   * @see http://jira.pentaho.com/browse/PDI-689
   * 
   * @throws HopException
   */
  @Test
  public void testCopyPasteStepsErrorHandling() throws HopException {

    final TransMeta transMeta = new TransMeta();

    //for check copy both step and hop
    StepMeta sourceStep = new StepMeta( "CsvInput", "Step1", new CsvInputMeta() );
    StepMeta targetStep = new StepMeta( "Dummy", "Dummy Step1", new DummyTransMeta() );

    sourceStep.setSelected( true );
    targetStep.setSelected( true );

    transMeta.addStep( sourceStep );
    transMeta.addStep( targetStep  );

    StepErrorMeta errorMeta = new StepErrorMeta( transMeta, sourceStep, targetStep );
    sourceStep.setStepErrorMeta( errorMeta );
    errorMeta.setSourceStep( sourceStep );
    errorMeta.setTargetStep( targetStep );

    final int stepsSizeBefore = transMeta.getSteps().size();
    doAnswer( new Answer() {
      @Override
      public Object answer( InvocationOnMock invocation ) throws Throwable {
        hopUi.pasteXML( transMeta, (String) invocation.getArguments()[0], mock( Point.class ) );
        assertTrue( "Steps was not copied", stepsSizeBefore < transMeta.getSteps().size() );
        //selected copied step
        for ( StepMeta s:transMeta.getSelectedSteps() ) {
          if ( s.getStepMetaInterface() instanceof CsvInputMeta ) {
            //check that stepError was copied
            assertNotNull( "Error hop was not copied", s.getStepErrorMeta() );
          }
        }
        return null;
      }
    } ).when( hopUi ).toClipboard( anyString() );
    hopUi.copySelected( transMeta, transMeta.getSelectedSteps(), Collections.<NotePadMeta>emptyList() );
  }

  /**
   * test copy one step with error handling 
   * @see http://jira.pentaho.com/browse/PDI-13358
   * 
   * @throws HopException
   */
  @Test
  public void testCopyPasteOneStepWithErrorHandling() throws HopException {

    final TransMeta transMeta = new TransMeta();
    StepMeta sourceStep = new StepMeta( "CsvInput", "Step1", new CsvInputMeta() );
    StepMeta targetStep = new StepMeta( "Dummy", "Dummy Step1", new DummyTransMeta() );

    sourceStep.setSelected( true );
    transMeta.addStep( sourceStep );
    transMeta.addStep( targetStep );

    StepErrorMeta errorMeta = new StepErrorMeta( transMeta, sourceStep, targetStep );
    sourceStep.setStepErrorMeta( errorMeta );
    errorMeta.setSourceStep( sourceStep );
    errorMeta.setTargetStep( targetStep );

    final int stepsSizeBefore = transMeta.getSteps().size();
    doAnswer( new Answer() {
      @Override
      public Object answer( InvocationOnMock invocation ) throws Throwable {
        hopUi.pasteXML( transMeta, (String) invocation.getArguments()[0], mock( Point.class ) );
        assertTrue( "Steps was not copied", stepsSizeBefore < transMeta.getSteps().size() );
        //selected copied step
        for ( StepMeta s:transMeta.getSelectedSteps() ) {
          if ( s.getStepMetaInterface() instanceof CsvInputMeta ) {
            //check that stepError was empty, because we copy only one step from pair
            assertNull( "Error hop was not copied", s.getStepErrorMeta() );
          }
        }
        return null;
      }
    } ).when( hopUi ).toClipboard( anyString() );

    hopUi.copySelected( transMeta, transMeta.getSelectedSteps(), Collections.<NotePadMeta>emptyList() );
  }

  /**
   * Testing displayed test in case versioning enabled
   * @see http://jira.pentaho.com/browse/BACKLOG-11607
   *
   * @throws HopException
   */
  @Test
  public void testSetShellTextForTransformationWVersionEnabled() {
    TransMeta mockTransMeta = mock( TransMeta.class );
    Shell
        mockShell =
        prepareSetShellTextTests( hopUi, mockTransMeta, true, true, false, false, false, false, false, false );

    verify( mockShell ).setText( "Hop - [RepositoryName] transformationName v1.0" );
  }

  @Test
  public void testSetShellTextForTransformationWVersionEnabledRepIsNull() {
    TransMeta mockTransMeta = mock( TransMeta.class );
    Shell
        mockShell =
        prepareSetShellTextTests( hopUi, mockTransMeta, true, true, true, false, false, false, false, false );

    verify( mockShell ).setText( "Hop - transformationName v1.0" );
  }

  @Test
  public void testSetShellTextForTransformationWVersionEnabledRevIsNull() {
    TransMeta mockTransMeta = mock( TransMeta.class );
    Shell
        mockShell =
        prepareSetShellTextTests( hopUi, mockTransMeta, true, true, true, true, false, false, false, false );

    verify( mockShell ).setText( "Hop - transformationName" );
  }

  @Test
  public void testSetShellTextForTransformationWVersionEnabledChanged() {
    TransMeta mockTransMeta = mock( TransMeta.class );
    Shell
        mockShell =
        prepareSetShellTextTests( hopUi, mockTransMeta, true, true, false, false, true, false, false, false );

    verify( mockShell ).setText( "Hop - [RepositoryName] transformationName v1.0 " + BaseMessages
        .getString( HopUi.class, "Spoon.Various.Changed" ) );
  }

  @Test
  public void testSetShellTextForTransformationWVersionEnabledNameIsNull() {
    TransMeta mockTransMeta = mock( TransMeta.class );
    Shell
        mockShell =
        prepareSetShellTextTests( hopUi, mockTransMeta, true, true, false, false, false, true, false, false );

    verify( mockShell ).setText( "Hop - [RepositoryName] transformationFilename v1.0" );
  }

  @Test
  public void testSetShellTextForTransformationWVersionEnabledNameFileNameNull() {
    TransMeta mockTransMeta = mock( TransMeta.class );
    Shell
        mockShell =
        prepareSetShellTextTests( hopUi, mockTransMeta, true, true, false, false, false, true, true, false );

    verify( mockShell ).setText( "Hop - [RepositoryName] tabName v1.0" );
  }

  @Test
  public void testSetShellTextForTransformationWVersionEnabledNameFileNameTabNull() {
    TransMeta mockTransMeta = mock( TransMeta.class );
    Shell
        mockShell =
        prepareSetShellTextTests( hopUi, mockTransMeta, true, true, false, false, false, true, true, true );

    verify( mockShell )
        .setText( "Hop - [RepositoryName] " + BaseMessages.getString( HopUi.class, "Spoon.Various.NoName" ) + " v1.0" );
  }

  @Test
  public void testSetShellTextForTransformationWVersionDisabled() {
    TransMeta mockTransMeta = mock( TransMeta.class );
    Shell
        mockShell =
        prepareSetShellTextTests( hopUi, mockTransMeta, false, true, false, false, false, false, false, false );

    verify( mockShell ).setText( "Hop - [RepositoryName] transformationName" );
  }

  @Test
  public void testSetShellTextForJobWVersionEnabled() {
    JobMeta mockJobMeta = mock( JobMeta.class );
    Shell
        mockShell =
        prepareSetShellTextTests( hopUi, mockJobMeta, true, false, false, false, false, false, false, false );

    verify( mockShell ).setText( "Hop - [RepositoryName] jobName v1.0" );
  }

  @Test
  public void testSetShellTextForJobWVersionEnabledRepIsNull() {
    JobMeta mockJobMeta = mock( JobMeta.class );
    Shell
        mockShell =
        prepareSetShellTextTests( hopUi, mockJobMeta, true, false, true, false, false, false, false, false );

    verify( mockShell ).setText( "Hop - jobName v1.0" );
  }

  @Test
  public void testSetShellTextForJobWVersionEnabledRevIsNull() {
    JobMeta mockJobMeta = mock( JobMeta.class );
    Shell
        mockShell =
        prepareSetShellTextTests( hopUi, mockJobMeta, true, false, true, true, false, false, false, false );

    verify( mockShell ).setText( "Hop - jobName" );
  }

  @Test
  public void testSetShellTextForJobWVersionDisabled() {
    JobMeta mockJobMeta = mock( JobMeta.class );
    Shell
        mockShell =
        prepareSetShellTextTests( hopUi, mockJobMeta, false, false, false, false, false, false, false, false );

    verify( mockShell ).setText( "Hop - [RepositoryName] jobName" );
  }

  private static Shell prepareSetShellTextTests( HopUi hopUi, AbstractMeta abstractMeta, boolean versionEnabled,
                                                 boolean isTransformation, boolean repIsNull, boolean revIsNull, boolean hasChanged, boolean nameIsNull,
                                                 boolean filenameIsNull, boolean tabNameIsNull ) {
    Shell mockShell = mock( Shell.class );
    ObjectRevision mockObjectRevision = revIsNull ? null : mock( ObjectRevision.class );
    RepositoryDirectory mockRepDirectory = mock( RepositoryDirectory.class );
    Repository mockRepository = repIsNull ? null : mock( Repository.class );
    RepositorySecurityProvider mockRepSecurityProvider = mock( RepositorySecurityProvider.class );
    HopUiDelegates mockDelegate = mock( HopUiDelegates.class );
    HopUiTabsDelegate mockDelegateTabs = mock( HopUiTabsDelegate.class );
    hopUi.rep = mockRepository;
    hopUi.delegates = mockDelegate;
    mockDelegate.tabs = mockDelegateTabs;

    doCallRealMethod().when( hopUi ).openHopUi();
    doCallRealMethod().when( hopUi ).setShellText();

    doReturn( mockShell ).when( hopUi ).getShell();
    if ( !tabNameIsNull ) {
      doReturn( "tabName" ).when( hopUi ).getActiveTabText();
    }

    doReturn( false ).when( mockShell ).isDisposed();
    setTransJobValues( abstractMeta, hopUi, mockObjectRevision, mockRepDirectory, isTransformation, hasChanged,
        nameIsNull, filenameIsNull );

    if ( !revIsNull ) {
      doReturn( "1.0" ).when( mockObjectRevision ).getName();
    }
    doReturn( "/admin" ).when( mockRepDirectory ).getPath();

    Mockito.doReturn( null ).when( abstractMeta ).getVersioningEnabled();
    if ( !repIsNull ) {
      doReturn( mockRepSecurityProvider ).when( mockRepository ).getSecurityProvider();
      doReturn( versionEnabled ).when( mockRepSecurityProvider ).isVersioningEnabled( anyString() );
    }

    doReturn( "RepositoryName" ).when( hopUi ).getRepositoryName();

    doReturn( new ArrayList<TabMapEntry>() ).when( mockDelegateTabs ).getTabs();

    try {
      hopUi.openHopUi();
    } catch ( NullPointerException e ) {
      //ignore work is done
    }

    hopUi.setShellText();

    return mockShell;

  }

  private static void setTransJobValues( AbstractMeta mockObjMeta, HopUi hopUi, ObjectRevision objectRevision,
                                         RepositoryDirectory repositoryDirectory, boolean isTransformation, boolean hasChanged, boolean nameIsNull,
                                         boolean filenameIsNull ) {

    if ( isTransformation ) {
      doReturn( mockObjMeta ).when( hopUi ).getActiveTransformation();
      doReturn( null ).when( hopUi ).getActiveJob();
    } else {
      doReturn( null ).when( hopUi ).getActiveTransformation();
      doReturn( mockObjMeta ).when( hopUi ).getActiveJob();
    }
    if ( objectRevision != null ) {
      doReturn( objectRevision ).when( mockObjMeta ).getObjectRevision();
    }

    if ( !filenameIsNull ) {
      doReturn( isTransformation ? "transformationFilename" : "jobFilename" ).when( mockObjMeta ).getFilename();
    }
    doReturn( hasChanged ).when( mockObjMeta ).hasChanged();
    if ( !nameIsNull ) {
      doReturn( isTransformation ? "transformationName" : "jobName" ).when( mockObjMeta ).getName();
    }
    doReturn( repositoryDirectory ).when( mockObjMeta ).getRepositoryDirectory();
    doReturn( isTransformation ? RepositoryObjectType.TRANSFORMATION : RepositoryObjectType.JOB ).when( mockObjMeta )
        .getRepositoryElementType();
  }

  @Test
  public void testDelHop() throws Exception {

    StepMetaInterface fromStepMetaInterface = Mockito.mock( StepMetaInterface.class );
    StepMeta fromStep = new StepMeta();
    fromStep.setStepMetaInterface( fromStepMetaInterface );

    StepMetaInterface toStepMetaInterface = Mockito.mock( StepMetaInterface.class );
    StepMeta toStep = new StepMeta();
    toStep.setStepMetaInterface( toStepMetaInterface );

    TransHopMeta transHopMeta = new TransHopMeta();
    transHopMeta.setFromStep( fromStep );
    transHopMeta.setToStep( toStep );

    TransMeta transMeta = Mockito.mock( TransMeta.class );

    hopUi.delHop( transMeta, transHopMeta );
    Mockito.verify( fromStepMetaInterface, times( 1 ) ).cleanAfterHopFromRemove( toStep );
    Mockito.verify( toStepMetaInterface, times( 1 ) ).cleanAfterHopToRemove( fromStep );
  }

  @Test
  public void testNullParamSaveToFile() throws Exception {
    doCallRealMethod().when( hopUi ).saveToFile( any() );
    assertFalse( hopUi.saveToFile( null ) );
  }

  @Test
  public void testJobToRepSaveToFile() throws Exception {
    JobMeta mockJobMeta = mock( JobMeta.class );

    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockJobMeta, false, false, MainHopUiPerspective.ID, true,
        true, null, null, true, true );

    doCallRealMethod().when( hopUi ).saveToFile( mockJobMeta );
    assertTrue( hopUi.saveToFile( mockJobMeta ) );
    verify( mockJobMeta ).setRepository( hopUi.rep );
    verify( mockJobMeta ).setMetaStore( hopUi.metaStore );

    verify( mockJobMeta ).setFilename( null );

    verify( hopUi.delegates.tabs ).renameTabs();
    verify( hopUi ).enableMenus();
  }

  @Test
  public void testJobToFileSaveToFile() throws Exception {
    JobMeta mockJobMeta = mock( JobMeta.class );

    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockJobMeta, true, false, "NotMainSpoonPerspective", true,
        true, null, "filename", true, true );

    doCallRealMethod().when( hopUi ).saveToFile( mockJobMeta );
    assertTrue( hopUi.saveToFile( mockJobMeta ) );
    verify( mockJobMeta ).setRepository( hopUi.rep );
    verify( mockJobMeta ).setMetaStore( hopUi.metaStore );

    verify( hopUi.delegates.tabs ).renameTabs();
    verify( hopUi ).enableMenus();
  }

  @Test
  public void testJobToFileWithoutNameSaveToFile() throws Exception {
    JobMeta mockJobMeta = mock( JobMeta.class );

    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockJobMeta, true, false, "NotMainSpoonPerspective", true,
        true, null, null, true, true );

    doCallRealMethod().when( hopUi ).saveToFile( mockJobMeta );
    doReturn( true ).when( hopUi ).saveFileAs( mockJobMeta );
    assertTrue( hopUi.saveToFile( mockJobMeta ) );
    verify( mockJobMeta ).setRepository( hopUi.rep );
    verify( mockJobMeta ).setMetaStore( hopUi.metaStore );

    verify( hopUi.delegates.tabs ).renameTabs();
    verify( hopUi ).enableMenus();
  }

  @Test
  public void testJobToFileCantSaveToFile() throws Exception {
    JobMeta mockJobMeta = mock( JobMeta.class );

    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockJobMeta, true, false, "NotMainSpoonPerspective", true,
        true, null, null, true, false );

    doCallRealMethod().when( hopUi ).saveToFile( mockJobMeta );
    doReturn( true ).when( hopUi ).saveFileAs( mockJobMeta );
    assertFalse( hopUi.saveToFile( mockJobMeta ) );
    verify( mockJobMeta ).setRepository( hopUi.rep );
    verify( mockJobMeta ).setMetaStore( hopUi.metaStore );

    // repo is null, meta filename is null and meta.canSave() returns false, therefore none of the save methods are
    // called on the meta and the meta isn't actually saved - tabs should not be renamed
    verify( hopUi.delegates.tabs, never() ).renameTabs();
    verify( hopUi ).enableMenus();

    // now mock mockJobMeta.canSave() to return true, such that saveFileAs is called (also mocked to return true)
    doReturn( true ).when( mockJobMeta ).canSave();
    hopUi.saveToFile( mockJobMeta );
    // and verify that renameTabs is called
    verify( hopUi.delegates.tabs ).renameTabs();
  }

  @Test
  public void testTransToRepSaveToFile() throws Exception {
    TransMeta mockTransMeta = mock( TransMeta.class );

    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockTransMeta, false, false, MainHopUiPerspective.ID, true,
        true, null, null, true, true );

    doCallRealMethod().when( hopUi ).saveToFile( mockTransMeta );
    assertTrue( hopUi.saveToFile( mockTransMeta ) );
    verify( mockTransMeta ).setRepository( hopUi.rep );
    verify( mockTransMeta ).setMetaStore( hopUi.metaStore );

    verify( mockTransMeta ).setFilename( null );

    verify( hopUi.delegates.tabs ).renameTabs();
    verify( hopUi ).enableMenus();
  }

  @Test
  public void testJobToRepSaveFileAs() throws Exception {
    JobMeta mockJobMeta = mock( JobMeta.class );

    //passing a invalid type so not running GUIResource class
    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockJobMeta, false, false, MainHopUiPerspective.ID, true,
        true, "Invalid TYPE", null, true, true );

    doCallRealMethod().when( hopUi ).saveFileAs( mockJobMeta );
    assertTrue( hopUi.saveFileAs( mockJobMeta ) );
    verify( mockJobMeta ).setRepository( hopUi.rep );
    verify( mockJobMeta ).setMetaStore( hopUi.metaStore );

    verify( mockJobMeta ).setObjectId( null );
    verify( mockJobMeta ).setFilename( null );

    verify( hopUi.delegates.tabs ).findTabMapEntry( mockJobMeta );
    verify( hopUi ).enableMenus();
  }

  @Test
  public void testJobToRepSaveFileAsFailed() throws Exception {
    JobMeta mockJobMeta = mock( JobMeta.class );

    //passing a invalid type so not running GUIResource class
    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockJobMeta, false, false, MainHopUiPerspective.ID, false,
        true, "Invalid TYPE", null, true, true );

    doCallRealMethod().when( hopUi ).saveFileAs( mockJobMeta );
    assertFalse( hopUi.saveFileAs( mockJobMeta ) );
    verify( mockJobMeta ).setRepository( hopUi.rep );
    verify( mockJobMeta ).setMetaStore( hopUi.metaStore );

    verify( mockJobMeta ).setObjectId( null );
    verify( mockJobMeta ).setFilename( null );

    verify( hopUi ).enableMenus();
  }

  @Test
  public void testJobToXMLFileSaveFileAs() throws Exception {
    JobMeta mockJobMeta = mock( JobMeta.class );

    //passing a invalid type so not running GUIResource class
    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockJobMeta, true, true, "NotMainSpoonPerspective", true,
        true, "Invalid TYPE", null, true, true );

    doCallRealMethod().when( hopUi ).saveFileAs( mockJobMeta );
    assertTrue( hopUi.saveFileAs( mockJobMeta ) );
    verify( mockJobMeta ).setRepository( hopUi.rep );
    verify( mockJobMeta ).setMetaStore( hopUi.metaStore );

    verify( hopUi.delegates.tabs ).findTabMapEntry( mockJobMeta );
    verify( hopUi ).enableMenus();
  }

  @Test
  public void testJobToXMLFileSaveFileAsFailed() throws Exception {
    JobMeta mockJobMeta = mock( JobMeta.class );

    //passing a invalid type so not running GUIResource class
    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockJobMeta, true, true, "NotMainSpoonPerspective", true,
        false, "Invalid TYPE", null, true, true );

    doCallRealMethod().when( hopUi ).saveFileAs( mockJobMeta );
    assertFalse( hopUi.saveFileAs( mockJobMeta ) );
    verify( mockJobMeta ).setRepository( hopUi.rep );
    verify( mockJobMeta ).setMetaStore( hopUi.metaStore );

    verify( hopUi ).enableMenus();
  }

  @Test
  public void testTransToRepSaveFileAs() throws Exception {
    TransMeta mockTransMeta = mock( TransMeta.class );

    //passing a invalid type so not running GUIResource class
    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockTransMeta, false, false, MainHopUiPerspective.ID, true,
        true, "Invalid TYPE", null, true, true );

    doCallRealMethod().when( hopUi ).saveFileAs( mockTransMeta );
    assertTrue( hopUi.saveFileAs( mockTransMeta ) );
    verify( mockTransMeta ).setRepository( hopUi.rep );
    verify( mockTransMeta ).setMetaStore( hopUi.metaStore );

    verify( mockTransMeta ).setObjectId( null );
    verify( mockTransMeta ).setFilename( null );

    verify( hopUi.delegates.tabs ).findTabMapEntry( mockTransMeta );
    verify( hopUi ).enableMenus();
  }

  @Test
  public void testTransToRepSaveFileAsFailed() throws Exception {
    TransMeta mockTransMeta = mock( TransMeta.class );

    //passing a invalid type so not running GUIResource class
    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockTransMeta, false, false, MainHopUiPerspective.ID, false,
        true, "Invalid TYPE", null, true, true );

    doCallRealMethod().when( hopUi ).saveFileAs( mockTransMeta );
    assertFalse( hopUi.saveFileAs( mockTransMeta ) );
    verify( mockTransMeta ).setRepository( hopUi.rep );
    verify( mockTransMeta ).setMetaStore( hopUi.metaStore );

    verify( mockTransMeta ).setObjectId( null );
    verify( mockTransMeta ).setFilename( null );

    verify( hopUi ).enableMenus();
  }

  @Test
  public void testTransToXMLFileSaveFileAs() throws Exception {
    TransMeta mockTransMeta = mock( TransMeta.class );

    //passing a invalid type so not running GUIResource class
    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockTransMeta, true, true, "NotMainSpoonPerspective", true,
        true, "Invalid TYPE", null, true, true );

    doCallRealMethod().when( hopUi ).saveFileAs( mockTransMeta );
    assertTrue( hopUi.saveFileAs( mockTransMeta ) );
    verify( mockTransMeta ).setRepository( hopUi.rep );
    verify( mockTransMeta ).setMetaStore( hopUi.metaStore );

    verify( hopUi.delegates.tabs ).findTabMapEntry( mockTransMeta );
    verify( hopUi ).enableMenus();
  }

  @Test
  public void testTransToXMLFileSaveFileAsFailed() throws Exception {
    TransMeta mockTransMeta = mock( TransMeta.class );

    //passing a invalid type so not running GUIResource class
    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockTransMeta, true, true, "NotMainSpoonPerspective", true,
        false, "Invalid TYPE", null, true, true );

    doCallRealMethod().when( hopUi ).saveFileAs( mockTransMeta );
    assertFalse( hopUi.saveFileAs( mockTransMeta ) );
    verify( mockTransMeta ).setRepository( hopUi.rep );
    verify( mockTransMeta ).setMetaStore( hopUi.metaStore );

    verify( hopUi ).enableMenus();
  }

  @Test
  public void testTransToRepSaveObjectIdNotNullToFile() throws Exception {
    TransMeta mockTransMeta = mock( TransMeta.class );

    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockTransMeta, false, false, MainHopUiPerspective.ID, true,
        true, null, null, false, true );

    doCallRealMethod().when( hopUi ).saveToFile( mockTransMeta );
    assertTrue( hopUi.saveToFile( mockTransMeta ) );
    verify( mockTransMeta ).setRepository( hopUi.rep );
    verify( mockTransMeta ).setMetaStore( hopUi.metaStore );

    verify( mockTransMeta, never() ).setFilename( null );

    verify( hopUi.delegates.tabs ).renameTabs();
    verify( hopUi ).enableMenus();
  }

  @Test
  public void saveToRepository() throws Exception {
    JobMeta mockJobMeta = mock( JobMeta.class );

    prepareSetSaveTests( hopUi, log, mockHopUiPerspective, mockJobMeta, false, false, "NotMainSpoonPerspective", true,
      true, "filename", null, true, false );

    RepositoryDirectoryInterface dirMock = mock( RepositoryDirectoryInterface.class );
    doReturn( "my/path" ).when( dirMock ).getPath();
    doReturn( dirMock ).when( mockJobMeta ).getRepositoryDirectory();
    doReturn( "trans" ).when( mockJobMeta ).getName();

    RepositoryDirectoryInterface newDirMock = mock( RepositoryDirectoryInterface.class );
    doReturn( "my/new/path" ).when( newDirMock ).getPath();
    RepositoryObject repositoryObject = mock( RepositoryObject.class );
    doReturn( newDirMock ).when( repositoryObject ).getRepositoryDirectory();

    FileDialogOperation fileDlgOp = mock( FileDialogOperation.class );
    doReturn( repositoryObject ).when( fileDlgOp ).getRepositoryObject();
    doReturn( fileDlgOp ).when( hopUi ).getFileDialogOperation( FileDialogOperation.SAVE,
      FileDialogOperation.ORIGIN_SPOON );
    doReturn( "newTrans" ).when( repositoryObject ).getName();
    doCallRealMethod().when( hopUi ).saveToRepository( mockJobMeta, true );

    // mock a successful save
    doReturn( true ).when( hopUi ).saveToRepositoryConfirmed( mockJobMeta );
    hopUi.saveToRepository( mockJobMeta, true );
    // verify that the meta name and directory have been updated and renameTabs is called
    verify( hopUi.delegates.tabs, times( 1 ) ).renameTabs();
    verify( mockJobMeta, times( 1 ) ).setRepositoryDirectory( newDirMock );
    verify( mockJobMeta, never() ).setRepositoryDirectory( dirMock ); // verify that the dir is never set back
    verify( mockJobMeta, times( 1 ) ).setName( "newTrans" );
    verify( mockJobMeta, never()  ).setName( "trans" ); // verify that the name is never set back

    // mock a failed save
    doReturn( false ).when( hopUi ).saveToRepositoryConfirmed( mockJobMeta );
    hopUi.saveToRepository( mockJobMeta, true );
    // verify that the meta name and directory have not changed and renameTabs is not called (only once form the
    // previous test)
    verify( hopUi.delegates.tabs, times( 1 ) ).renameTabs();
    verify( mockJobMeta, times( 2 ) ).setRepositoryDirectory( newDirMock );
    verify( mockJobMeta, times( 1 ) ).setRepositoryDirectory( dirMock ); // verify that the dir is set back
    verify( mockJobMeta, times( 2 ) ).setName( "newTrans" );
    verify( mockJobMeta, times( 1 ) ).setName( "trans" ); // verify that the name is set back
  }

  private static void prepareSetSaveTests( HopUi hopUi, LogChannelInterface log, HopUiPerspective hopUiPerspective,
                                           AbstractMeta metaData, boolean repIsNull, boolean basicLevel, String perspectiveID, boolean saveToRepository,
                                           boolean saveXMLFile, String fileType, String filename, boolean objectIdIsNull, boolean canSave )
      throws Exception {

    TabMapEntry mockTabMapEntry = mock( TabMapEntry.class );
    TabItem mockTabItem = mock( TabItem.class );

    Repository mockRepository = mock( Repository.class );
    DelegatingMetaStore mockMetaStore = mock( DelegatingMetaStore.class );

    hopUi.rep = repIsNull ? null : mockRepository;
    hopUi.metaStore = mockMetaStore;
    hopUi.delegates = mock( HopUiDelegates.class );
    hopUi.delegates.tabs = mock( HopUiTabsDelegate.class );
    hopUi.props = mock( PropsUI.class );

    doReturn( mock( LogChannelInterface.class ) ).when( hopUi ).getLog();
    doReturn( perspectiveID ).when( hopUiPerspective ).getId();

    doReturn( basicLevel ).when( log ).isBasic();
    doReturn( basicLevel ).when( log ).isDetailed();
    doReturn( mockTabMapEntry ).when( hopUi.delegates.tabs ).findTabMapEntry( any() );
    doReturn( mockTabItem ).when( mockTabMapEntry ).getTabItem();
    doReturn( saveToRepository ).when( hopUi ).saveToRepository( metaData, true );
    doReturn( saveXMLFile ).when( hopUi ).saveXMLFile( metaData, false );
    if ( objectIdIsNull ) {
      doReturn( null ).when( metaData ).getObjectId();
    } else {
      doReturn( new ObjectId() {
        @Override public String getId() {
          return "objectId";
        }
      } ).when( metaData ).getObjectId();
    }

    //saveFile
    doReturn( filename ).when( metaData ).getFilename();
    doReturn( canSave ).when( metaData ).canSave();
    doReturn( false ).when( hopUi.props ).useDBCache();
    doReturn( saveToRepository ).when( hopUi ).saveToRepository( metaData );
    doReturn( saveXMLFile ).when( hopUi ).save( metaData, filename, false );

    doReturn( fileType ).when( metaData ).getFileType();
  }

  @Test
  public void testLoadLastUsedTransLocalWithRepository() throws Exception {
    String repositoryName = "repositoryName";
    String fileName = "fileName";

    setLoadLastUsedJobLocalWithRepository( false, repositoryName, null, fileName, true );
    verify( hopUi ).openFile( fileName, true );
  }

  @Test
  public void testLoadLastUsedTransLocalNoRepository() throws Exception {
    String repositoryName = null;
    String fileName = "fileName";

    setLoadLastUsedJobLocalWithRepository( false, repositoryName, null, fileName, true );
    verify( hopUi ).openFile( fileName, false );
  }

  @Test
  public void testLoadLastUsedTransLocalNoFilename() throws Exception {
    String repositoryName = null;
    String fileName = null;

    setLoadLastUsedJobLocalWithRepository( false, repositoryName, null, fileName, true );
    verify( hopUi, never() ).openFile( anyString(), anyBoolean() );
  }

  @Test
  public void testLoadLastUsedJobLocalWithRepository() throws Exception {
    String repositoryName = null;
    String fileName = "fileName";

    setLoadLastUsedJobLocalWithRepository( false, repositoryName, null, fileName, false );
    verify( hopUi ).openFile( fileName, false );
  }

  @Test
  public void testLoadLastUsedRepTransNoRepository() throws Exception {
    String repositoryName = null;
    String fileName = "fileName";

    setLoadLastUsedJobLocalWithRepository( true, repositoryName, null, fileName, false );
    verify( hopUi, never() ).openFile( anyString(), anyBoolean() );
  }


  @Test
  public void testLoadLastUsedTransLocalWithRepositoryAtStartup() throws Exception {
    String repositoryName = "repositoryName";
    String fileName = "fileName";

    setLoadLastUsedJobLocalWithRepository( false, repositoryName, null, fileName, true, true );
    verify( hopUi ).openFile( fileName, true );
  }

  @Test
  public void testLoadLastUsedTransLocalNoRepositoryAtStartup() throws Exception {
    String repositoryName = null;
    String fileName = "fileName";

    setLoadLastUsedJobLocalWithRepository( false, repositoryName, null, fileName, true, true );
    verify( hopUi ).openFile( fileName, false );
  }

  @Test
  public void testLoadLastUsedTransLocalNoFilenameAtStartup() throws Exception {
    String repositoryName = null;
    String fileName = null;

    setLoadLastUsedJobLocalWithRepository( false, repositoryName, null, fileName, true, true );
    verify( hopUi, never() ).openFile( anyString(), anyBoolean() );
  }

  @Test
  public void testLoadLastUsedJobLocalWithRepositoryAtStartup() throws Exception {
    String repositoryName = null;
    String fileName = "fileName";

    setLoadLastUsedJobLocalWithRepository( false, repositoryName, null, fileName, false, true );
    verify( hopUi ).openFile( fileName, false );
  }

  @Test
  public void testLoadLastUsedRepTransNoRepositoryAtStartup() throws Exception {
    String repositoryName = null;
    String fileName = "fileName";

    setLoadLastUsedJobLocalWithRepository( true, repositoryName, null, fileName, false, true );
    verify( hopUi, never() ).openFile( anyString(), anyBoolean() );
  }


  private void setLoadLastUsedJobLocalWithRepository(
    boolean isSourceRepository, String repositoryName, String directoryName, String fileName, boolean
    isTransformation ) throws Exception {
    setLoadLastUsedJobLocalWithRepository( isSourceRepository, repositoryName, directoryName, fileName,
      isTransformation, false );
  }

  private void setLoadLastUsedJobLocalWithRepository( boolean isSourceRepository, String repositoryName,
      String directoryName, String fileName, boolean isTransformation, boolean isStartup ) throws Exception {
    LastUsedFile mockLastUsedFile = mock( LastUsedFile.class );

    if ( repositoryName != null ) {
      Repository mockRepository = mock( Repository.class );
      hopUi.rep = mockRepository;
      doReturn( repositoryName ).when( mockRepository ).getName();
    } else {
      hopUi.rep = null;
    }

    doReturn( isSourceRepository ).when( mockLastUsedFile ).isSourceRepository();
    doReturn( repositoryName ).when( mockLastUsedFile ).getRepositoryName();
    doReturn( directoryName ).when( mockLastUsedFile ).getDirectory();
    doReturn( fileName ).when( mockLastUsedFile ).getFilename();
    doReturn( isTransformation ).when( mockLastUsedFile ).isTransformation();
    doReturn( !isTransformation ).when( mockLastUsedFile ).isJob();

    if ( isStartup ) {
      doCallRealMethod().when( hopUi ).loadLastUsedFileAtStartup( mockLastUsedFile, repositoryName );
      hopUi.loadLastUsedFileAtStartup( mockLastUsedFile, repositoryName );
    } else {
      doCallRealMethod().when( hopUi ).loadLastUsedFile( mockLastUsedFile, repositoryName );
      hopUi.loadLastUsedFile( mockLastUsedFile, repositoryName );
    }
  }

  @Test
  public void testCancelPromptToSave() throws Exception {
    setPromptToSave( SWT.CANCEL, false );
    assertFalse( hopUi.promptForSave() );
  }

  @Test
  public void testNoPromptToSave() throws Exception {
    HopUiBrowser mockBrowser = setPromptToSave( SWT.NO, false );
    assertTrue( hopUi.promptForSave() );
    verify( mockBrowser, never() ).applyChanges();
  }

  @Test
  public void testYesPromptToSave() throws Exception {
    HopUiBrowser mockBrowser = setPromptToSave( SWT.YES, false );
    assertTrue( hopUi.promptForSave() );
    verify( mockBrowser ).applyChanges();
  }

  @Test
  public void testCanClosePromptToSave() throws Exception {
    setPromptToSave( SWT.YES, true );
    assertTrue( hopUi.promptForSave() );
  }

  private HopUiBrowser setPromptToSave( int buttonPressed, boolean canbeClosed ) throws Exception {
    TabMapEntry mockTabMapEntry = mock( TabMapEntry.class );
    TabSet mockTabSet = mock( TabSet.class );
    ArrayList<TabMapEntry> lTabs = new ArrayList<>();
    lTabs.add( mockTabMapEntry );

    HopUiBrowser mockHopUiBrowser = mock( HopUiBrowser.class );

    hopUi.delegates = mock( HopUiDelegates.class );
    hopUi.delegates.tabs = mock( HopUiTabsDelegate.class );
    hopUi.tabfolder = mockTabSet;

    doReturn( lTabs ).when( hopUi.delegates.tabs ).getTabs();
    doReturn( mockHopUiBrowser ).when( mockTabMapEntry ).getObject();
    doReturn( canbeClosed ).when( mockHopUiBrowser ).canBeClosed();
    doReturn( buttonPressed ).when( mockHopUiBrowser ).showChangedWarning();

    doCallRealMethod().when( hopUi ).promptForSave();

    return mockHopUiBrowser;
  }

  @Test
  public void testVersioningEnabled() throws Exception {
    Repository repository = Mockito.mock( Repository.class );
    RepositorySecurityProvider securityProvider = Mockito.mock( RepositorySecurityProvider.class );
    Mockito.doReturn( securityProvider ).when( repository ).getSecurityProvider();
    EngineMetaInterface jobTransMeta = Mockito.spy( new TransMeta() );
    RepositoryDirectoryInterface repositoryDirectoryInterface = Mockito.mock( RepositoryDirectoryInterface.class );
    Mockito.doReturn( "/home" ).when( repositoryDirectoryInterface ).toString();
    Mockito.doReturn( "trans" ).when( jobTransMeta ).getName();
    Mockito.doReturn( RepositoryObjectType.TRANSFORMATION ).when( jobTransMeta ).getRepositoryElementType();
    Mockito.doReturn( true ).when( jobTransMeta ).getVersioningEnabled();

    boolean result = HopUi.isVersionEnabled( repository, jobTransMeta );

    Assert.assertTrue( result );
    Mockito.verify( securityProvider, Mockito.never() ).isVersioningEnabled( Mockito.anyString() );
  }

  @Test
  public void testVersioningDisabled() throws Exception {
    Repository repository = Mockito.mock( Repository.class );
    RepositorySecurityProvider securityProvider = Mockito.mock( RepositorySecurityProvider.class );
    Mockito.doReturn( securityProvider ).when( repository ).getSecurityProvider();
    EngineMetaInterface jobTransMeta = Mockito.spy( new TransMeta() );
    RepositoryDirectoryInterface repositoryDirectoryInterface = Mockito.mock( RepositoryDirectoryInterface.class );
    Mockito.doReturn( "/home" ).when( repositoryDirectoryInterface ).toString();
    Mockito.doReturn( "trans" ).when( jobTransMeta ).getName();
    Mockito.doReturn( RepositoryObjectType.TRANSFORMATION ).when( jobTransMeta ).getRepositoryElementType();
    Mockito.doReturn( false ).when( jobTransMeta ).getVersioningEnabled();

    boolean result = HopUi.isVersionEnabled( repository, jobTransMeta );

    Assert.assertFalse( result );
    Mockito.verify( securityProvider, Mockito.never() ).isVersioningEnabled( Mockito.anyString() );
  }

  @Test
  public void testVersioningCheckingOnServer() throws Exception {
    Repository repository = Mockito.mock( Repository.class );
    RepositorySecurityProvider securityProvider = Mockito.mock( RepositorySecurityProvider.class );
    Mockito.doReturn( securityProvider ).when( repository ).getSecurityProvider();
    EngineMetaInterface jobTransMeta = Mockito.spy( new TransMeta() );
    RepositoryDirectoryInterface repositoryDirectoryInterface = Mockito.mock( RepositoryDirectoryInterface.class );
    Mockito.doReturn( "/home" ).when( repositoryDirectoryInterface ).toString();
    Mockito.doReturn( "trans" ).when( jobTransMeta ).getName();
    Mockito.doReturn( RepositoryObjectType.TRANSFORMATION ).when( jobTransMeta ).getRepositoryElementType();
    Mockito.doReturn( true ).when( securityProvider ).isVersioningEnabled( Mockito.anyString() );

    boolean result = HopUi.isVersionEnabled( repository, jobTransMeta );
    Assert.assertTrue( result );
  }

  @Test
  public void textGetFileType() {

    assertEquals( "File", HopUi.getFileType( null ) );
    assertEquals( "File", HopUi.getFileType( "" ) );
    assertEquals( "File", HopUi.getFileType( " " ) );
    assertEquals( "File", HopUi.getFileType( "foo" ) );
    assertEquals( "File", HopUi.getFileType( "foo/foe" ) );
    assertEquals( "File", HopUi.getFileType( "ktr" ) );
    assertEquals( "File", HopUi.getFileType( "ktr" ) );

    assertEquals( "Transformation", HopUi.getFileType( "foo/foe.ktr" ) );
    assertEquals( "Transformation", HopUi.getFileType( "foe.ktr" ) );
    assertEquals( "Transformation", HopUi.getFileType( ".ktr" ) );

    assertEquals( "Job", HopUi.getFileType( "foo/foe.kjb" ) );
    assertEquals( "Job", HopUi.getFileType( "foe.kjb" ) );
    assertEquals( "Job", HopUi.getFileType( ".kjb" ) );
  }
}
