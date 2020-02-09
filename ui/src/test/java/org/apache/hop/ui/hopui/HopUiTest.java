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

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepErrorMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
//import org.apache.hop.trans.steps.csvinput.CsvInputMeta;
import org.apache.hop.trans.steps.dummytrans.DummyTransMeta;
import org.apache.hop.ui.hopui.delegates.HopUiDelegates;
import org.apache.hop.ui.hopui.delegates.HopUiTabsDelegate;
import org.apache.xul.swt.tab.TabSet;
import org.eclipse.swt.SWT;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyListOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
   * see also http://jira.pentaho.com/browse/PDI-689
   *
   * @throws HopException
   */
  //TODO: refactor
/*
  @Test
  public void testCopyPasteStepsErrorHandling() throws HopException {

    final TransMeta transMeta = new TransMeta();

    //for check copy both step and hop
    StepMeta sourceStep = new StepMeta( "CsvInput", "Step1", new CsvInputMeta() );
    StepMeta targetStep = new StepMeta( "Dummy", "Dummy Step1", new DummyTransMeta() );

    sourceStep.setSelected( true );
    targetStep.setSelected( true );

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
        hopUi.pasteXML( transMeta, (String) invocation.getArguments()[ 0 ], mock( Point.class ) );
        assertTrue( "Steps was not copied", stepsSizeBefore < transMeta.getSteps().size() );
        //selected copied step
        for ( StepMeta s : transMeta.getSelectedSteps() ) {
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
*/

  /**
   * test copy one step with error handling
   * see http://jira.pentaho.com/browse/PDI-13358
   *
   * @throws HopException
   */
  //TODO: refactor
  /*@Test
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
        hopUi.pasteXML( transMeta, (String) invocation.getArguments()[ 0 ], mock( Point.class ) );
        assertTrue( "Steps was not copied", stepsSizeBefore < transMeta.getSteps().size() );
        //selected copied step
        for ( StepMeta s : transMeta.getSelectedSteps() ) {
          if ( s.getStepMetaInterface() instanceof CsvInputMeta ) {
            //check that stepError was empty, because we copy only one step from pair
            assertNull( "Error hop was not copied", s.getStepErrorMeta() );
          }
        }
        return null;
      }
    } ).when( hopUi ).toClipboard( anyString() );

    hopUi.copySelected( transMeta, transMeta.getSelectedSteps(), Collections.<NotePadMeta>emptyList() );
  }*/


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
