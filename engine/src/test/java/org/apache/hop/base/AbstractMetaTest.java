/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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
package org.apache.hop.base;

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.changed.IChanged;
import org.apache.hop.core.changed.IHopObserver;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.listeners.IContentChangedListener;
import org.apache.hop.core.listeners.ICurrentDirectoryChangedListener;
import org.apache.hop.core.listeners.IFilenameChangedListener;
import org.apache.hop.core.listeners.INameChangedListener;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.parameters.INamedParams;
import org.apache.hop.core.parameters.NamedParamsDefault;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.undo.ChangeAction;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AbstractMetaTest {
  AbstractMeta meta;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType( DatabasePluginType.getInstance() );
    PluginRegistry.init();
  }

  @Before
  public void setUp() throws Exception {
    meta = new AbstractMetaStub();
  }

  @Test
  public void testGetParent() {
    assertNull( meta.getParent() );
  }

  @Test
  public void testGetSetContainerObjectId() throws Exception {
    assertNull( meta.getContainerId() );
    meta.setCarteObjectId( "myObjectId" );
    assertEquals( "myObjectId", meta.getContainerId() );
  }

  @Test
  public void testGetSetName() throws Exception {
    assertNull( meta.getName() );
    meta.setName( "myName" );
    assertEquals( "myName", meta.getName() );
  }

  @Test
  public void testNameFilenameSync() throws Exception {
    meta.setName( "myName" );
    assertEquals( "myName", meta.getName() );

    meta.setNameSynchronizedWithFilename( true );
    meta.setFilename( "/my/path/some-file-name.ext" );
    assertEquals( "some-file-name", meta.getName() );

    meta.setFilename( "C:\\some\\windows\\path\\windows-name.ext" );
    assertEquals( "windows-name", meta.getName() );
  }


  @Test
  public void testGetSetDescription() throws Exception {
    assertNull( meta.getDescription() );
    meta.setDescription( "I am a meta" );
    assertEquals( "I am a meta", meta.getDescription() );
  }

  @Test
  public void testGetSetExtendedDescription() throws Exception {
    assertNull( meta.getExtendedDescription() );
    meta.setExtendedDescription( "I am a meta" );
    assertEquals( "I am a meta", meta.getExtendedDescription() );
  }

  @Test
  public void testNameFromFilename() throws Exception {
    assertNull( meta.getName() );
    assertNull( meta.getFilename() );
    meta.nameFromFilename();
    assertNull( meta.getName() );
    meta.setFilename( "/path/to/my/file 2.hpl" );
    meta.nameFromFilename();
    assertEquals( "file 2", meta.getName() );
  }

  @Test
  public void testGetSetFilename() throws Exception {
    assertNull( meta.getFilename() );
    meta.setFilename( "myfile" );
    assertEquals( "myfile", meta.getFilename() );
  }

  @Test
  public void testAddNameChangedListener() throws Exception {
    meta.fireNameChangedListeners( "a", "a" );
    meta.fireNameChangedListeners( "a", "b" );
    meta.addNameChangedListener( null );
    meta.fireNameChangedListeners( "a", "b" );
    INameChangedListener listener = mock( INameChangedListener.class );
    meta.addNameChangedListener( listener );
    meta.fireNameChangedListeners( "b", "a" );
    verify( listener, times( 1 ) ).nameChanged( meta, "b", "a" );
    meta.removeNameChangedListener( null );
    meta.removeNameChangedListener( listener );
    meta.fireNameChangedListeners( "b", "a" );
    verifyNoMoreInteractions( listener );
  }

  @Test
  public void testAddFilenameChangedListener() throws Exception {
    meta.fireFilenameChangedListeners( "a", "a" );
    meta.fireFilenameChangedListeners( "a", "b" );
    meta.addFilenameChangedListener( null );
    meta.fireFilenameChangedListeners( "a", "b" );
    IFilenameChangedListener listener = mock( IFilenameChangedListener.class );
    meta.addFilenameChangedListener( listener );
    meta.fireFilenameChangedListeners( "b", "a" );
    verify( listener, times( 1 ) ).filenameChanged( meta, "b", "a" );
    meta.removeFilenameChangedListener( null );
    meta.removeFilenameChangedListener( listener );
    meta.fireFilenameChangedListeners( "b", "a" );
    verifyNoMoreInteractions( listener );
  }

  @Test
  public void testAddRemoveFireContentChangedListener() throws Exception {
    assertTrue( meta.getContentChangedListeners().isEmpty() );
    IContentChangedListener listener = mock( IContentChangedListener.class );
    meta.addContentChangedListener( listener );
    assertFalse( meta.getContentChangedListeners().isEmpty() );
    meta.fireContentChangedListeners();
    verify( listener, times( 1 ) ).contentChanged( anyObject() );
    verify( listener, never() ).contentSafe( anyObject() );
    meta.fireContentChangedListeners( true );
    verify( listener, times( 2 ) ).contentChanged( anyObject() );
    verify( listener, never() ).contentSafe( anyObject() );
    meta.fireContentChangedListeners( false );
    verify( listener, times( 2 ) ).contentChanged( anyObject() );
    verify( listener, times( 1 ) ).contentSafe( anyObject() );
    meta.removeContentChangedListener( listener );
    assertTrue( meta.getContentChangedListeners().isEmpty() );
  }

  @Test
  public void testAddCurrentDirectoryChangedListener() throws Exception {
    meta.fireNameChangedListeners( "a", "a" );
    meta.fireNameChangedListeners( "a", "b" );
    meta.addCurrentDirectoryChangedListener( null );
    meta.fireCurrentDirectoryChanged( "a", "b" );
    ICurrentDirectoryChangedListener listener = mock( ICurrentDirectoryChangedListener.class );
    meta.addCurrentDirectoryChangedListener( listener );
    meta.fireCurrentDirectoryChanged( "b", "a" );
    verify( listener, times( 1 ) ).directoryChanged( meta, "b", "a" );
    meta.fireCurrentDirectoryChanged( "a", "a" );
    meta.removeCurrentDirectoryChangedListener( null );
    meta.removeCurrentDirectoryChangedListener( listener );
    meta.fireNameChangedListeners( "b", "a" );
    verifyNoMoreInteractions( listener );
  }

  @Test
  public void testAddRemoveViewUndo() throws Exception {
    // addUndo() right now will fail with an NPE
    assertEquals( 0, meta.getUndoSize() );
    meta.clearUndo();
    assertEquals( 0, meta.getUndoSize() );
    assertEquals( 0, meta.getMaxUndo() );
    meta.setMaxUndo( 3 );
    assertEquals( 3, meta.getMaxUndo() );
    // viewThisUndo() and viewPreviousUndo() have the same logic
    assertNull( meta.viewThisUndo() );
    assertNull( meta.viewPreviousUndo() );
    assertNull( meta.viewNextUndo() );
    assertNull( meta.previousUndo() );
    assertNull( meta.nextUndo() );
    TransformMeta fromMeta = mock( TransformMeta.class );
    TransformMeta toMeta = mock( TransformMeta.class );
    Object[] from = new Object[] { fromMeta };
    Object[] to = new Object[] { toMeta };
    int[] pos = new int[ 0 ];
    Point[] prev = new Point[ 0 ];
    Point[] curr = new Point[ 0 ];

    meta.addUndo( from, to, pos, prev, curr, AbstractMeta.TYPE_UNDO_NEW, false );
    assertNotNull( meta.viewThisUndo() );
    assertNotNull( meta.viewPreviousUndo() );
    assertNull( meta.viewNextUndo() );
    meta.addUndo( from, to, pos, prev, curr, AbstractMeta.TYPE_UNDO_CHANGE, false );
    assertNotNull( meta.viewThisUndo() );
    assertNotNull( meta.viewPreviousUndo() );
    assertNull( meta.viewNextUndo() );
    ChangeAction action = meta.previousUndo();
    assertNotNull( action );
    assertEquals( ChangeAction.ActionType.ChangeTransform, action.getType() );
    assertNotNull( meta.viewThisUndo() );
    assertNotNull( meta.viewPreviousUndo() );
    assertNotNull( meta.viewNextUndo() );
    meta.addUndo( from, to, pos, prev, curr, AbstractMeta.TYPE_UNDO_DELETE, false );
    meta.addUndo( from, to, pos, prev, curr, AbstractMeta.TYPE_UNDO_POSITION, false );
    assertNotNull( meta.previousUndo() );
    assertNotNull( meta.nextUndo() );
    meta.setMaxUndo( 1 );
    assertEquals( 1, meta.getUndoSize() );
    meta.addUndo( from, to, pos, prev, curr, AbstractMeta.TYPE_UNDO_NEW, false );
  }

  @Test
  public void testGetSetAttributes() throws Exception {
    assertNull( meta.getAttributesMap() );
    Map<String, Map<String, String>> attributesMap = new HashMap<>();
    meta.setAttributesMap( attributesMap );
    assertNull( meta.getAttributes( "group1" ) );
    Map<String, String> group1Attributes = new HashMap<>();
    attributesMap.put( "group1", group1Attributes );
    assertEquals( group1Attributes, meta.getAttributes( "group1" ) );
    assertNull( meta.getAttribute( "group1", "attr1" ) );
    group1Attributes.put( "attr1", "value1" );
    assertEquals( "value1", meta.getAttribute( "group1", "attr1" ) );
    assertNull( meta.getAttribute( "group1", "attr2" ) );
    meta.setAttribute( "group1", "attr2", "value2" );
    assertEquals( "value2", meta.getAttribute( "group1", "attr2" ) );
    meta.setAttributes( "group2", null );
    assertNull( meta.getAttributes( "group2" ) );
    meta.setAttribute( "group2", "attr3", "value3" );
    assertNull( meta.getAttribute( "group3", "attr4" ) );
  }

  @Test
  public void testNotes() throws Exception {
    assertNull( meta.getNotes() );
    // most note methods will NPE at this point, so call clear() to create an empty note list
    meta.clear();
    assertNotNull( meta.getNotes() );
    assertTrue( meta.getNotes().isEmpty() );
    // Can't get a note from an empty list (i.e. no indices)
    Exception e = null;
    try {
      assertNull( meta.getNote( 0 ) );
    } catch ( IndexOutOfBoundsException ioobe ) {
      e = ioobe;
    }
    assertNotNull( e );
    assertNull( meta.getNote( 20, 20 ) );
    NotePadMeta note1 = mock( NotePadMeta.class );
    meta.removeNote( 0 );
    assertFalse( meta.hasChanged() );
    meta.addNote( note1 );
    assertTrue( meta.hasChanged() );
    NotePadMeta note2 = mock( NotePadMeta.class );
    when( note2.getLocation() ).thenReturn( new Point( 0, 0 ) );
    when( note2.isSelected() ).thenReturn( true );
    meta.addNote( 1, note2 );
    assertEquals( note2, meta.getNote( 0, 0 ) );
    List<NotePadMeta> selectedNotes = meta.getSelectedNotes();
    assertNotNull( selectedNotes );
    assertEquals( 1, selectedNotes.size() );
    assertEquals( note2, selectedNotes.get( 0 ) );
    assertEquals( 1, meta.indexOfNote( note2 ) );
    meta.removeNote( 2 );
    assertEquals( 2, meta.nrNotes() );
    meta.removeNote( 1 );
    assertEquals( 1, meta.nrNotes() );
    assertTrue( meta.haveNotesChanged() );
    meta.clearChanged();
    assertFalse( meta.haveNotesChanged() );

    meta.addNote( 1, note2 );
    meta.lowerNote( 1 );
    assertTrue( meta.haveNotesChanged() );
    meta.clearChanged();
    assertFalse( meta.haveNotesChanged() );
    meta.raiseNote( 0 );
    assertTrue( meta.haveNotesChanged() );
    meta.clearChanged();
    assertFalse( meta.haveNotesChanged() );
    int[] indexes = meta.getNoteIndexes( Arrays.asList( note1, note2 ) );
    assertNotNull( indexes );
    assertEquals( 2, indexes.length );
  }


  @Test
  public void testCopyVariablesFrom() throws Exception {
    assertNull( meta.getVariable( "var1" ) );
    IVariables vars = mock( IVariables.class );
    when( vars.getVariable( "var1" ) ).thenReturn( "x" );
    when( vars.listVariables() ).thenReturn( new String[] { "var1" } );
    meta.copyVariablesFrom( vars );
    assertEquals( "x", meta.getVariable( "var1", "y" ) );
  }

  @Test
  public void testEnvironmentSubstitute() throws Exception {
    // This is just a delegate method, verify it's called
    IVariables vars = mock( IVariables.class );
    // This method is reused by the stub to set the mock as the variables object
    meta.setInternalHopVariables( vars );

    meta.environmentSubstitute( "${param}" );
    verify( vars, times( 1 ) ).environmentSubstitute( "${param}" );
    String[] params = new String[] { "${param}" };
    meta.environmentSubstitute( params );
    verify( vars, times( 1 ) ).environmentSubstitute( params );
  }

  @Test
  public void testFieldSubstitute() throws Exception {
    // This is just a delegate method, verify it's called
    IVariables vars = mock( IVariables.class );
    // This method is reused by the stub to set the mock as the variables object
    meta.setInternalHopVariables( vars );

    IRowMeta rowMeta = mock( IRowMeta.class );
    Object[] data = new Object[ 0 ];
    meta.fieldSubstitute( "?{param}", rowMeta, data );
    verify( vars, times( 1 ) ).fieldSubstitute( "?{param}", rowMeta, data );
  }

  @Test
  public void testGetSetParentVariableSpace() throws Exception {
    assertNull( meta.getParentVariableSpace() );
    IVariables variables = mock( IVariables.class );
    meta.setParentVariableSpace( variables );
    assertEquals( variables, meta.getParentVariableSpace() );
  }

  @Test
  public void testGetSetVariable() throws Exception {
    assertNull( meta.getVariable( "var1" ) );
    assertEquals( "x", meta.getVariable( "var1", "x" ) );
    meta.setVariable( "var1", "y" );
    assertEquals( "y", meta.getVariable( "var1", "x" ) );
  }

  @Test
  public void testGetSetParameterValue() throws Exception {
    assertNull( meta.getParameterValue( "var1" ) );
    assertNull( meta.getParameterDefault( "var1" ) );
    assertNull( meta.getParameterDescription( "var1" ) );

    meta.setParameterValue( "var1", "y" );
    // Values for new parameters must be added by addParameterDefinition
    assertNull( meta.getParameterValue( "var1" ) );
    assertNull( meta.getParameterDefault( "var1" ) );
    assertNull( meta.getParameterDescription( "var1" ) );

    meta.addParameterDefinition( "var2", "z", "My Description" );
    assertEquals( "", meta.getParameterValue( "var2" ) );
    assertEquals( "z", meta.getParameterDefault( "var2" ) );
    assertEquals( "My Description", meta.getParameterDescription( "var2" ) );
    meta.setParameterValue( "var2", "y" );
    assertEquals( "y", meta.getParameterValue( "var2" ) );
    assertEquals( "z", meta.getParameterDefault( "var2" ) );

    String[] params = meta.listParameters();
    assertNotNull( params );

    // clearParameters() just clears their values, not their presence
    meta.clearParameters();
    assertEquals( "", meta.getParameterValue( "var2" ) );

    // eraseParameters() clears the list of parameters
    meta.eraseParameters();
    assertNull( meta.getParameterValue( "var1" ) );

    INamedParams newParams = new NamedParamsDefault();
    newParams.addParameterDefinition( "var3", "default", "description" );
    newParams.setParameterValue( "var3", "a" );
    meta.copyParametersFrom( newParams );
    meta.activateParameters();
    assertEquals( "default", meta.getParameterDefault( "var3" ) );
  }

  @Test
  public void testGetSetLogLevel() throws Exception {
    assertEquals( LogLevel.BASIC, meta.getLogLevel() );
    meta.setLogLevel( LogLevel.DEBUG );
    assertEquals( LogLevel.DEBUG, meta.getLogLevel() );
  }

  @Test
  public void testGetSetCreatedDate() throws Exception {
    assertNull( meta.getCreatedDate() );
    Date now = Calendar.getInstance().getTime();
    meta.setCreatedDate( now );
    assertEquals( now, meta.getCreatedDate() );
  }

  @Test
  public void testGetSetCreatedUser() throws Exception {
    assertNull( meta.getCreatedUser() );
    meta.setCreatedUser( "joe" );
    assertEquals( "joe", meta.getCreatedUser() );
  }

  @Test
  public void testGetSetModifiedDate() throws Exception {
    assertNull( meta.getModifiedDate() );
    Date now = Calendar.getInstance().getTime();
    meta.setModifiedDate( now );
    assertEquals( now, meta.getModifiedDate() );
  }

  @Test
  public void testGetSetModifiedUser() throws Exception {
    assertNull( meta.getModifiedUser() );
    meta.setModifiedUser( "joe" );
    assertEquals( "joe", meta.getModifiedUser() );
  }

  @Test
  public void testAddDeleteModifyObserver() throws Exception {
    IHopObserver observer = mock( IHopObserver.class );
    meta.addObserver( observer );
    Object event = new Object();
    meta.notifyObservers( event );
    // Changed flag isn't set, so this won't be called
    verify( observer, never() ).update( meta, event );
    meta.setChanged( true );
    meta.notifyObservers( event );
    verify( observer, times( 1 ) ).update( any( IChanged.class ), anyObject() );
  }

  @Test
  public void testGetRegistrationDate() throws Exception {
    assertNull( meta.getRegistrationDate() );
  }

  @Test
  public void testGetObjectNameCopyRevision() throws Exception {
    assertNull( meta.getObjectName() );
    meta.setName( "x" );
    assertEquals( "x", meta.getObjectName() );
    assertNull( meta.getObjectCopy() );
  }

  @Test
  public void testHasMissingPlugins() throws Exception {
    assertFalse( meta.hasMissingPlugins() );
  }

  @Test
  public void testGetBooleanValueOfVariable() {
    assertFalse( meta.getBooleanValueOfVariable( null, false ) );
    assertTrue( meta.getBooleanValueOfVariable( "", true ) );
    assertTrue( meta.getBooleanValueOfVariable( "true", true ) );
    assertFalse( meta.getBooleanValueOfVariable( "${myVar}", false ) );
    meta.setVariable( "myVar", "Y" );
    assertTrue( meta.getBooleanValueOfVariable( "${myVar}", false ) );
  }

  @Test
  public void testInitializeShareInjectVariables() {
    meta.initializeVariablesFrom( null );
    IVariables parent = mock( IVariables.class );
    when( parent.getVariable( "var1" ) ).thenReturn( "x" );
    when( parent.listVariables() ).thenReturn( new String[] { "var1" } );
    meta.initializeVariablesFrom( parent );
    assertEquals( "x", meta.getVariable( "var1" ) );
    assertNotNull( meta.listVariables() );
    IVariables newVars = mock( IVariables.class );
    when( newVars.getVariable( "var2" ) ).thenReturn( "y" );
    when( newVars.listVariables() ).thenReturn( new String[] { "var2" } );
    meta.shareVariablesWith( newVars );
    assertEquals( "y", meta.getVariable( "var2" ) );
    Map<String, String> props = new HashMap<>();
    props.put( "var3", "a" );
    props.put( "var4", "b" );
    meta.shareVariablesWith( new Variables() );
    meta.injectVariables( props );
    // Need to "Activate" the injection, we can initialize from null
    meta.initializeVariablesFrom( null );
    assertEquals( "a", meta.getVariable( "var3" ) );
    assertEquals( "b", meta.getVariable( "var4" ) );
  }

  @Test
  public void testCanSave() {
    assertTrue( meta.canSave() );
  }

  @Test
  public void testHasChanged() {
    meta.clear();
    assertFalse( meta.hasChanged() );
    meta.setChanged( true );
    assertTrue( meta.hasChanged() );
  }

  @Test
  public void testMultithreadHammeringOfListener() throws Exception {

    CountDownLatch latch = new CountDownLatch( 3 );
    AbstractMetaListenerThread th1 = new AbstractMetaListenerThread( meta, 2000, latch ); // do 2k random add/delete/fire
    AbstractMetaListenerThread th2 = new AbstractMetaListenerThread( meta, 2000, latch ); // do 2k random add/delete/fire
    AbstractMetaListenerThread th3 = new AbstractMetaListenerThread( meta, 2000, latch ); // do 2k random add/delete/fire

    Thread t1 = new Thread( th1 );
    Thread t2 = new Thread( th2 );
    Thread t3 = new Thread( th3 );
    try {
      t1.start();
      t2.start();
      t3.start();
      latch.await(); // Will hang out waiting for each thread to complete...
    } catch ( InterruptedException badTest ) {
      throw badTest;
    }
    assertEquals( "No exceptions encountered", th1.message );
    assertEquals( "No exceptions encountered", th2.message );
    assertEquals( "No exceptions encountered", th3.message );
  }

  /**
   * Stub class for AbstractMeta. No need to test the abstract methods here, they should be done in unit tests for
   * proper child classes.
   */
  public static class AbstractMetaStub extends AbstractMeta {

    @Override protected String getExtension() {
      return ".ext";
    }

    // Reuse this method to set a mock internal variable space
    @Override
    public void setInternalHopVariables( IVariables var ) {
      this.variables = var;
    }

    @Override
    protected void setInternalFilenameHopVariables( IVariables var ) {

    }

    @Override
    protected void setInternalNameHopVariable( IVariables var ) {

    }

    @Override
    public String getXml() throws HopException {
      return null;
    }

    @Override
    public String getLogChannelId() {
      return null;
    }

    @Override
    public LoggingObjectType getObjectType() {
      return null;
    }

    @Override
    public boolean isGatheringMetrics() {
      return false;
    }

    @Override
    public void setGatheringMetrics( boolean b ) {
    }

    @Override
    public void setForcingSeparateLogging( boolean b ) {
    }

    @Override
    public boolean isForcingSeparateLogging() {
      return false;
    }

  }


  private class AbstractMetaListenerThread implements Runnable {
    AbstractMeta metaToWork;
    int times;
    CountDownLatch whenDone;
    String message;

    AbstractMetaListenerThread( AbstractMeta aMeta, int times, CountDownLatch latch ) {
      this.metaToWork = aMeta;
      this.times = times;
      this.whenDone = latch;
    }

    @Override public void run() {
      for ( int i = 0; i < times; i++ ) {
        int randomNum = ThreadLocalRandom.current().nextInt( 0, 3 );
        switch ( randomNum ) {
          case 0: {
            try {
              metaToWork.addFilenameChangedListener( mock( IFilenameChangedListener.class ) );
            } catch ( Throwable ex ) {
              message = "Exception adding listener.";
            }
            break;
          }
          case 1: {
            try {
              metaToWork.removeFilenameChangedListener( mock( IFilenameChangedListener.class ) );
            } catch ( Throwable ex ) {
              message = "Exception removing listener.";
            }
            break;
          }
          default: {
            try {
              metaToWork.fireFilenameChangedListeners( "oldName", "newName" );
            } catch ( Throwable ex ) {
              message = "Exception firing listeners.";
            }
            break;
          }
        }
      }
      if ( message == null ) {
        message = "No exceptions encountered";
      }
      whenDone.countDown(); // show success...
    }
  }

}
